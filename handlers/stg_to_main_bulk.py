"""
Bulk Stage 2 — staging → production tables, one account per invocation.

The delta counterpart to handlers/stg_to_db.py. Same twelve processors in the same strict
dependency order, but every query is account-scoped only: the bulk staging tables are
unique on (account_id, business key), so one pass covers all of an account's locations.

One action per invocation; the Step Function sequences them:

  vacuum   VACUUM ANALYZE the target tables once, before the Map fans out. The
           onboarding pipeline vacuums inside each account's run; at Map concurrency that
           would mean several concurrent VACUUMs over the same tables.
  promote  (default) run the twelve processors for one account, then recompute that
           account's customer class dates
  verify   duplicate-parent pre-flight for one account (or all promotable accounts),
           writes nothing

Event:
  {"action": "promote", "account_id": 1410}
  {"action": "promote", "account_id": 1410, "allow_duplicate_parents": true}
  {"action": "vacuum"}
  {"action": "verify", "account_id": 1410}

Environment variables — see core/bulk_config.py.
"""

import asyncio
import json
import logging
import sys
import time

from sqlalchemy import create_engine, text

from core.bulk_config import settings
from core.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

from stg_to_main_bulk.customers import process_customers
from stg_to_main_bulk.customer_notes import process_customer_notes
from stg_to_main_bulk.customer_tags.base import process_customer_tags
from stg_to_main_bulk.class_sessions import process_class_sessions
from stg_to_main_bulk.membership_instances import process_membership_instances
from stg_to_main_bulk.credit_transactions import process_credit_transactions
from stg_to_main_bulk.credit_transactions_orders import process_credit_transactions_orders
from stg_to_main_bulk.membership_transactions import process_membership_transactions
from stg_to_main_bulk.membership_transactions_orders import process_membership_transactions_orders
from stg_to_main_bulk.orders import process_orders
from stg_to_main_bulk.order_lines.base import process_order_lines
from stg_to_main_bulk.reservations.base import process_reservations


PROCESSING_ORDER = [
    ("customers_01",                       process_customers),
    ("customer_notes_01a",                 process_customer_notes),
    ("customer_tags_01b",                  process_customer_tags),
    ("class_sessions_02",                  process_class_sessions),
    ("membership_instances_03",            process_membership_instances),
    ("credit_transactions_04",             process_credit_transactions),
    ("credit_transactions_orders_04a",     process_credit_transactions_orders),
    ("membership_transactions_05",         process_membership_transactions),
    ("membership_transactions_orders_05a", process_membership_transactions_orders),
    ("orders_06",                          process_orders),
    ("order_lines_07",                     process_order_lines),
    ("reservations_08",                    process_reservations),
]

VACUUM_TABLES = [
    "customers", "orders", "order_lines", "reservations",
    "class_sessions", "credit_transactions", "credit_transactions_orders",
    "membership_transactions", "membership_transactions_orders", "membership_instances",
]

# Parent tables the processors join by business key, now that the location predicate is
# gone: (table, business key column). Two rows sharing (account_id, key) would make those
# joins fan out and insert a row per duplicate — see _check_duplicate_parents.
PARENT_KEYS = [
    ("customers", "customer_id"),
    ("class_sessions", "class_session_id"),
    ("orders", "order_id"),
    ("credit_transactions", "credit_transactions_id"),
    ("credit_transactions_orders", "credit_transactions_id"),
    ("membership_transactions", "membership_transactions_id"),
    ("membership_transactions_orders", "membership_transactions_id"),
    ("membership_instances", "membership_instances_id"),
]


def _engine():
    return create_engine(settings.DATABASE_URL, pool_pre_ping=True)


# ── Duplicate-parent pre-flight ─────────────────────────────────────────────

def _check_duplicate_parents(engine, account_id) -> dict:
    """
    Find parent tables holding more than one row for the same (account_id, business key).

    The onboarding pipeline scopes every join and every "already exists" check to one
    location, so it can (and historically does) create two rows for the same customer_id
    under two locations of one account. The bulk pipeline drops the location predicate —
    which stops NEW duplicates being created, but means an INNER JOIN onto a
    pre-existing duplicate pair fans out and inserts the child row twice.

    Reported per table so a promote can refuse rather than silently double-insert.
    """
    findings = {}
    with engine.connect() as conn:
        for table, key_col in PARENT_KEYS:
            row = conn.execute(text(f"""
                SELECT COUNT(*) AS dup_keys, COALESCE(SUM(n) - COUNT(*), 0) AS extra_rows
                FROM (
                  SELECT {key_col}, COUNT(*) AS n
                  FROM {table}
                  WHERE account_id = :account_id
                    AND {key_col} IS NOT NULL
                  GROUP BY {key_col}
                  HAVING COUNT(*) > 1
                ) d
            """), {"account_id": account_id}).fetchone()

            if row and int(row[0]) > 0:
                findings[table] = {
                    "duplicate_keys": int(row[0]),
                    "extra_rows": int(row[1]),
                    "key_column": key_col,
                }

    return findings


# ── Class dates ─────────────────────────────────────────────────────────────

def _update_customer_class_dates(engine, account_id, update: bool = True) -> dict:
    """
    Recompute last_class_date / next_class_date from the account's promoted reservations.

    Unchanged from handlers/stg_to_db.py — it was already account-scoped with no location
    predicate. This, not Silver, is the authority for these two columns: s3_to_stg_bulk
    lists them in the customers spec's `no_update` set precisely so the stale update does
    not fight this pass.
    """
    count_sql = text("""
        WITH last_class AS (
            SELECT
                r.customer_ref_id,
                MAX(r.check_in_date) AS last_class_date
            FROM reservations r
            WHERE r.account_id    = :account_id
              AND r.status        = 'check in'
              AND r.deleted_at    IS NULL
              AND r.check_in_date IS NOT NULL
            GROUP BY r.customer_ref_id
        ),
        next_class AS (
            SELECT DISTINCT ON (r.customer_ref_id)
                r.customer_ref_id,
                cs.start_datetime AS next_class_date
            FROM reservations r
            INNER JOIN class_sessions cs
                ON cs.id          = r.class_session_ref_id
               AND cs.deleted_at  IS NULL
            WHERE r.account_id = :account_id
              AND r.status     = 'pending'
              AND r.deleted_at IS NULL
              AND cs.start_datetime > NOW()
            ORDER BY r.customer_ref_id, cs.start_datetime ASC
        )
        SELECT COUNT(*) FROM customers c
        LEFT JOIN last_class lc ON lc.customer_ref_id = c.id
        LEFT JOIN next_class nc ON nc.customer_ref_id = c.id
        WHERE c.account_id = :account_id
          AND (
            c.last_class_date IS DISTINCT FROM lc.last_class_date
            OR
            c.next_class_date IS DISTINCT FROM nc.next_class_date
          )
    """)

    update_sql = text("""
        WITH last_class AS (
            SELECT
                r.customer_ref_id,
                MAX(r.check_in_date) AS last_class_date
            FROM reservations r
            WHERE r.account_id    = :account_id
              AND r.status        = 'check in'
              AND r.deleted_at    IS NULL
              AND r.check_in_date IS NOT NULL
            GROUP BY r.customer_ref_id
        ),
        next_class AS (
            SELECT DISTINCT ON (r.customer_ref_id)
                r.customer_ref_id,
                cs.start_datetime AS next_class_date
            FROM reservations r
            INNER JOIN class_sessions cs
                ON cs.id          = r.class_session_ref_id
               AND cs.deleted_at  IS NULL
            WHERE r.account_id = :account_id
              AND r.status     = 'pending'
              AND r.deleted_at IS NULL
              AND cs.start_datetime > NOW()
            ORDER BY r.customer_ref_id, cs.start_datetime ASC
        )
        UPDATE customers c
        SET
            last_class_date = lc.last_class_date,
            next_class_date = nc.next_class_date,
            updated_at      = NOW(),
            updated_by      = -1
        FROM
            (SELECT id FROM customers WHERE account_id = :account_id) target
        LEFT JOIN last_class lc ON lc.customer_ref_id = target.id
        LEFT JOIN next_class nc ON nc.customer_ref_id = target.id
        WHERE c.id = target.id
          AND (
            c.last_class_date IS DISTINCT FROM lc.last_class_date
            OR
            c.next_class_date IS DISTINCT FROM nc.next_class_date
          )
    """)

    with engine.begin() as conn:
        expected = conn.execute(count_sql, {"account_id": account_id}).scalar()

    logger.info(f"[class_dates] expected to update: {expected} customers for account_id={account_id}")

    if not update:
        return {"expected": expected, "updated": None, "match": None}

    with engine.begin() as conn:
        updated = conn.execute(update_sql, {"account_id": account_id}).rowcount

    match = updated == expected
    if match:
        logger.info(f"[class_dates] updated {updated} customers — count matches expected")
    else:
        diff_pct = abs(expected - updated) / expected * 100 if expected else 100
        level = logger.warning if diff_pct < 1 else logger.error
        level(f"[class_dates] count mismatch ({diff_pct:.2f}%) — expected {expected}, updated {updated}")

    return {"expected": expected, "updated": updated, "match": match}


# ── Post-processing ─────────────────────────────────────────────────────────

async def post_processing_step(account_id: int, is_post_process: bool) -> bool:
    """
    Optional downstream EZTexting sync. Off by default for bulk (see core/bulk_config) —
    it is an onboarding concern, and a delta run would fire it once per account per run.
    """
    if not is_post_process:
        logger.info(f"Post-processing skipped for account_id={account_id}")
        return True

    logger.info(f"Post-processing starting for account_id={account_id}")
    try:
        from clients.api_client import AnalyticsAPIClient
        from clients.lambda_auth_client import get_lambda_invoker

        token_service_lambda_name = getattr(settings, "TOKEN_SERVICE_LAMBDA_NAME", None)

        if token_service_lambda_name:
            lambda_invoker = get_lambda_invoker()
            access_token = await lambda_invoker.get_access_token_from_lambda(token_service_lambda_name)
            if not access_token:
                logger.error("Failed to get access token from Lambda")
                return False
        else:
            from clients.auth_client import Auth0Client
            async with Auth0Client() as auth_client:
                access_token = await auth_client.get_access_token()

        async with AnalyticsAPIClient() as api_client:
            response = await api_client.post_endpoint(
                endpoint="eztexting/sync-with-eztexting",
                account_id=str(account_id),
                access_token=access_token,
            )
        logger.info(f"Post-processing succeeded: {response}")
        return True

    except Exception as e:
        logger.error(f"Post-processing failed: {e}", exc_info=True)
        return False


# ── Actions ─────────────────────────────────────────────────────────────────

def _vacuum(event: dict, engine) -> dict:
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for table in VACUUM_TABLES:
            conn.execute(text(f"VACUUM ANALYZE {table}"))
            logger.info(f"[vacuum] VACUUM ANALYZE {table} done")
    logger.info("[vacuum] all tables vacuumed — waiting 10s for stats to settle")
    time.sleep(10)
    return {"status": "success", "action": "vacuum", "tables": VACUUM_TABLES}


def _verify(event: dict, engine) -> dict:
    account_id = event.get("account_id")
    if account_id is None:
        raise ValueError("verify requires account_id")

    duplicates = _check_duplicate_parents(engine, int(account_id))
    return {
        "status": "success" if not duplicates else "error",
        "action": "verify",
        "account_id": int(account_id),
        "duplicate_parents": duplicates,
    }


async def _promote(event: dict, engine) -> dict:
    account_id = event.get("account_id")
    if account_id is None:
        raise ValueError("promote requires account_id")
    account_id = int(account_id)

    start_time = time.time()
    logger.info(f"stg_to_main_bulk starting — account_id={account_id}")

    duplicates = _check_duplicate_parents(engine, account_id)
    if duplicates and not event.get("allow_duplicate_parents", False):
        # Refusing beats double-inserting: the joins below would fan out one child row per
        # duplicate parent. Deduplicate the parent rows, or re-invoke with
        # {"allow_duplicate_parents": true} if the fan-out is understood and acceptable.
        raise RuntimeError(
            f"account {account_id} has duplicate parent business keys "
            f"{json.dumps(duplicates)} — location-free joins would fan out. "
            f'Pass {{"allow_duplicate_parents": true}} to promote anyway.'
        )

    successful_tables = []
    failed_tables = []
    total_processed = 0

    for table_name, process_func in PROCESSING_ORDER:
        t0 = time.time()
        try:
            logger.info(f"[{table_name}] starting...")
            result = await process_func(account_id, engine)
            inserted = result.get("inserted_records", 0) if isinstance(result, dict) else result
            elapsed = round(time.time() - t0, 2)
            logger.info(f"[{table_name}] done — {inserted} inserted in {elapsed}s")
            successful_tables.append({
                "table": table_name, "status": "success",
                "inserted_records": inserted, "elapsed_seconds": elapsed,
            })
            total_processed += inserted
        except Exception as e:
            elapsed = round(time.time() - t0, 2)
            logger.error(f"[{table_name}] failed in {elapsed}s: {e} — stopping pipeline")
            failed_tables.append({
                "table": table_name, "status": "failed",
                "error": str(e), "elapsed_seconds": elapsed,
            })
            break

    total_duration = round(time.time() - start_time, 2)
    total_tables = len(PROCESSING_ORDER)
    success_count = len(successful_tables)

    logger.info(
        f"stg_to_main_bulk done — account_id={account_id}, "
        f"{success_count}/{total_tables} tables, {total_processed} records, {total_duration}s"
    )

    if failed_tables:
        # Fail so Step Functions retries this account's Map branch, then routes to a Fail
        # state (redrive). Other accounts' branches are unaffected.
        raise Exception(
            f"stg_to_main_bulk: {len(failed_tables)}/{total_tables} tables failed "
            f"for account {account_id}: {failed_tables}"
        )

    try:
        class_dates = _update_customer_class_dates(engine, account_id, update=True)
    except Exception as e:
        logger.error(f"[class_dates] failed for account_id={account_id}: {e}")
        class_dates = {"expected": None, "updated": None, "match": None}

    post_ok = await post_processing_step(account_id, settings.IS_POST_PROCESS)
    if not post_ok:
        raise Exception("stg_to_main_bulk: all tables succeeded but post-processing failed")

    return {
        "status": "success",
        "action": "promote",
        "account_id": account_id,
        "duplicate_parents": duplicates,
        "summary": {
            "total_tables": total_tables,
            "successful_tables": success_count,
            "failed_tables": 0,
            "total_records_inserted": total_processed,
            "class_dates_expected": class_dates["expected"],
            "class_dates_updated": class_dates["updated"],
            "class_dates_match": class_dates["match"],
            "elapsed_seconds": total_duration,
        },
        "table_results": successful_tables,
    }


def lambda_handler(event, context=None):
    event = event or {}
    action = event.get("action", "promote")
    engine = _engine()
    start = time.time()

    try:
        if action == "promote":
            response = asyncio.run(_promote(event, engine))
        elif action == "vacuum":
            response = _vacuum(event, engine)
        elif action == "verify":
            response = _verify(event, engine)
        else:
            raise ValueError(
                f"unknown action {action!r} — expected one of ['promote', 'vacuum', 'verify']"
            )

        response["elapsed_time_seconds"] = round(time.time() - start, 2)
        if response["status"] == "error":
            raise RuntimeError(f"{action} failed: {json.dumps(response, default=str)[:900]}")
        return response

    except Exception as e:
        logger.error(f"[{action}] failed after {round(time.time() - start, 2)}s: {e}")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    # ./stg_to_main_bulk.py promote 1410
    cli_event = {"action": sys.argv[1] if len(sys.argv) > 1 else "promote"}
    if len(sys.argv) > 2:
        cli_event["account_id"] = int(sys.argv[2])
    result = lambda_handler(cli_event)
    print(json.dumps(result, indent=2, default=str))
    if result["status"] != "success":
        sys.exit(1)
