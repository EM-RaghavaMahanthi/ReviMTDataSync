"""
Bulk Stage 2 — staging → production tables, every staged account in one invocation.

Stage 1 loads Silver into staging and UPDATEs what already exists; this is the other half,
and it INSERTs what does not. Between them they cover the two shortfalls `reconcile`
reports: `differing` for stage 1, `missing` here.

Account-independent, like stage 1. Staging is unique on (account_id, business key), so one
pass covers every account and every location. Table order is the dependency order and is
strict — each insert resolves its parent's `*_ref_id` by joining the parent's target table,
so parents must land first.

One action per invocation; the Step Function sequences them:

  vacuum   VACUUM ANALYZE the target tables once, before the inserts.
  promote  (default) run the processors for every staged account. `account_ids` narrows it.
           A duplicate parent anywhere fails the run before a single row is written.
  verify   duplicate-parent pre-flight, writes nothing. Run it before a promote.

Event:
  {"action": "promote"}                                  everything staged
  {"action": "promote", "account_ids": [1410, 1411]}     narrowed
  {"action": "promote", "allow_duplicate_parents": true} override the gate
  {"action": "vacuum"}
  {"action": "verify"}

Class dates are not recomputed here — a separate script owns them for all accounts, and
`customers` is cfg.RDS_OWNED, so neither bulk lambda writes to it.

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
from s3_to_stg_bulk import config as cfg

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

def _check_duplicate_parents(engine, account_ids: list = None, sample: int = 5) -> dict:
    """
    Find parent tables holding more than one row for the same (account_id, business key).

    `(account_id, business key)` is unique by policy, maintained by a script that deletes
    duplicates and repairs any *_ref_id left pointing at the removed row. So a hit here is
    an invariant violation, not a condition to work around — see the caller, which fails
    the whole run rather than skipping the account.

    It matters because every child insert resolves its parent with an INNER JOIN on the
    business key. Two parent rows for one key return two rows for one staging row, and the
    child is inserted twice. The onboarding pipeline can create such pairs: it scopes its
    "already exists" check per location, so one customer_id can end up with a row under
    each of an account's locations.

    Account-independent: one query per parent table for the whole run, not one per table
    per account. `account_ids` narrows it when given; omit it to check everything.
    Offending accounts are named so the dedup script has somewhere to start.
    """
    findings = {}
    scope = "AND account_id = ANY(:ids)" if account_ids else ""
    params = {"ids": account_ids} if account_ids else {}

    with engine.connect() as conn:
        for table, key_col in PARENT_KEYS:
            row = conn.execute(text(f"""
                SELECT COUNT(*) AS dup_keys, COALESCE(SUM(n) - COUNT(*), 0) AS extra_rows
                FROM (
                  SELECT account_id, {key_col}, COUNT(*) AS n
                  FROM {table}
                  WHERE {key_col} IS NOT NULL {scope}
                  GROUP BY account_id, {key_col}
                  HAVING COUNT(*) > 1
                ) d
            """), params).fetchone()

            if row and int(row[0]) > 0:
                offenders = [
                    {"account_id": int(r[0]), key_col: r[1], "rows": int(r[2])}
                    for r in conn.execute(text(f"""
                        SELECT account_id, {key_col}, COUNT(*) AS n
                        FROM {table}
                        WHERE {key_col} IS NOT NULL {scope}
                        GROUP BY account_id, {key_col}
                        HAVING COUNT(*) > 1
                        ORDER BY COUNT(*) DESC
                        LIMIT {int(sample)}
                    """), params).fetchall()
                ]
                findings[table] = {
                    "duplicate_keys": int(row[0]),
                    "extra_rows": int(row[1]),
                    "key_column": key_col,
                    "sample": offenders,
                }
                logger.error(
                    f"[duplicate_parents] {table}: {row[0]} duplicated "
                    f"{key_col} values, {row[1]} extra rows"
                )

    return findings


# Class dates are NOT recomputed here. A separate script updates last_class_date /
# next_class_date for every account at once and is the authority for them; customers is
# cfg.RDS_OWNED, so neither bulk lambda writes to that table at all.


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
    """
    Read-only: would a promote be blocked right now?

    `account_ids` narrows the check; omit it to cover every account, which is the same
    scope `promote` uses. Reports rather than raises — it exists to be run before a
    promote, so an operator can hand the offenders to the dedup script.
    """
    account_ids = [int(a) for a in (event.get("account_ids") or [])]
    if event.get("account_id") is not None:
        account_ids.append(int(event["account_id"]))

    duplicates = _check_duplicate_parents(engine, account_ids or None)
    return {
        "status": "success" if not duplicates else "blocked",
        "action": "verify",
        "account_ids": account_ids or "all",
        "duplicate_parents": duplicates,
        "promote_would_run": not duplicates,
    }


def _staged_account_ids(engine) -> list:
    """
    Every account with rows in staging. Derived from cfg.STAGING_ORDER rather than a
    hardcoded list, so it follows stage 1's table set automatically.
    """
    parts = [
        f'SELECT DISTINCT account_id FROM "{cfg.staging_name(t)}"'
        for t in cfg.STAGING_ORDER
    ]
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT account_id FROM (" + " UNION ".join(parts) + ") u "
            "WHERE account_id IS NOT NULL ORDER BY account_id"
        )).fetchall()
    return [int(r[0]) for r in rows]


async def _promote(event: dict, engine) -> dict:
    # Account-independent by default: promote covers whatever stage 1 staged. `account_ids`
    # narrows it; the old single `account_id` is still accepted so a Map branch or a manual
    # one-account run keeps working.
    account_ids = [int(a) for a in (event.get("account_ids") or [])]
    if event.get("account_id") is not None:
        account_ids.append(int(event["account_id"]))
    if not account_ids:
        account_ids = _staged_account_ids(engine)
    if not account_ids:
        return {
            "status": "success", "action": "promote", "account_ids": [],
            "summary": {"note": "nothing staged"},
        }

    start_time = time.time()
    logger.info(f"stg_to_main_bulk starting — {len(account_ids)} accounts")

    # One gate for the whole run, before a single row is written. (account_id, business key)
    # is unique by policy and a script maintains it, so a hit here is an invariant
    # violation: fail everything rather than promote some accounts and skip others. A
    # partial promote across sixty accounts is far harder to reason about than a clean stop,
    # and the remedy is one script run away.
    duplicates = _check_duplicate_parents(engine, account_ids)
    if duplicates and not event.get("allow_duplicate_parents", False):
        raise RuntimeError(
            f"duplicate parent business keys found — the FK joins would fan out and insert "
            f"child rows twice. Nothing was written. Run the dedup script, then re-run. "
            f'Override with {{"allow_duplicate_parents": true}} only if the fan-out is '
            f"understood and acceptable. Findings: {json.dumps(duplicates)}"
        )

    successful_tables = []
    failed_tables = []
    total_processed = 0

    # Scaffolding until step 3 makes the processors account-independent. The gate above
    # already runs once for the whole set; the processors still take one account, so they
    # are looped per table. Table order is the dependency order and must be preserved, so
    # tables are the outer loop — every account gets its parents before any child.
    #
    # A failure stops the whole run, not just that account: the tables after this one
    # depend on it, so continuing would insert children whose parents are missing. Same
    # strictness the per-account version had, applied to the whole set.
    for table_name, process_func in PROCESSING_ORDER:
        t_table = time.time()
        table_inserted = 0
        failed = None

        for account_id in account_ids:
            try:
                result = await process_func(account_id, engine)
                table_inserted += (
                    result.get("inserted_records", 0) if isinstance(result, dict) else result
                )
            except Exception as e:
                logger.error(f"[{table_name}] account={account_id} failed: {e}")
                failed = {"account_id": account_id, "error": str(e)}
                break

        elapsed = round(time.time() - t_table, 2)
        if failed:
            failed_tables.append({
                "table": table_name, "status": "failed",
                "error": failed["error"], "account_id": failed["account_id"],
                "inserted_before_failure": table_inserted, "elapsed_seconds": elapsed,
            })
            logger.error(f"[{table_name}] failed in {elapsed}s — stopping pipeline")
            break

        logger.info(
            f"[{table_name}] done — {table_inserted} inserted across "
            f"{len(account_ids)} accounts in {elapsed}s"
        )
        successful_tables.append({
            "table": table_name, "status": "success",
            "inserted_records": table_inserted, "elapsed_seconds": elapsed,
        })
        total_processed += table_inserted

    total_duration = round(time.time() - start_time, 2)
    total_tables = len(PROCESSING_ORDER)
    success_count = len(successful_tables)

    logger.info(
        f"stg_to_main_bulk done — {len(account_ids)} accounts, "
        f"{success_count}/{total_tables} tables, {total_processed} records, {total_duration}s"
    )

    if failed_tables:
        # Fail the invocation so Step Functions retries, then routes to a Fail state
        # (redrive). Tables after the failed one depend on it, so nothing further ran.
        raise Exception(
            f"stg_to_main_bulk: {len(failed_tables)}/{total_tables} tables failed "
            f"across {len(account_ids)} accounts: {failed_tables}"
        )

    # Account-scoped and off by default for bulk (IS_POST_PROCESS=false) — the EZTexting
    # sync is an onboarding concern. Kept per account rather than dropped, so turning the
    # flag on still behaves the way it did.
    for account_id in account_ids:
        if not await post_processing_step(account_id, settings.IS_POST_PROCESS):
            raise Exception(
                f"stg_to_main_bulk: all tables succeeded but post-processing failed "
                f"for account {account_id}"
            )

    return {
        "status": "success",
        "action": "promote",
        "account_ids": account_ids,
        "duplicate_parents": duplicates,
        "summary": {
            "accounts": len(account_ids),
            "total_tables": total_tables,
            "successful_tables": success_count,
            "failed_tables": 0,
            "total_records_inserted": total_processed,
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
