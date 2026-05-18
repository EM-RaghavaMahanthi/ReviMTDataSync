import sys
import asyncio
import time
import logging
from core.stg_db_config import settings
from sqlalchemy import create_engine, text

from core.logger import setup_logging
setup_logging()

from stg_db_services.customers import process_customers
from stg_db_services.class_sessions import process_class_sessions
from stg_db_services.membership_instances import process_membership_instances
from stg_db_services.credit_transactions import process_credit_transactions
from stg_db_services.credit_transactions_orders import process_credit_transactions_orders
from stg_db_services.membership_transactions import process_membership_transactions
from stg_db_services.membership_transactions_orders import process_membership_transactions_orders
from stg_db_services.orders import process_orders
from stg_db_services.order_lines.base import process_order_lines
from stg_db_services.reservations.base import process_reservations

logger = logging.getLogger(__name__)


async def _notify_stage3(account_id, status: str, success_count: int, total_tables: int,
                         total_inserted: int, failed_tables: list, elapsed: float):
    if not settings.TEAMS_ENABLED:
        return
    try:
        from notifiers.onboard_notifier import Stage3Notifier
        failed_table = failed_tables[0]["table"] if failed_tables else None
        await Stage3Notifier().notify({
            "account_id":     account_id,
            "status":         status,
            "success_count":  success_count,
            "total_tables":   total_tables,
            "total_inserted": total_inserted,
            "failed_table":   failed_table,
            "elapsed":        f"{elapsed}s",
        })
    except Exception as e:
        logger.error(f"Stage3Notifier failed: {e}")

def _update_customer_class_dates(engine, account_id, update: bool = False) -> dict:
    """
    Count customers whose last_class_date or next_class_date differs from computed values.
    If update=True, runs the UPDATE and validates rowcount matches expected.
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
        result = conn.execute(update_sql, {"account_id": account_id})
        updated = result.rowcount

    match = updated == expected
    if match:
        logger.info(f"[class_dates] updated {updated} customers — count matches expected")
    else:
        diff_pct = abs(expected - updated) / expected * 100 if expected else 100
        if diff_pct < 1:
            logger.warning(f"[class_dates] minor count discrepancy ({diff_pct:.2f}%) — expected {expected}, updated {updated}")
        else:
            logger.error(f"[class_dates] count mismatch ({diff_pct:.2f}%) — expected {expected}, updated {updated}")

    return {"expected": expected, "updated": updated, "match": match}


VACUUM_TABLES = [
    "customers", "orders", "order_lines", "reservations",
    "class_sessions", "credit_transactions", "credit_transactions_orders",
    "membership_transactions", "membership_transactions_orders", "membership_instances",
]


def _vacuum_tables(engine):
    import time
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for table in VACUUM_TABLES:
            conn.execute(text(f"VACUUM ANALYZE {table}"))
            logger.info(f"[vacuum] VACUUM ANALYZE {table} done")
    logger.info("[vacuum] all tables vacuumed — waiting 10s for stats to settle")
    time.sleep(10)


PROCESSING_ORDER = [
    ("customers_01",                    process_customers),
    ("class_sessions_02",               process_class_sessions),
    ("membership_instances_03",         process_membership_instances),
    ("credit_transactions_04",          process_credit_transactions),
    ("credit_transactions_orders_04a",  process_credit_transactions_orders),
    ("membership_transactions_05",      process_membership_transactions),
    ("membership_transactions_orders_05a", process_membership_transactions_orders),
    ("orders_06",                       process_orders),
    ("order_lines_07",                  process_order_lines),
    ("reservations_08",                 process_reservations),
]


async def post_processing_step(account_id: int, is_post_process: bool = False) -> bool:
    if not is_post_process:
        logger.info(f"Post-processing skipped for account_id={account_id}")
        return True

    logger.info(f"Post-processing starting for account_id={account_id}")
    try:
        from clients.api_client import AnalyticsAPIClient
        from clients.lambda_auth_client import get_lambda_invoker

        token_service_lambda_name = getattr(settings, "TOKEN_SERVICE_LAMBDA_NAME", None)

        if token_service_lambda_name:
            logger.info(f"Getting token from Lambda: {token_service_lambda_name}")
            lambda_invoker = get_lambda_invoker()
            access_token = await lambda_invoker.get_access_token_from_lambda(token_service_lambda_name)
            if not access_token:
                logger.error("Failed to get access token from Lambda")
                return False
        else:
            logger.info("TOKEN_SERVICE_LAMBDA_NAME not set — falling back to direct Auth0 call")
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


async def async_stg_to_db_handler(event, context=None):
    start_time = time.time()
    account_id = event.get("account_id")
    location_id = event.get("location_id")
    is_post_process = settings.IS_POST_PROCESS

    if account_id is None:
        logger.error("account_id is required")
        return {"status": "error", "error": "account_id is required"}
    if location_id is None:
        logger.error("location_id is required")
        return {"status": "error", "error": "location_id is required"}

    engine = create_engine(settings.DATABASE_URL)

    try:
        logger.info(f"stg_to_db starting — account_id={account_id}, location_id={location_id}")

        _vacuum_tables(engine)

        successful_tables = []
        failed_tables = []
        total_processed = 0

        for table_name, process_func in PROCESSING_ORDER:
            t0 = time.time()
            try:
                logger.info(f"[{table_name}] starting...")
                result = await process_func(account_id, location_id, engine)
                inserted = result.get("inserted_records", 0) if isinstance(result, dict) else result
                elapsed = round(time.time() - t0, 2)
                logger.info(f"[{table_name}] done — {inserted} inserted in {elapsed}s")
                successful_tables.append({"table": table_name, "status": "success", "inserted_records": inserted, "elapsed_seconds": elapsed})
                total_processed += inserted
            except Exception as e:
                elapsed = round(time.time() - t0, 2)
                logger.error(f"[{table_name}] failed in {elapsed}s: {e} — stopping pipeline")
                failed_tables.append({"table": table_name, "status": "failed", "error": str(e), "elapsed_seconds": elapsed})
                break

        total_duration = round(time.time() - start_time, 2)
        total_tables = len(PROCESSING_ORDER)
        success_count = len(successful_tables)
        failure_count = len(failed_tables)

        logger.info(f"stg_to_db done — {success_count}/{total_tables} tables, {total_processed} records, {total_duration}s")

        if failure_count > 0:
            await _notify_stage3(account_id, "error", success_count, total_tables,
                                 total_processed, failed_tables, total_duration)
            return {
                "status": "error",
                "account_id": account_id,
                "location_id": location_id,
                "error": f"{failure_count}/{total_tables} tables failed",
                "post_process_successful": False,
                "summary": {
                    "total_tables": total_tables,
                    "successful_tables": success_count,
                    "failed_tables": failure_count,
                    "total_records_inserted": total_processed,
                    "elapsed_seconds": total_duration,
                },
                "table_results": successful_tables + failed_tables,
            }

        try:
            class_dates_result = _update_customer_class_dates(engine, account_id, update=True)
        except Exception as e:
            logger.error(f"[class_dates] failed for account_id={account_id}: {e}")
            class_dates_result = {"expected": None, "updated": None, "match": None}

        post_ok = await post_processing_step(account_id, is_post_process)
        if not post_ok:
            return {
                "status": "error",
                "account_id": account_id,
                "location_id": location_id,
                "error": "All tables succeeded but post-processing failed",
                "post_process_successful": False,
                "summary": {
                    "total_tables": total_tables,
                    "successful_tables": success_count,
                    "failed_tables": 0,
                    "total_records_inserted": total_processed,
                    "elapsed_seconds": total_duration,
                },
                "table_results": successful_tables,
            }

        await _notify_stage3(account_id, "success", success_count, total_tables,
                             total_processed, [], total_duration)
        return {
            "status": "success",
            "account_id": account_id,
            "location_id": location_id,
            "is_post_process": settings.IS_POST_PROCESS,
            "post_process_successful": post_ok,
            "summary": {
                "total_tables": total_tables,
                "successful_tables": success_count,
                "failed_tables": 0,
                "total_records_inserted": total_processed,
                "class_dates_expected": class_dates_result["expected"],
                "class_dates_updated": class_dates_result["updated"],
                "class_dates_match": class_dates_result["match"],
                "elapsed_seconds": total_duration,
            },
            "table_results": successful_tables,
        }

    except Exception as e:
        elapsed = round(time.time() - start_time, 2)
        logger.error(f"stg_to_db failed completely: {e}")
        await _notify_stage3(account_id, "error", 0, len(PROCESSING_ORDER), 0, [], elapsed)
        return {
            "status": "error",
            "account_id": account_id,
            "location_id": location_id,
            "error": str(e),
            "elapsed_seconds": elapsed,
        }
    finally:
        engine.dispose()


def lambda_handler(event, context=None):
    return asyncio.run(async_stg_to_db_handler(event, context))


if __name__ == "__main__":
    event = {
        "account_id": int(sys.argv[1]) if len(sys.argv) > 1 else 85,
        "location_id": int(sys.argv[2]) if len(sys.argv) > 2 else 48718,
    }
    result = lambda_handler(event)
    print("Result:", result)
    if result["status"] != "success":
        sys.exit(1)
