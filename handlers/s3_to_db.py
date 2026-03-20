import sys
import asyncio
import time
import logging

from core.config import settings
from core.logger import setup_logging
from clients.db_client import DatabaseManager
from db_services.main_bulk_insert import etl_all_tables

setup_logging(log_level="INFO")
logger = logging.getLogger(__name__)


def _notify_stage2(account_id, etl_success: bool, failed_tables: list, elapsed: float):
    if not settings.TEAMS_ENABLED:
        return
    try:
        from notifiers.onboard_notifier import Stage2Notifier
        asyncio.run(Stage2Notifier().notify({
            "account_id":    account_id,
            "etl_success":   etl_success,
            "failed_tables": failed_tables,
            "elapsed":       f"{elapsed}s",
        }))
    except Exception as e:
        logger.error(f"Stage2Notifier failed: {e}")


def lambda_handler(event, context=None):
    account_id = event.get("account_id")
    location_id = event.get("location_id")
    check_stale = settings.CHECK_STALE_DATA

    if not account_id:
        logger.error("account_id is required")
        return {"status": "error", "error": "account_id is required"}
    if not location_id:
        logger.error("location_id is required")
        return {"status": "error", "error": "location_id is required"}

    db = DatabaseManager()
    db.initialize()
    start = time.time()

    try:
        logger.info(f"ETL stage 2 starting — account_id={account_id}, location_id={location_id}, check_stale={check_stale}")
        results = etl_all_tables(settings.S3_BUCKET, account_id, db.engine, check_stale=check_stale)
        elapsed = round(time.time() - start, 2)

        etl_success = results.get("etl_success", False)
        stale_success = results.get("stale_update_success", True)
        failed_tables = results.get("failed_tables", [])
        stale_results = results.get("stale_results")
        overall_success = etl_success and stale_success

        logger.info(f"ETL stage 2 done — etl={etl_success}, stale={stale_success}, "
                    f"failed={failed_tables}, elapsed={elapsed}s")

        response = {
            "status": "success" if overall_success else "error",
            "account_id": account_id,
            "location_id": location_id,
            "elapsed_time_seconds": elapsed,
            "etl_success": etl_success,
            "stale_check_enabled": check_stale,
        }

        if not etl_success:
            response["failed_tables"] = failed_tables

        if check_stale and stale_results:
            response["stale_update_success"] = stale_success
            response["stale_tables"] = {
                tbl: {
                    "expected": r.get("expected", 0),
                    "updated": r.get("updated", 0),
                    "success": r.get("success", False),
                    "elapsed_time_seconds": r.get("elapsed_time_seconds", 0),
                }
                for tbl, r in stale_results.get("tables", {}).items()
            }

        _notify_stage2(account_id, overall_success, failed_tables, elapsed)

        if not overall_success:
            raise Exception(f"ETL stage 2 failed: {failed_tables}")

        return response

    except Exception as e:
        elapsed = round(time.time() - start, 2)
        logger.error(f"Lambda failed — account={account_id}: {e}")
        _notify_stage2(account_id, False, [], elapsed)
        return {
            "status": "error",
            "error": str(e),
            "account_id": account_id,
            "location_id": location_id,
            "elapsed_time_seconds": elapsed,
        }
    finally:
        db.close()


if __name__ == "__main__":
    event = {
        "account_id": int(sys.argv[1]) if len(sys.argv) > 1 else 89,
        "location_id": int(sys.argv[2]) if len(sys.argv) > 2 else 48717,
    }
    print(lambda_handler(event))
