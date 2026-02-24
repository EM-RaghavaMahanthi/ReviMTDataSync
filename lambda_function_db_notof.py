import sys
import logging
import time
from db_services.main_bulk_insert_service import etl_all_tables
from core.config import settings
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lambda_function_db")

def lambda_handler(event, context=None):
    import awswrangler as wr
    account_id = event.get('account_id')
    location_id = event.get('location_id')
    if(account_id is None):
        logger.error("account_id is required")
        return {
            'status': 'error',
            'error': 'account_id is required'
        }
    if(location_id is None):
        logger.error("location_id is required")
        return {
            'status': 'error',
            'error': 'location_id is required'
        }
    bucket = settings.S3_BUCKET
    engine = create_engine(settings.DATABASE_URL)
    try:
        start_time = time.time()
        logger.info(f"Starting ETL stage 2 for account_id={account_id}, location_id={location_id}")
        print(f"Starting ETL stage 2 for account_id={account_id}, location_id={location_id}")
        etl_result = etl_all_tables(bucket, account_id , engine)
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)

        success = etl_result.get('success', False)
        table_row_counts = etl_result.get('table_row_counts', {})
        failed_tables = etl_result.get('failed_tables', [])
        successful_tables = etl_result.get('successful_tables', [])

        teams_enabled = bool(getattr(settings, "teams_enabled", False))
        print("teams_enabled", teams_enabled)
        if teams_enabled:
            try:
                from adapter_src.onboard_notifier import S3toStgNotifier
                card = {
                    "stage": "S3toStg(Stage 2)",
                    "account_id": account_id,
                    "location_id": location_id,
                    "status": "✅ SUCCESS" if success else "❌ ERROR",
                    "elapsed_time": f"{elapsed_time} seconds",
                    "table_row_counts": table_row_counts,
                    "failed_tables": failed_tables,
                    "successful_tables": successful_tables
                }
                import asyncio
                asyncio.run(S3toStgNotifier().notify(card))
            except Exception as notify_exc:
                logger.error(f"S3toStgNotifier failed: {notify_exc}")

        if success:
            logger.info(f"ETL stage 2 completed: Loaded all tables as staged (Time elapsed: {elapsed_time}s)")
            return {
                'status': 'success',
                'account_id': account_id,
                'location_id': location_id,
                'elapsed_time_seconds': elapsed_time,
                'table_row_counts': table_row_counts
            }
        else:
            error_msg = f"ETL stage 2 failed: Some tables failed to process (Time elapsed: {elapsed_time}s)"
            logger.error(error_msg)
            return {
                'status': 'error',
                'error': error_msg,
                'account_id': account_id,
                'location_id': location_id,
                'elapsed_time_seconds': elapsed_time,
                'table_row_counts': table_row_counts,
                'failed_tables': failed_tables
            }
    except Exception as e:
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        logger.error(f"Lambda execution failed: {e} (Time elapsed: {elapsed_time}s)")
        teams_enabled = bool(getattr(settings, "teams_enabled", False))
        print("teams_enabled", teams_enabled)
        if teams_enabled:
            try:
                from adapter_src.onboard_notifier import S3toStgNotifier
                card = {
                    "stage": "S3toStg(Stage 2)",
                    "account_id": event.get('account_id', '-'),
                    "location_id": event.get('location_id', '-'),
                    "status": "❌ ERROR",
                    "elapsed_time": f"{elapsed_time} seconds",
                    "table_row_counts": {},
                    "failed_tables": [],
                    "successful_tables": [],
                    "error": str(e)
                }
                import asyncio
                asyncio.run(S3toStgNotifier().notify(card))
            except Exception as notify_exc:
                logger.error(f"S3toStgNotifier failed in exception: {notify_exc}")
        return {
            'status': 'error',
            'error': str(e),
            'account_id': event.get('account_id', '-'),
            'location_id': event.get('location_id', '-'),
            'elapsed_time_seconds': elapsed_time
        }

# For local testing
if __name__ == "__main__":
    event = {
        'account_id': int(sys.argv[1]) if len(sys.argv) > 1 else 89,
        'location_id': int(sys.argv[2]) if len(sys.argv) > 2 else 48717
    }
    print(lambda_handler(event))
