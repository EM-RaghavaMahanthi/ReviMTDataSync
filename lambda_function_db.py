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
        success = etl_all_tables(bucket, account_id , engine)
        
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        
        if success:
            logger.info(f"ETL stage 2 completed: Loaded all tables as staged (Time elapsed: {elapsed_time}s)")
            return {
                'status': 'success',
                'account_id': account_id,
                'location_id': location_id,
                'elapsed_time_seconds': elapsed_time
            }
        else:
            error_msg = f"ETL stage 2 failed: Some tables failed to process (Time elapsed: {elapsed_time}s)"
            logger.error(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        logger.error(f"Lambda execution failed: {e} (Time elapsed: {elapsed_time}s)")
        return {
            'status': 'error',
            'error': str(e),
            'elapsed_time_seconds': elapsed_time
        }

# For local testing
if __name__ == "__main__":
    event = {
        'account_id': int(sys.argv[1]) if len(sys.argv) > 1 else 89,
        'location_id': int(sys.argv[2]) if len(sys.argv) > 2 else 48717
    }
    print(lambda_handler(event))
