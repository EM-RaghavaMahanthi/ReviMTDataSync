import sys
import logging
from db_services.main_bulk_insert_service import etl_all_tables
from db_services.run_sql_cmds import run_sql_files_in_order
from core.config import settings
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lambda_function_db")

def lambda_handler(event, context=None):
    account_id = event.get('account_id', 89)
    location_id = event.get('location_id', 48717)
    bucket = settings.S3_BUCKET
    engine = create_engine(settings.DATABASE_URL)
    try:
        logger.info(f"Starting ETL for account_id={account_id}, location_id={location_id}")
        #etl_all_tables(bucket, account_id , engine)
        logger.info("ETL completed. Running post-load SQL commands...")
        #run_sql_files_in_order(account_id, location_id)
        logger.info("Post-load SQL commands completed.")
        return {
            'status': 'success',
            'account_id': account_id,
            'location_id': location_id
        }
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }

# For local testing
if __name__ == "__main__":
    event = {
        'account_id': int(sys.argv[1]) if len(sys.argv) > 1 else 89,
        'location_id': int(sys.argv[2]) if len(sys.argv) > 2 else 48717
    }
    print(lambda_handler(event))
