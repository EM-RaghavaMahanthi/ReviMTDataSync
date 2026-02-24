import sys
import logging
import time
from db_services.main_bulk_insert_service import etl_all_tables
from core.config import settings
from core.logger import setup_logging
from sqlalchemy import create_engine

# Setup logging using centralized configuration
setup_logging(log_level="INFO")
logger = logging.getLogger(__name__)

def lambda_handler(event, context=None):
    import awswrangler as wr
    account_id = event.get('account_id')
    location_id = event.get('location_id')
    check_stale = settings.CHECK_STALE_DATA  # Optional parameter for stale data checking
    
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
        logger.info(f"Starting ETL stage 2 for account_id={account_id}, location_id={location_id}, check_stale={check_stale}")
        print(f"Starting ETL stage 2 for account_id={account_id}, location_id={location_id}, check_stale={check_stale}")
        
        # etl_all_tables now returns a dictionary with detailed results
        results = etl_all_tables(bucket, account_id, engine, check_stale=check_stale)
        
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        
        # Extract results
        etl_success = results.get('etl_success', False)
        stale_update_success = results.get('stale_update_success', True)  # True if not checked
        failed_tables = results.get('failed_tables', [])
        stale_results = results.get('stale_results', None)
        
        # Overall success if both ETL and stale updates succeeded
        overall_success = etl_success and stale_update_success

        logger.info(f"ETL stage 2 completed for account_id={account_id}, location_id={location_id} with ETL success={etl_success}, stale_update_success={stale_update_success}, failed_tables={failed_tables}, check_stale={check_stale} (Time elapsed: {elapsed_time}s)")
        
        if overall_success:
            logger.info(f"ETL stage 2 completed successfully: All tables processed (Time elapsed: {elapsed_time}s)")
            response = {
                'status': 'success',
                'account_id': account_id,
                'location_id': location_id,
                'elapsed_time_seconds': elapsed_time,
                'etl_success': etl_success,
                'stale_check_enabled': check_stale
            }
            
            # Add stale update details if check_stale was True
            if check_stale and stale_results:
                response['stale_update_success'] = stale_update_success
                response['stale_tables'] = {}
                logger.info(f"Stale update results: {stale_results}")
                for table_name, table_result in stale_results.get('tables', {}).items():
                    response['stale_tables'][table_name] = {
                        'expected': table_result.get('expected', 0),
                        'updated': table_result.get('updated', 0),
                        'success': table_result.get('success', False),
                        'elapsed_time_seconds': table_result.get('elapsed_time_seconds', 0)
                    }
            
            return response
        else:
            error_details = []
            if not etl_success:
                error_details.append(f"Failed tables: {', '.join(failed_tables)}")
            if check_stale and not stale_update_success:
                error_details.append("Stale data update failed")
            
            error_msg = f"ETL stage 2 failed: {'; '.join(error_details)} (Time elapsed: {elapsed_time}s)"
            logger.error(error_msg)
            
            response = {
                'status': 'error',
                'error': error_msg,
                'account_id': account_id,
                'location_id': location_id,
                'elapsed_time_seconds': elapsed_time,
                'etl_success': etl_success,
                'failed_tables': failed_tables,
                'stale_check_enabled': check_stale
            }
            
            if check_stale and stale_results:
                response['stale_update_success'] = stale_update_success
                response['stale_tables'] = {}
                for table_name, table_result in stale_results.get('tables', {}).items():
                    response['stale_tables'][table_name] = {
                        'expected': table_result.get('expected', 0),
                        'updated': table_result.get('updated', 0),
                        'success': table_result.get('success', False),
                        'error': table_result.get('error', '')
                    }
            
            raise Exception(error_msg)
    except Exception as e:
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        logger.error(f"Lambda execution failed: {e} (Time elapsed: {elapsed_time}s)")
        return {
            'status': 'error',
            'error': str(e),
            'elapsed_time_seconds': elapsed_time,
            'account_id': account_id,
            'location_id': location_id
        }

# For local testing
if __name__ == "__main__":
    event = {
        'account_id': int(sys.argv[1]) if len(sys.argv) > 1 else 89,
        'location_id': int(sys.argv[2]) if len(sys.argv) > 2 else 48717
    }
    print(lambda_handler(event))
