import sys
import asyncio
import time
import logging
from core.stg_db_config import settings
from sqlalchemy import create_engine

from core.logger import setup_logging
setup_logging()

# Import all the staging-to-db service functions in order
from stg_db_services.customers_service_01 import process_customers
from stg_db_services.class_sessions_service_02 import process_class_sessions
from stg_db_services.membership_instances_service_03 import process_membership_instances
from stg_db_services.credit_transactions_service_04 import process_credit_transactions
from stg_db_services.credit_transactions_orders_service_4a import process_credit_transactions_orders
from stg_db_services.membership_transactions_service_05 import process_membership_transactions
from stg_db_services.membership_transactions_orders_service_5a import process_membership_transactions_orders
from stg_db_services.orders_service_06 import process_orders
from stg_db_services.order_lines_service_07 import process_order_lines
from stg_db_services.reservations_service_08 import process_reservations

logger = logging.getLogger("lambda_function_stg_db")

# Define the processing order and service functions
PROCESSING_ORDER = [
    ("customers_01", process_customers),
    ("class_sessions_02", process_class_sessions),
    ("membership_instances_03", process_membership_instances),
    ("credit_transactions_04", process_credit_transactions),
    ("credit_transactions_orders_4a", process_credit_transactions_orders),
    ("membership_transactions_05", process_membership_transactions),
    ("membership_transactions_orders_5a", process_membership_transactions_orders),
    ("orders_06", process_orders),
    ("order_lines_07", process_order_lines),
    ("reservations_08", process_reservations),
]

async def async_stg_to_db_handler(event, context=None):
    """
    Process staging tables to final database tables in the correct order.
    """
    start_time = time.time()
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

    # Create database engine
    engine = create_engine(settings.DATABASE_URL)
    
    try:
        logger.info(f"Starting staging-to-DB processing for account_id={account_id}, location_id={location_id}")
        
        # Track success/failure for each table
        successful_tables = []
        failed_tables = []
        total_processed = 0
        
        # Process each table in the defined order
        for table_name, process_func in PROCESSING_ORDER:
            table_start_time = time.time()
            try:
                logger.info(f"🔄 Starting {table_name} processing...")
                
                # Call the processing function with account_id, location_id, and engine
                processed_result = await process_func(account_id, location_id, engine)
                
                # Handle both integer and dictionary returns
                if isinstance(processed_result, dict):
                    processed_count = processed_result.get('inserted_records', 0)
                else:
                    processed_count = processed_result
                
                table_end_time = time.time()
                table_duration = table_end_time - table_start_time
                
                logger.info(f"✅ {table_name} completed successfully: {processed_result} records processed in {table_duration:.2f}s")
                successful_tables.append({
                    "table": table_name,
                    "status": "success",
                    "records_processed": processed_result,
                    "duration": table_duration
                })
                total_processed += processed_count
                
            except Exception as e:
                table_end_time = time.time()
                table_duration = table_end_time - table_start_time
                
                logger.error(f"❌ {table_name} failed: {e}")
                logger.error(f"🛑 Stopping processing - subsequent services depend on {table_name}")
                failed_tables.append({
                    "table": table_name,
                    "status": "failed",
                    "error": str(e),
                    "duration": table_duration
                })
                # Stop processing immediately - later services depend on previous ones
                break
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Summary logging
        total_tables = len(PROCESSING_ORDER)
        success_count = len(successful_tables)
        failure_count = len(failed_tables)
        
        logger.info(f"🏁 STAGING-TO-DB PROCESSING SUMMARY:")
        logger.info(f"   📊 Tables: {success_count}/{total_tables} successful, {failure_count} failed")
        logger.info(f"   📈 Total records processed: {total_processed}")
        logger.info(f"   ⏱️  Total duration: {total_duration:.2f} seconds")
        
        if successful_tables:
            logger.info(f"   ✅ Successful: {[t['table'] for t in successful_tables]}")
        if failed_tables:
            logger.error(f"   ❌ Failed: {[t['table'] for t in failed_tables]}")
        
        # Determine overall status
        overall_status = "success" if failure_count == 0 else "partial_success" if success_count > 0 else "failed"
        
        return {
            'status': overall_status,
            'account_id': account_id,
            'location_id': location_id,
            'summary': {
                'total_tables': total_tables,
                'successful_tables': success_count,
                'failed_tables': failure_count,
                'total_records_processed': total_processed,
                'total_duration_seconds': total_duration
            },
            'table_results': successful_tables + failed_tables
        }
        
    except Exception as e:
        end_time = time.time()
        total_duration = end_time - start_time
        
        logger.error(f"💥 Staging-to-DB processing failed completely: {e}")
        return {
            'status': 'error',
            'account_id': account_id,
            'location_id': location_id,
            'error': str(e),
            'duration': total_duration
        }
    finally:
        engine.dispose()

def lambda_handler(event, context=None):
    """
    Lambda entry point - runs the async staging-to-DB processing.
    """
    return asyncio.run(async_stg_to_db_handler(event, context))

# For local testing
if __name__ == "__main__":
    event = {
        'account_id': int(sys.argv[1]) if len(sys.argv) > 1 else 85,
        'location_id': int(sys.argv[2]) if len(sys.argv) > 2 else 48718
    }
    result = lambda_handler(event)
    print("Result:", result)
    
    if result['status'] == 'success':
        print("🎉 All staging-to-DB processing completed successfully!")
    elif result['status'] == 'partial_success':
        print("⚠️  Staging-to-DB processing completed with some failures.")
    else:
        print("❌ Staging-to-DB processing failed.")
        sys.exit(1)
