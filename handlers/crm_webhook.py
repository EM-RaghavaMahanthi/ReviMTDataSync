import json
import asyncio
import logging
import time
from typing import Dict, Any, List
import os
import sys

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from crm_services.customers_service import process_customers_batch
from crm_services.class_sessions_service import process_class_sessions_batch
from crm_services.orders_service import process_orders_batch
from crm_services.order_lines_service import process_order_lines_batch
from crm_services.reservations_service import process_reservations_batch
from crm_services.membership_instances_service import process_membership_instances_batch
from crm_services.credit_transactions_service import process_credit_transactions_batch
from crm_services.membership_transactions_service import process_membership_transactions_batch

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for CRM data processing.
    
    Expected event structure:
    {
        "crm_api_endpoint": "https://reformedpilates.marianatek.com/api",
        "start_page": 1,
        "end_page": 100,
        "services": ["customers", "orders", "reservations"],  # Optional: specific services to run
        "concurrency_limit": 8,  # Optional
        "save_to_s3": true  # Optional, default true
    }
    """
    
    logger.info("=== CRM LAMBDA FUNCTION STARTED ===")
    start_time = time.time()
    
    try:
        # Extract parameters from event
        crm_api_endpoint = event.get("crm_api_endpoint")
        if not crm_api_endpoint:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Missing required parameter: crm_api_endpoint",
                    "status": "error"
                })
            }
        
        start_page = event.get("start_page", 1)
        end_page = event.get("end_page", 100)
        concurrency_limit = event.get("concurrency_limit", 8)
        save_to_s3 = event.get("save_to_s3", True)
        
        # Services to run (default: all services)
        requested_services = event.get("services", [
            "customers", 
            "class_sessions", 
            "orders", 
            "order_lines", 
            "reservations", 
            "membership_instances", 
            "credit_transactions", 
            "membership_transactions"
        ])
        
        logger.info(f"Processing CRM API: {crm_api_endpoint}")
        logger.info(f"Page range: {start_page} to {end_page}")
        logger.info(f"Services to process: {requested_services}")
        logger.info(f"Concurrency limit: {concurrency_limit}")
        logger.info(f"Save to S3: {save_to_s3}")
        
        # Run the CRM processing
        result = asyncio.run(process_crm_services(
            crm_api_endpoint=crm_api_endpoint,
            start_page=start_page,
            end_page=end_page,
            services=requested_services,
            concurrency_limit=concurrency_limit,
            save_to_s3=save_to_s3
        ))
        
        elapsed_time = time.time() - start_time
        logger.info(f"=== CRM LAMBDA FUNCTION COMPLETED in {elapsed_time:.2f}s ===")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "processing_time_seconds": round(elapsed_time, 2),
                "results": result
            })
        }
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"CRM Lambda function failed: {str(e)}", exc_info=True)
        
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "error": str(e),
                "processing_time_seconds": round(elapsed_time, 2)
            })
        }


async def process_crm_services(
    crm_api_endpoint: str,
    start_page: int,
    end_page: int,
    services: List[str],
    concurrency_limit: int = 8,
    save_to_s3: bool = True
) -> Dict[str, Any]:
    """
    Process all requested CRM services concurrently.
    """
    
    batch_id = f"crm_batch_{int(time.time())}"
    
    # Define service mapping
    service_functions = {
        "customers": process_customers_batch,
        "class_sessions": process_class_sessions_batch,
        "orders": process_orders_batch,
        "order_lines": process_order_lines_batch,
        "reservations": process_reservations_batch,
        "membership_instances": process_membership_instances_batch,
        "credit_transactions": process_credit_transactions_batch,
        "membership_transactions": process_membership_transactions_batch
    }
    
    # Validate requested services
    invalid_services = [s for s in services if s not in service_functions]
    if invalid_services:
        raise ValueError(f"Invalid services requested: {invalid_services}. Available: {list(service_functions.keys())}")
    
    logger.info(f"Starting concurrent processing of {len(services)} CRM services")
    
    # Create tasks for all requested services
    tasks = []
    for service_name in services:
        service_batch_id = f"{batch_id}_{service_name}"
        
        task = service_functions[service_name](
            start_page=start_page,
            end_page=end_page,
            api_base_url=crm_api_endpoint,
            batch_id=service_batch_id,
            concurrency_limit=concurrency_limit,
            save_to_s3=save_to_s3
        )
        tasks.append((service_name, task))
    
    # Execute all services concurrently
    results = {}
    successful_services = []
    failed_services = []
    
    try:
        # Wait for all tasks to complete
        task_results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        # Process results
        for i, (service_name, result) in enumerate(zip([name for name, _ in tasks], task_results)):
            if isinstance(result, Exception):
                logger.error(f"Service {service_name} failed with exception: {result}")
                results[service_name] = {
                    "status": "error",
                    "error": str(result),
                    "total_entries": 0
                }
                failed_services.append(service_name)
            else:
                results[service_name] = result
                if result.get("status") == "success":
                    successful_services.append(service_name)
                else:
                    failed_services.append(service_name)
                    
                logger.info(f"Service {service_name}: {result.get('status')} - {result.get('total_entries', 0)} entries")
    
    except Exception as e:
        logger.error(f"Critical error in concurrent service processing: {e}")
        raise
    
    # Calculate summary metrics
    total_entries = sum(result.get("total_entries", 0) for result in results.values())
    total_pages_processed = sum(result.get("pages_processed", 0) for result in results.values())
    total_pages_failed = sum(result.get("pages_failed", 0) for result in results.values())
    
    # Determine overall status
    if len(failed_services) == 0:
        overall_status = "success"
    elif len(successful_services) > 0:
        overall_status = "partial_success"
    else:
        overall_status = "failure"
    
    summary = {
        "overall_status": overall_status,
        "total_services": len(services),
        "successful_services": len(successful_services),
        "failed_services": len(failed_services),
        "total_entries_processed": total_entries,
        "total_pages_processed": total_pages_processed,
        "total_pages_failed": total_pages_failed,
        "batch_id": batch_id,
        "crm_api_endpoint": crm_api_endpoint,
        "page_range": f"{start_page}-{end_page}",
        "service_results": results
    }
    
    logger.info("=== CRM PROCESSING SUMMARY ===")
    logger.info(f"Overall Status: {overall_status}")
    logger.info(f"Services Processed: {len(successful_services)}/{len(services)}")
    logger.info(f"Total Entries: {total_entries}")
    logger.info(f"Total Pages Processed: {total_pages_processed}")
    if failed_services:
        logger.warning(f"Failed Services: {failed_services}")
    logger.info("=============================")
    
    return summary


def main():
    """
    Test function to run the CRM lambda locally.
    """
    
    # Test event
    test_event = {
        "crm_api_endpoint": "https://reformedpilates.marianatek.com/api",
        "start_page": 1,
        "end_page": 10,  # Small test range
        "services": [
            "customers", 
            "orders", 
            "reservations"
        ],  # Test with subset of services
        "concurrency_limit": 4,
        "save_to_s3": True
    }
    
    print("=" * 60)
    print("TESTING CRM LAMBDA FUNCTION LOCALLY")
    print("=" * 60)
    print(f"Test Event: {json.dumps(test_event, indent=2)}")
    print("=" * 60)
    
    # Run the lambda handler
    result = lambda_handler(test_event, None)
    
    print("\n" + "=" * 60)
    print("LAMBDA FUNCTION RESULT")
    print("=" * 60)
    print(json.dumps(result, indent=2))
    print("=" * 60)
    
    return result


if __name__ == "__main__":
    main()