import math
import logging
import os
import json
import time
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings  
import asyncio

logger = logging.getLogger(__name__)

async def fetch_customers_page(location_id: str, page: int, account_id: str, api_base_url: str):
    """
    Fetches one page of customers for a given location (async).
    """
    page_size = getattr(settings, "PAGE_SIZE", 500)
    params = {"home_location": location_id, "page": page, "page_size": page_size}
    logger.info(f"[FETCH] Location {location_id}, Account {account_id}: Fetching page {page} with page_size {page_size}")
    resp = await api_get("/users", api_base_url, params)
    from schemas.revi_schema import Customer
    valid_customers = []
    for u in resp.get("data", []):
        raw = dict({"customer_id": u["id"]}, **u["attributes"])
        raw['location_id'] = location_id
        raw['account_id'] = account_id
        logger.info(f"[VALIDATION] Location {location_id}, Account {account_id}: Validating customer id={raw.get('customer_id')}")
        try:
            customer = Customer(**raw)
            valid_customers.append(customer.model_dump())
        except Exception as e:
            logger.warning(f"[VALIDATION] Location {location_id}, Account {account_id}: Skipping customer id={raw.get('customer_id')}: {e}")
    logger.info(f"[PAGE PROCESSED] Location {location_id}, Account {account_id}: Page {page} processed, {len(valid_customers)} valid customers")
    return valid_customers, resp  

async def write_failed_entries(failed_entries, location_id, account_id):
    """Write failed page entries to DLQ temp file."""
    if not failed_entries:
        return None
    os.makedirs("data", exist_ok=True)
    dlq_file = f"data/failed_customers_{location_id}_{account_id}.json"
    with open(dlq_file, "w") as f:
        for entry in failed_entries:
            f.write(json.dumps(entry) + "\n")
    logger.warning(f"[DLQ WRITE] Location {location_id}: {len(failed_entries)} pages written to {dlq_file}")
    return dlq_file

async def retry_failed_entries(dlq_file, location_id, account_id, api_base_url, concurrency_limit=None):
    """Retry failed pages from DLQ file in parallel and return recovered data + remaining failures."""
    if not os.path.exists(dlq_file):
        return [], 0
    
    # Read all failed entries
    failed_entries = []
    with open(dlq_file, "r") as f:
        for line in f:
            entry = json.loads(line.strip())
            failed_entries.append(entry)
    
    if not failed_entries:
        return [], 0
    
    # Use concurrency limit (same as main processing)
    if concurrency_limit is None:
        concurrency_limit = settings.CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency_limit)
    
    async def retry_single_page(entry):
        page = entry["page"]
        async with semaphore:
            try:
                logger.info(f"[DLQ RETRY] Location {location_id}: Retrying page {page}")
                customers, _ = await fetch_customers_page(location_id, page, account_id, api_base_url)
                logger.info(f"[DLQ RETRY] Location {location_id}, page {page}: Success on retry - {len(customers)} customers recovered")
                return {"success": True, "page": page, "customers": customers}
            except Exception as e:
                logger.error(f"[DLQ RETRY] Location {location_id}, page {page}: Failed again: {e}")
                return {"success": False, "page": page, "error": str(e)}
    
    # Process all retries in parallel
    tasks = [retry_single_page(entry) for entry in failed_entries]
    results = await asyncio.gather(*tasks)
    
    # Collect results
    recovered_customers = []
    new_failed = []
    
    for result in results:
        if result["success"]:
            recovered_customers.extend(result["customers"])
        else:
            new_failed.append({
                "page": result["page"], 
                "error": result["error"], 
                "timestamp": time.time()
            })
    
    # Update or clean up DLQ file
    if new_failed:
        with open(dlq_file, "w") as f:
            for entry in new_failed:
                f.write(json.dumps(entry) + "\n")
        logger.error(f"[DLQ FINAL] Location {location_id}: {len(new_failed)} pages still failing after retry")
    else:
        os.remove(dlq_file)
        logger.info(f"[DLQ SUCCESS] Location {location_id}: All failed pages recovered, DLQ file removed")
    
    return recovered_customers, len(new_failed)

async def process_customers_for_location(location_id: str, account_id: str, api_base_url: str, max_workers: int = 8, concurrency_limit: int = None, save_to_s3: bool = True):
    """
    Fetch customer pages in parallel with failure handling and DLQ retry.
    """
    start_time = time.time()
    logger.info(f"[START] Location {location_id}, Account {account_id}: Starting customer processing")
    
    # --- Get the first page to know total pages ---
    try:
        first_customers, first_resp = await fetch_customers_page(location_id, 1, account_id, api_base_url)
    except Exception as e:
        logger.critical(f"[FATAL] Location {location_id}: Failed to fetch first page: {e}")
        raise e
        
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_customers))

    logger.info(f"[INFO] Location {location_id}, Account {account_id}: Expected {total_records} records across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate customers into batches
    batch_data = []
    batch_num = 1
    total_processed = 0
    failed_pages = []  # In-memory failure collection

    # Save any accumulated batch to S3
    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data and save_to_s3:
            prefix = settings.S3_PREFIXES["customers"]
            await write_parquet_to_s3(batch_data, location_id, batch_num, account_id, s3_prefix=prefix)
            logger.info(f"[S3 WRITE] Location {location_id}, Account {account_id}: Batch {batch_num} written to S3 with {len(batch_data)} records")
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []
        elif batch_data:
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []

    # Process first page
    batch_data.extend(first_customers)
    if len(batch_data) >= parquet_batch_size:
        await flush_batch()

    # Prepare remaining pages list
    remaining_pages = list(range(2, total_pages + 1))

    # Use concurrency limit from settings if not provided
    if concurrency_limit is None:
        concurrency_limit = settings.CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def fetch_and_process(page):
        async with semaphore:
            try:
                customers, _ = await fetch_customers_page(location_id, page, account_id, api_base_url)
                logger.info(f"[PAGE DONE] Location {location_id}, Account {account_id}: Page {page} fetched with {len(customers)} customers")
                return customers, page, None
            except Exception as e:
                logger.error(f"[ERROR] Location {location_id}, Account {account_id}: Page {page} failed: {e}")
                # Store failure in memory
                failed_pages.append({"page": page, "error": str(e), "timestamp": time.time()})
                return [], page, e

    # Process remaining pages in parallel
    user_ids = [c["customer_id"] for c in first_customers]
    
    if remaining_pages:
        tasks = [fetch_and_process(p) for p in remaining_pages]
        results = await asyncio.gather(*tasks)

        # Collect user IDs and batch data from successful pages
        for customers, page_no, error in results:
            batch_data.extend(customers)
            user_ids.extend([c["customer_id"] for c in customers])
            if len(batch_data) >= parquet_batch_size:
                await flush_batch()

    # Flush any remaining data from initial processing
    await flush_batch()

    # Handle failures with DLQ retry
    dlq_file = None
    final_failed_count = 0
    
    if failed_pages:
        logger.warning(f"[PARTIAL SUCCESS] Location {location_id}: {len(failed_pages)} pages failed initially")
        dlq_file = await write_failed_entries(failed_pages, location_id, account_id)
        
        # Retry failed pages
        logger.info(f"[DLQ RETRY] Location {location_id}: Retrying {len(failed_pages)} failed pages")
        recovered_customers, final_failed_count = await retry_failed_entries(dlq_file, location_id, account_id, api_base_url)
        
        # Process recovered customers with proper S3 batching
        if recovered_customers:
            logger.info(f"[DLQ RECOVERY] Location {location_id}: Processing {len(recovered_customers)} recovered customers")
            for customer in recovered_customers:
                batch_data.append(customer)
                user_ids.append(customer["customer_id"])
                # Use the same flush logic as main processing
                if len(batch_data) >= parquet_batch_size:
                    await flush_batch()
            
            logger.info(f"[DLQ RECOVERY] Location {location_id}: {len(recovered_customers)} customers processed from retry")

    # Final flush for any remaining data (including recovered data)
    await flush_batch()

    elapsed = time.time() - start_time
    
    # Final success/failure determination
    missing_records = total_records - total_processed
    
    if save_to_s3:
        logger.info(f"[DONE] Location {location_id}, Account {account_id}: Processed in {elapsed:.2f} seconds. Records written to S3: {total_processed}")
        
        if final_failed_count > 0:
            logger.error(f"[FINAL FAILURE] Location {location_id}: {final_failed_count} pages permanently failed after retry. Check {dlq_file}")
            raise Exception(f"Customer processing failed: {final_failed_count} pages could not be processed after retry")
        
        if missing_records > 0:
            logger.warning(f"[MISSING] Location {location_id}, Account {account_id}: Expected {total_records}, but only {total_processed} written to S3. Missing: {missing_records}")
            # Don't raise error for missing records due to validation failures
        else:
            logger.info(f"[FULL SUCCESS] Location {location_id}: All expected records processed successfully")
    
    return total_processed, total_records, user_ids

