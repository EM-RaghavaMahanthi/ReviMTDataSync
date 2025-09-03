import os
import json
import time
import logging
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio
from services.membership_instances_service import fetch_membership_instances_page

logger = logging.getLogger(__name__)

async def write_failed_entries(failed_entries, batch_id, account_id, location_id):
    """Write failed entries to DLQ temp file with batch, account, and location context."""
    if not failed_entries:
        return None
    os.makedirs("data", exist_ok=True)
    dlq_file = f"data/failed_membership_instances_batch_{batch_id}_account_{account_id}_location_{location_id}.json"
    with open(dlq_file, "w") as f:
        for entry in failed_entries:
            f.write(json.dumps(entry) + "\n")
    logger.warning(f"[DLQ WRITE] Batch {batch_id} (account={account_id}, location={location_id}): {len(failed_entries)} user-page failures written to {dlq_file}")
    return dlq_file

async def retry_failed_entries(dlq_file, batch_id, account_id, api_base_url, location_id, concurrency_limit=None):
    """Retry failed user pages from DLQ file in parallel and return recovered data + remaining failures."""
    if not os.path.exists(dlq_file):
        return [], 0
    
    # Read and group failures by user
    user_failures = {}
    with open(dlq_file, "r") as f:
        for line in f:
            entry = json.loads(line.strip())
            user_id = entry["user_id"]
            if user_id not in user_failures:
                user_failures[user_id] = []
            user_failures[user_id].append(entry)
    
    if not user_failures:
        return [], 0
    
    # Use concurrency limit
    if concurrency_limit is None:
        concurrency_limit = settings.CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency_limit)
    
    async def retry_user_pages(user_id, failures):
        async with semaphore:
            user_recovered = []
            user_new_failures = []
            
            for failure in failures:
                if failure["page"] == "all":
                    # Skip user-level failures - these need manual intervention
                    logger.error(f"[DLQ RETRY] Batch {batch_id}: User {user_id} had complete failure, skipping: {failure['error']}")
                    user_new_failures.append(failure)
                    continue
                    
                try:
                    logger.info(f"[DLQ RETRY] Batch {batch_id}: Retrying user {user_id} page {failure['page']}")
                    instances, _ = await fetch_membership_instances_page(
                        user_id, failure["page"], account_id, api_base_url, location_id
                    )
                    user_recovered.extend(instances)
                    logger.info(f"[DLQ RETRY] Batch {batch_id}: User {user_id} page {failure['page']} recovered - {len(instances)} instances")
                except Exception as e:
                    logger.error(f"[DLQ RETRY] Batch {batch_id}: User {user_id} page {failure['page']} failed again: {e}")
                    user_new_failures.append({
                        "user_id": user_id,
                        "page": failure["page"],
                        "error": str(e),
                        "timestamp": time.time()
                    })
            
            return user_recovered, user_new_failures
    
    # Process all user retries in parallel
    tasks = [retry_user_pages(uid, failures) for uid, failures in user_failures.items()]
    results = await asyncio.gather(*tasks)
    
    # Aggregate results
    recovered_data = []
    new_failures = []
    
    for user_recovered, user_new_failures in results:
        recovered_data.extend(user_recovered)
        new_failures.extend(user_new_failures)
    
    # Update or clean up DLQ file
    if new_failures:
        with open(dlq_file, "w") as f:
            for entry in new_failures:
                f.write(json.dumps(entry) + "\n")
        logger.error(f"[DLQ FINAL] Batch {batch_id}: {len(new_failures)} user-page failures still failing after retry")
    else:
        os.remove(dlq_file)
        logger.info(f"[DLQ SUCCESS] Batch {batch_id}: All failed user pages recovered, DLQ file removed")
    
    return recovered_data, len(new_failures)

async def fetch_membership_instances_for_user(user_id: str, account_id: str, api_base_url: str, location_id: int):
    """
    Fetch all membership instances for a user with failure tracking.
    Returns all data, expected count, and failed pages for aggregation.
    """
    logger.info(f"[FETCH] Starting membership instances fetch for user {user_id} (account_id={account_id}, location_id={location_id})")
    
    try:
        # Get first page to know total pages
        first_instances, first_resp = await fetch_membership_instances_page(user_id, 1, account_id, api_base_url, location_id)
    except Exception as e:
        logger.error(f"[FETCH] User {user_id} first page failed: {e}")
        # Return user-level failure
        failed_pages = [{
            "user_id": user_id,
            "page": "all",
            "error": f"First page failure: {str(e)}",
            "timestamp": time.time()
        }]
        return [], 0, failed_pages
    
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_instances))
    
    logger.info(f"[FETCH] User {user_id} (account_id={account_id}, location_id={location_id}): Expected {total_records} membership instances across {total_pages} pages.")
    
    all_instances = []
    failed_pages = []  # In-memory failure collection
    
    all_instances.extend(first_instances)
    logger.info(f"[FETCH] User {user_id} (account_id={account_id}): Processed page 1 with {len(first_instances)} records.")

    # Fetch remaining pages with failure tracking
    if total_pages > 1:
        concurrency_limit = getattr(settings, "CONCURRENCY_LIMIT", 16)
        semaphore = asyncio.Semaphore(concurrency_limit)
        
        async def fetch_page(page):
            async with semaphore:
                try:
                    instances, _ = await fetch_membership_instances_page(user_id, page, account_id, api_base_url, location_id)
                    logger.info(f"[FETCH] User {user_id} (account_id={account_id}, location_id={location_id}): Processed page {page} with {len(instances)} records.")
                    return instances, page, None
                except Exception as e:
                    logger.error(f"[ERROR] User {user_id} (account_id={account_id}, location_id={location_id}) page {page} failed: {e}")
                    # Store failure in memory
                    failed_pages.append({
                        "user_id": user_id,
                        "page": page,
                        "error": str(e),
                        "timestamp": time.time()
                    })
                    return [], page, e
        
        remaining_pages = list(range(2, total_pages + 1))
        tasks = [fetch_page(p) for p in remaining_pages]
        results = await asyncio.gather(*tasks)
        
        for instances, page_no, error in results:
            all_instances.extend(instances)
    
    logger.info(f"[FETCH] User {user_id} (account_id={account_id}, location_id={location_id}) completed. Fetched {len(all_instances)} membership instances, {len(failed_pages)} page failures.")
    return all_instances, total_records, failed_pages

async def process_membership_instances_batch(user_ids: list, batch_id: int, account_id: str, api_base_url: str, location_id: int):
    """
    Process multiple users and save aggregated data with batch_id, including DLQ handling.
    """
    start_time = time.time()
    logger.info(f"[BATCH {batch_id}] Processing {len(user_ids)} users for membership instances (account_id={account_id}, location_id={location_id})")
    
    # Fetch all users in parallel with failure tracking
    concurrency_limit = getattr(settings, "CONCURRENCY_LIMIT", 16)
    semaphore = asyncio.Semaphore(concurrency_limit)
    
    # Collect all failures from all users
    all_batch_failures = []
    
    async def guarded_fetch_user(user_id):
        async with semaphore:
            try:
                logger.info(f"[BATCH {batch_id}] Starting fetch for user {user_id} (account_id={account_id}, location_id={location_id})")
                user_data, expected, user_failures = await fetch_membership_instances_for_user(user_id, account_id, api_base_url, location_id)
                # Collect failures from this user
                all_batch_failures.extend(user_failures)
                return user_data, expected
            except Exception as e:
                logger.error(f"[BATCH {batch_id}] User {user_id} (account_id={account_id}, location_id={location_id}) completely failed: {e}")
                # User-level complete failure
                all_batch_failures.append({
                    "user_id": user_id,
                    "page": "all",
                    "error": f"Complete user failure: {str(e)}",
                    "timestamp": time.time()
                })
                return [], 0
    
    tasks = [guarded_fetch_user(uid) for uid in user_ids]
    results = await asyncio.gather(*tasks)
    
    # Aggregate all data
    all_data = []
    total_expected = 0
    for user_data, expected in results:
        all_data.extend(user_data)
        total_expected += expected
    
    logger.info(f"[BATCH {batch_id}] Initial aggregation: {len(all_data)} membership instances from {len(user_ids)} users (account_id={account_id})")
    
    # Handle failures with DLQ retry
    dlq_file = None
    final_failed_count = 0
    
    if all_batch_failures:
        logger.warning(f"[PARTIAL SUCCESS] Batch {batch_id}: {len(all_batch_failures)} user-page failures occurred initially")
        dlq_file = await write_failed_entries(all_batch_failures, batch_id, account_id, location_id)
        
        # Retry failed pages
        logger.info(f"[DLQ RETRY] Batch {batch_id}: Retrying {len(all_batch_failures)} failed user pages")
        recovered_data, final_failed_count = await retry_failed_entries(
            dlq_file, batch_id, account_id, api_base_url, location_id, concurrency_limit
        )
        
        # Add recovered data to main dataset
        if recovered_data:
            all_data.extend(recovered_data)
            logger.info(f"[DLQ RECOVERY] Batch {batch_id}: {len(recovered_data)} instances recovered from retry")
    
    # Save in chunks with batch_id using updated file naming
    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)
    prefix = settings.S3_PREFIXES.get("membership_instances", "membership-instances-details")
    
    file_num = 1
    total_processed = 0
    
    for i in range(0, len(all_data), parquet_batch_size):
        batch_data = all_data[i:i+parquet_batch_size]
        await write_parquet_to_s3(
            batch_data,
            str(batch_id),  # Keep it simple
            file_num,
            account_id,
            s3_prefix=prefix,
            entity_type="user_batch"
        )
        total_processed += len(batch_data)
        logger.info(f"[BATCH {batch_id}] Saved file {file_num} with {len(batch_data)} records (account_id={account_id}, location_id={location_id})")
        file_num += 1

    elapsed = time.time() - start_time
    
    # Final success/failure determination
    missing_records = total_expected - total_processed
    
    logger.info(f"[BATCH {batch_id}] Completed in {elapsed:.2f} seconds. Total processed: {total_processed}, Expected: {total_expected} (account_id={account_id}, location_id={location_id})")
    
    if final_failed_count > 0:
        logger.error(f"[FINAL FAILURE] Batch {batch_id}: {final_failed_count} user pages permanently failed after retry. Check {dlq_file}")
        raise Exception(f"Membership instances batch {batch_id} failed: {final_failed_count} user pages could not be processed after retry")
    
    if missing_records > 0:
        logger.warning(f"[MISSING] Batch {batch_id}: Expected {total_expected}, but only {total_processed} processed. Missing: {missing_records}")
        # Don't raise error for missing records due to validation failures
    else:
        logger.info(f"[FULL SUCCESS] Batch {batch_id}: All expected records processed successfully")

    return total_processed, total_expected
