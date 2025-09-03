import os
import json
import time
import logging
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
from pydantic import ValidationError
import asyncio

logger = logging.getLogger(__name__)

async def fetch_class_sessions_page(location_id: str, page: int, account_id: str, api_base_url: str):
    """
    Fetches one page of class sessions for a given location (async).
    """
    # Ensure location_id is string
    location_id = str(location_id)
    page_size = settings.PAGE_SIZE or 100
    params = {"location": location_id, "page": page, "page_size": page_size}
    logger.info(f"[FETCH] Fetching page {page} for location {location_id} (account {account_id}) with page_size {page_size}")
    resp = await api_get("/class_sessions", api_base_url, params)
    logger.info(f"[API RESPONSE] Location {location_id} (account {account_id}) page {page}: Got {len(resp.get('data', []))} records from API")
    from schemas.revi_schema import ClassSessions  # Ensure correct import

    valid_class_sessions = []
    for u in resp.get("data", []):
        attributes = u.get("attributes", {})
        raw = {
            "class_session_id": str(u.get("id")),
            "start_datetime": attributes.get("start_datetime"),
            "start_date": attributes.get("start_date"),
            "location": str(location_id),
            "end_datetime": attributes.get("end_datetime"),
            "cancellation_datetime": attributes.get("cancellation_datetime"),
            "created_at": None,          # Not needed if DB default
            "created_by": None,
            "updated_at": None,          # Not needed if DB default
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "account_id": account_id,
        }
        try:
            class_session = ClassSessions(**raw)
            valid_class_sessions.append(class_session.model_dump())
        except ValidationError as ve:
            logger.warning(f"[VALIDATION] Skipping class_session id={u.get('id')} for location {location_id} (account {account_id}) due to validation errors: {ve.errors()}")
        except Exception as e:
            logger.error(f"[ERROR] Unexpected error processing class_session id={u.get('id')} for location {location_id} (account {account_id}): {e}", exc_info=True)
    logger.info(f"[PAGE PROCESSED] Location {location_id} (account {account_id}) page {page}: {len(valid_class_sessions)} valid class sessions")
    return valid_class_sessions, resp

async def write_failed_entries(failed_entries, location_id, account_id):
    """Write failed page entries to DLQ temp file."""
    if not failed_entries:
        return None
    os.makedirs("data", exist_ok=True)
    dlq_file = f"data/failed_class_sessions_{location_id}_{account_id}.json"
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
                sessions, _ = await fetch_class_sessions_page(location_id, page, account_id, api_base_url)
                logger.info(f"[DLQ RETRY] Location {location_id}, page {page}: Success on retry - {len(sessions)} sessions recovered")
                return {"success": True, "page": page, "sessions": sessions}
            except Exception as e:
                logger.error(f"[DLQ RETRY] Location {location_id}, page {page}: Failed again: {e}")
                return {"success": False, "page": page, "error": str(e)}
    
    # Process all retries in parallel
    tasks = [retry_single_page(entry) for entry in failed_entries]
    results = await asyncio.gather(*tasks)
    
    # Collect results
    recovered_sessions = []
    new_failed = []
    
    for result in results:
        if result["success"]:
            recovered_sessions.extend(result["sessions"])
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
    
    return recovered_sessions, len(new_failed)


async def process_class_sessions_for_location(location_id: str, account_id: str, api_base_url: str, max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch class session pages in parallel with failure handling and DLQ retry.
    """
    location_id = str(location_id)
    start_time = time.time()
    logger.info(f"[START] Processing class sessions for location {location_id} (account {account_id})")
    
    # --- Get the first page to know total pages ---
    try:
        first_class_sessions, first_resp = await fetch_class_sessions_page(location_id, 1, account_id, api_base_url)
    except Exception as e:
        logger.critical(f"[FATAL] Location {location_id}: Failed to fetch first page: {e}")
        raise e
        
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_class_sessions))

    logger.info(f"[INFO] Location {location_id} (account {account_id}): Expected {total_records} class sessions across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate class sessions into batches
    batch_data = []
    batch_num = 1
    total_processed = 0
    failed_pages = []  # In-memory failure collection

    # Save any accumulated batch to S3
    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data:
            prefix = settings.S3_PREFIXES.get("class_sessions", "class_sessions-details")
            logger.info(f"[S3 WRITE] Writing batch {batch_num} with {len(batch_data)} records to S3 for location {location_id} (account {account_id})")
            await write_parquet_to_s3(batch_data, location_id, batch_num, account_id, s3_prefix=prefix)
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []

    # Process first page (sequential)
    batch_data.extend(first_class_sessions)
    logger.info(f"[BATCH] Added {len(first_class_sessions)} records from page 1 to batch for location {location_id} (account {account_id})")
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
                class_sessions, _ = await fetch_class_sessions_page(location_id, page, account_id, api_base_url)
                logger.info(f"[PAGE DONE] Location {location_id} (account {account_id}) page {page} processed with {len(class_sessions)} records")
                return class_sessions, page, None
            except Exception as e:
                logger.error(f"[ERROR] Location {location_id} (account {account_id}) page {page} failed: {e}")
                # Store failure in memory
                failed_pages.append({"page": page, "error": str(e), "timestamp": time.time()})
                return [], page, e

    # Process remaining pages in parallel
    if remaining_pages:
        tasks = [fetch_and_process(p) for p in remaining_pages]
        results = await asyncio.gather(*tasks)

        # Collect class sessions from successful pages
        for class_sessions, page_no, error in results:
            batch_data.extend(class_sessions)
            logger.info(f"[BATCH] Added {len(class_sessions)} records from page {page_no} to batch for location {location_id} (account {account_id})")
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
        recovered_sessions, final_failed_count = await retry_failed_entries(dlq_file, location_id, account_id, api_base_url)
        
        # Process recovered class sessions with proper S3 batching
        if recovered_sessions:
            logger.info(f"[DLQ RECOVERY] Location {location_id}: Processing {len(recovered_sessions)} recovered sessions")
            for session in recovered_sessions:
                batch_data.append(session)
                # Use the same flush logic as main processing
                if len(batch_data) >= parquet_batch_size:
                    await flush_batch()
            
            logger.info(f"[DLQ RECOVERY] Location {location_id}: {len(recovered_sessions)} sessions processed from retry")

    # Final flush for any remaining data (including recovered data)
    await flush_batch()

    elapsed = time.time() - start_time
    
    # Final success/failure determination
    missing_records = total_records - total_processed
    
    logger.info(f"[DONE] Location {location_id} (account {account_id}) processed in {elapsed:.2f} seconds. Class sessions written to S3: {total_processed}")
    
    if final_failed_count > 0:
        logger.error(f"[FINAL FAILURE] Location {location_id}: {final_failed_count} pages permanently failed after retry. Check {dlq_file}")
        raise Exception(f"Class session processing failed: {final_failed_count} pages could not be processed after retry")
    
    if missing_records > 0:
        logger.warning(f"[MISSING] Location {location_id} (account {account_id}): Expected {total_records}, but only {total_processed} class sessions written to S3. Missing: {missing_records}")
        # Don't raise error for missing records due to validation failures
    else:
        logger.info(f"[FULL SUCCESS] Location {location_id}: All expected records processed successfully")
    
    return total_processed, total_records
