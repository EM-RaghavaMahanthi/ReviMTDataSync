import os
import json
import time
import logging
import math
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

logger = logging.getLogger(__name__)

from datetime import datetime, timezone

async def fetch_reservations_page(entity_id: str, page: int, entity_type: str, account_id: str, api_base_url: str):
    """
    Fetches one page of reservations for a given entity (location/account).
    For this version, entity_id is treated as location.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"location": entity_id, "page": page, "page_size": page_size}

    logger.info(f"[FETCH] Fetching reservations for location={entity_id}, account_id={account_id}, page={page}")

    resp = await api_get("/reservations", api_base_url, params)
    from schemas.revi_schema import Reservation  # adjust import as needed
    valid_reservations = []

    crm_downloaded_at = datetime.now(timezone.utc)

    for u in resp.get("data", []):
        attributes = u.get("attributes", {})
        relationships = u.get("relationships", {})

        def get_ref_and_type(rel_name):
            rel = relationships.get(rel_name, {}).get("data")
            if isinstance(rel, list) and rel:
                return rel[0].get("id"), rel[0].get("type")
            elif isinstance(rel, dict):
                return rel.get("id"), rel.get("type")
            return None, None

        raw = {
            "reservations_id": u.get("id"),
            "cancel_date": attributes.get("cancel_date"),
            "check_in_date": attributes.get("check_in_date"),
            "creation_date": attributes.get("creation_date"),
            "status": attributes.get("status"),
            "guest": attributes.get("reserved_for_guest", False),
            "reservation_type": attributes.get("reservation_type"),
            "first_timer": attributes.get("first_timer", False),
            "location": entity_id,
            "credit_transactions_ref_id": None,
            "credit_transactions_type": None,
            "membership_transactions_ref_id": None,
            "membership_transactions_type": None,
            "transaction_type": None,
            "customer_ref_id": None,
            "class_session_ref_id": None,
            "credit_transactions_id": None,
            "membership_transactions_id": None,
            "created_at": None,
            "updated_at": crm_downloaded_at,
            "created_by": None,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "customer_id": None,
            "account_id": account_id,
            "class_session_id": None
        }

        raw["credit_transactions_id"], raw["credit_transactions_type"] = get_ref_and_type("credit_transactions")
        raw["membership_transactions_id"], raw["membership_transactions_type"] = get_ref_and_type("membership_transactions")
        raw["customer_id"] = relationships.get("user", {}).get("data", {}).get("id")
        raw["class_session_id"] = relationships.get("class_session", {}).get("data", {}).get("id")
        raw["transaction_type"] = raw["credit_transactions_type"] if raw["credit_transactions_type"] else raw["membership_transactions_type"]

        try:
            reservation = Reservation(**raw)
            valid_reservations.append(reservation.dict())
        except Exception as e:
            logger.warning(f"[VALIDATION] Skipping reservation id={raw.get('reservations_id')} for location={entity_id}, account_id={account_id}: {e}")

    logger.info(f"[PAGE] Processed page={page} for location={entity_id}, account_id={account_id}, reservations_found={len(valid_reservations)}")
    return valid_reservations, resp

async def write_failed_entries(failed_entries, entity_id, account_id):
    """Write failed page entries to DLQ temp file."""
    if not failed_entries:
        return None
    os.makedirs("data", exist_ok=True)
    dlq_file = f"data/failed_reservations_{entity_id}_{account_id}.json"
    with open(dlq_file, "w") as f:
        for entry in failed_entries:
            f.write(json.dumps(entry) + "\n")
    logger.warning(f"[DLQ WRITE] Location={entity_id}: {len(failed_entries)} pages written to {dlq_file}")
    return dlq_file

async def retry_failed_entries(dlq_file, entity_id, account_id, api_base_url, entity_type, concurrency_limit=None):
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
                logger.info(f"[DLQ RETRY] Location={entity_id}: Retrying page {page}")
                reservations, _ = await fetch_reservations_page(entity_id, page, entity_type, account_id, api_base_url)
                logger.info(f"[DLQ RETRY] Location={entity_id}, page {page}: Success on retry - {len(reservations)} reservations recovered")
                return {"success": True, "page": page, "reservations": reservations}
            except Exception as e:
                logger.error(f"[DLQ RETRY] Location={entity_id}, page {page}: Failed again: {e}")
                return {"success": False, "page": page, "error": str(e)}
    
    # Process all retries in parallel
    tasks = [retry_single_page(entry) for entry in failed_entries]
    results = await asyncio.gather(*tasks)
    
    # Collect results
    recovered_reservations = []
    new_failed = []
    
    for result in results:
        if result["success"]:
            recovered_reservations.extend(result["reservations"])
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
        logger.error(f"[DLQ FINAL] Location={entity_id}: {len(new_failed)} pages still failing after retry")
    else:
        os.remove(dlq_file)
        logger.info(f"[DLQ SUCCESS] Location={entity_id}: All failed pages recovered, DLQ file removed")
    
    return recovered_reservations, len(new_failed)

async def process_reservations_for_entity(entity_id: str, account_id: str, api_base_url: str, entity_type: str = "location", max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch reservation pages in parallel with failure handling and DLQ retry.
    """
    start_time = time.time()
    logger.info(f"[START] Processing reservations for location={entity_id}, account_id={account_id}")
    
    # --- Get the first page to know total pages ---
    try:
        first_reservations, first_resp = await fetch_reservations_page(entity_id, 1, entity_type, account_id, api_base_url)
    except Exception as e:
        logger.critical(f"[FATAL] Location={entity_id}: Failed to fetch first page: {e}")
        raise e
        
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_reservations))

    logger.info(f"[INFO] location={entity_id}, account_id={account_id}: Expected {total_records} reservations across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)
    
    # Accumulate reservations into batches
    batch_data = []
    batch_num = 1
    total_processed = 0
    failed_pages = []  # In-memory failure collection

    # Save any accumulated batch to S3
    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data:
            prefix = settings.S3_PREFIXES.get("reservations", "reservations-details")
            logger.info(f"[S3] Writing batch {batch_num} for location={entity_id}, account_id={account_id} with {len(batch_data)} reservations to S3")
            await write_parquet_to_s3(batch_data, entity_id, batch_num, account_id, s3_prefix=prefix, entity_type=entity_type)
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []

    # Process first page
    batch_data.extend(first_reservations)
    if len(batch_data) >= parquet_batch_size:
        await flush_batch()

    # Prepare remaining pages list
    remaining_pages = list(range(2, total_pages + 1))

    # Use concurrency limit from settings if not provided
    if concurrency_limit is None:
        concurrency_limit = min(settings.CONCURRENCY_LIMIT, 128)
        concurrency_limit = 64
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def fetch_and_process(page):
        async with semaphore:
            try:
                reservations, _ = await fetch_reservations_page(entity_id, page, entity_type, account_id, api_base_url)
                logger.info(f"[PAGE] Completed page={page} for location={entity_id}, account_id={account_id}, reservations_found={len(reservations)}")
                return reservations, page, None
            except Exception as e:
                logger.error(f"[ERROR] location={entity_id}, account_id={account_id}, page={page} failed: {e}")
                # Store failure in memory
                failed_pages.append({"page": page, "error": str(e), "timestamp": time.time()})
                return [], page, e

    # Process remaining pages in parallel
    if remaining_pages:
        tasks = [fetch_and_process(p) for p in remaining_pages]
        results = await asyncio.gather(*tasks)

        # Collect reservations from successful pages
        for reservations, page_no, error in results:
            batch_data.extend(reservations)
            if len(batch_data) >= parquet_batch_size:
                await flush_batch()

    # Flush any remaining data from initial processing
    await flush_batch()

    # Handle failures with DLQ retry
    dlq_file = None
    final_failed_count = 0
    
    if failed_pages:
        logger.warning(f"[PARTIAL SUCCESS] Location={entity_id}: {len(failed_pages)} pages failed initially")
        dlq_file = await write_failed_entries(failed_pages, entity_id, account_id)
        
        # Retry failed pages
        logger.info(f"[DLQ RETRY] Location={entity_id}: Retrying {len(failed_pages)} failed pages")
        recovered_reservations, final_failed_count = await retry_failed_entries(dlq_file, entity_id, account_id, api_base_url, entity_type, concurrency_limit)
        
        # Process recovered reservations with proper S3 batching
        if recovered_reservations:
            logger.info(f"[DLQ RECOVERY] Location={entity_id}: Processing {len(recovered_reservations)} recovered reservations")
            for r in recovered_reservations:
                batch_data.append(r)
                # Use the same flush logic as main processing
                if len(batch_data) >= parquet_batch_size:
                    await flush_batch()
            
            logger.info(f"[DLQ RECOVERY] Location={entity_id}: {len(recovered_reservations)} reservations processed from retry")

    # Final flush for any remaining data (including recovered data)
    await flush_batch()

    elapsed = time.time() - start_time
    
    # Final success/failure determination
    missing_records = total_records - total_processed
    
    logger.info(f"[DONE] location={entity_id}, account_id={account_id} processed in {elapsed:.2f} seconds. Reservations written to S3: {total_processed}")
    
    if final_failed_count > 0:
        logger.error(f"[FINAL FAILURE] Location={entity_id}: {final_failed_count} pages permanently failed after retry. Check {dlq_file}")
        raise Exception(f"Reservations processing failed: {final_failed_count} pages could not be processed after retry")
    
    if missing_records > 0:
        logger.warning(f"[MISSING] location={entity_id}, account_id={account_id}: Expected {total_records}, but only {total_processed} reservations written to S3. Missing: {missing_records}")
        # Don't raise error for missing records due to validation failures
    else:
        logger.info(f"[FULL SUCCESS] Location={entity_id}: All expected records processed successfully")
    
    return total_processed, total_records
