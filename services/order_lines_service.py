import os
import json
import time
import logging
import math
from pydantic_core import ValidationError
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

from datetime import datetime, timezone



logger = logging.getLogger(__name__)

def parse_single_transaction(transaction_data):
    if not transaction_data:
        return None, None, None  # no type, no credit or membership id
    
    t = transaction_data[0]  # assume one entry
    t_type = t.get("transaction_type")
    t_id = t.get("transaction_id")

    credit_transactions_id = None
    membership_transactions_id = None

    if t_type == "CreditTransaction":
        credit_transactions_id = t_id
    elif t_type == "MembershipTransaction":
        membership_transactions_id = t_id

    return t_type, credit_transactions_id, membership_transactions_id

async def fetch_order_lines_page(location_id: str, page: int, account_id: str, api_base_url: str):
    """
    Fetches one page of order lines for a given location (async), mapping fields to DB schema.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"location": location_id, "page": page, "page_size": page_size}
    logger.info(f"[FETCH] Location {location_id}, Account {account_id}: Fetching page {page} with page_size {page_size}")
    resp = await api_get("/order_lines", api_base_url, params)
    valid_order_lines = []

    from schemas.revi_schema import OrderLines  # Adjust import path if necessary

    for u in resp.get("data", []):
        attributes = u.get("attributes", {})
        relationships = u.get("relationships", {})
        order_line_id = u["id"]
        order_ref_id = relationships.get("order", {}).get("data", {}).get("id")
        options = attributes.get("options", [])
        processed_by = True
        if len(options) < 1 or any(opt.get("value") == "bill_on_purchase" for opt in options):
            processed_by = False

        transaction_type, credit_transactions_id, membership_transactions_id = parse_single_transaction(attributes.get("transaction_data"))
        crm_downloaded_at = datetime.now(timezone.utc)
        # Extract child_orders list - collect all order IDs where type is "orders"
        child_orders_data = relationships.get("child_orders", {}).get("data", [])
        child_orders = []
        for child in child_orders_data:
            if child.get("type") == "orders" and child.get("id"):
                child_orders.append(child["id"])

        raw = {
            "order_line_id": order_line_id,
            "order_id": order_ref_id,
            "transaction_type": transaction_type,
            "location": str(location_id),
            "title": attributes.get("title"),
            "created_at": None,
            "created_by": None,
            "updated_at": crm_downloaded_at,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "credit_transactions_id": credit_transactions_id,
            "membership_transactions_id": membership_transactions_id,
            "processed_by": processed_by,
            "account_id": account_id,
            "order_ref_id": None,
            "credit_transactions_ref_id": None,
            "membership_transactions_ref_id": None,
            "child_orders": child_orders,
            "is_valid": True  # Default to True, will be updated during validation
        }

        try:
            order_line = OrderLines(**raw)
            valid_order_lines.append(order_line.model_dump())
        except ValidationError as ve:  
            logger.warning(f"[VALIDATION] Location {location_id}, Account {account_id}: Skipping order_line id={u.get('id')} due to validation error: {ve.errors()}")
        except Exception as e:
            logger.error(f"[ERROR] Location {location_id}, Account {account_id}: Unexpected error processing order_line id={u.get('id')}: {e}", exc_info=True)

    logger.info(f"[PAGE PROCESSED] Location {location_id}, Account {account_id}: Page {page} processed, valid order lines: {len(valid_order_lines)}")
    return valid_order_lines, resp

async def write_failed_entries(failed_entries, location_id, account_id):
    """Write failed page entries to DLQ temp file."""
    if not failed_entries:
        return None
    os.makedirs("data", exist_ok=True)
    dlq_file = f"data/failed_order_lines_{location_id}_{account_id}.json"
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
                order_lines, _ = await fetch_order_lines_page(location_id, page, account_id, api_base_url)
                logger.info(f"[DLQ RETRY] Location {location_id}, page {page}: Success on retry - {len(order_lines)} order lines recovered")
                return {"success": True, "page": page, "order_lines": order_lines}
            except Exception as e:
                logger.error(f"[DLQ RETRY] Location {location_id}, page {page}: Failed again: {e}")
                return {"success": False, "page": page, "error": str(e)}
    
    # Process all retries in parallel
    tasks = [retry_single_page(entry) for entry in failed_entries]
    results = await asyncio.gather(*tasks)
    
    # Collect results
    recovered_order_lines = []
    new_failed = []
    
    for result in results:
        if result["success"]:
            recovered_order_lines.extend(result["order_lines"])
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
    
    return recovered_order_lines, len(new_failed)

async def process_order_lines_for_location(location_id: str, account_id: str, api_base_url: str, max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch order line pages in parallel with failure handling and DLQ retry.
    """
    start_time = time.time()
    logger.info(f"[START] Location {location_id}, Account {account_id}: Starting order lines processing.")
    
    # --- Get the first page to know total pages ---
    try:
        first_order_lines, first_resp = await fetch_order_lines_page(location_id, 1, account_id, api_base_url)
    except Exception as e:
        logger.critical(f"[FATAL] Location {location_id}: Failed to fetch first page: {e}")
        raise e
        
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_order_lines))

    logger.info(f"[INFO] Location {location_id}, Account {account_id}: Expected {total_records} order lines across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate order lines into batches
    batch_data = []
    batch_num = 1
    total_processed = 0
    failed_pages = []  # In-memory failure collection

    # Save any accumulated batch to S3
    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data:
            prefix = settings.S3_PREFIXES.get("order_lines", "order_lines-details")
            logger.info(f"[S3 WRITE] Location {location_id}, Account {account_id}: Writing batch {batch_num} with {len(batch_data)} records to S3.")
            await write_parquet_to_s3(batch_data, location_id, batch_num, account_id, s3_prefix=prefix)
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []

    # Process first page
    batch_data.extend(first_order_lines)
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
                order_lines, _ = await fetch_order_lines_page(location_id, page, account_id, api_base_url)
                logger.info(f"[PAGE DONE] Location {location_id}, Account {account_id}: Page {page} fetched, {len(order_lines)} valid order lines.")
                return order_lines, page, None
            except Exception as e:
                logger.error(f"[ERROR] Location {location_id}, Account {account_id}: Page {page} failed: {e}")
                # Store failure in memory
                failed_pages.append({"page": page, "error": str(e), "timestamp": time.time()})
                return [], page, e

    # Process remaining pages in parallel
    if remaining_pages:
        tasks = [fetch_and_process(p) for p in remaining_pages]
        results = await asyncio.gather(*tasks)

        # Collect order lines from successful pages
        for order_lines, page_no, error in results:
            batch_data.extend(order_lines)
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
        recovered_order_lines, final_failed_count = await retry_failed_entries(dlq_file, location_id, account_id, api_base_url, concurrency_limit)
        
        # Process recovered order lines with proper S3 batching
        if recovered_order_lines:
            logger.info(f"[DLQ RECOVERY] Location {location_id}: Processing {len(recovered_order_lines)} recovered order lines")
            for ol in recovered_order_lines:
                batch_data.append(ol)
                # Use the same flush logic as main processing
                if len(batch_data) >= parquet_batch_size:
                    await flush_batch()
            
            logger.info(f"[DLQ RECOVERY] Location {location_id}: {len(recovered_order_lines)} order lines processed from retry")

    # Final flush for any remaining data (including recovered data)
    await flush_batch()

    elapsed = time.time() - start_time
    
    # Final success/failure determination
    missing_records = total_records - total_processed
    
    logger.info(f"[DONE] Location {location_id}, Account {account_id}: Processed in {elapsed:.2f} seconds. Order lines written to S3: {total_processed}")
    
    if final_failed_count > 0:
        logger.error(f"[FINAL FAILURE] Location {location_id}: {final_failed_count} pages permanently failed after retry. Check {dlq_file}")
        raise Exception(f"Order lines processing failed: {final_failed_count} pages could not be processed after retry")
    
    if missing_records > 0:
        logger.warning(f"[MISSING] Location {location_id}, Account {account_id}: Expected {total_records}, but only {total_processed} order lines written to S3. Missing: {missing_records}")
        # Don't raise error for missing records due to validation failures
    else:
        logger.info(f"[FULL SUCCESS] Location {location_id}: All expected records processed successfully")
    
    return total_processed, total_records
