import os
import json
import time
import logging
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

logger = logging.getLogger(__name__)

from datetime import datetime, timezone



def extract_payment_labels(payment_sources):
    """Aggregate payment source labels into comma-separated string."""
    if not payment_sources:
        return None
    return ", ".join([ps.get("label", "") for ps in payment_sources if ps.get("label")])

async def fetch_orders_page(location_id: str, page: int, account_id: str, api_base_url: str):
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"location": location_id, "page": page, "page_size": page_size}
    logger.info(f"[FETCH] Fetching orders for location={location_id}, account_id={account_id}, page={page}")
    try:
        resp = await api_get("/orders", api_base_url, params)
    except Exception as e:
        logger.error(f"[API] Failed to fetch orders for location={location_id}, account_id={account_id}, page={page}: {repr(e)}")
        raise
    from schemas.revi_schema import Order
    valid_orders = []
    data_list = resp.get("data", [])
    crm_downloaded_at = datetime.now(timezone.utc)
    if not isinstance(data_list, list):
        logger.error(f"[API] Unexpected data format for location={location_id}, account_id={account_id}, page={page}: {data_list}")
        raise ValueError("API response 'data' is not a list")
    for u in data_list:
        attributes = u.get("attributes", {})
        relationships = u.get("relationships", {})

        def extract_id_from_relationship(rel_name):
            rel = relationships.get(rel_name, {})
            data = rel.get("data")
            if isinstance(data, dict):
                return data.get("id")
            elif isinstance(data, list) and data:
                return data[0].get("id")
            return None

        customer_id = extract_id_from_relationship("user")
        if not customer_id:
            logger.info(f"CUSTOMER_ID NOT FOUND order id={u.get('id')} for location={location_id}, account_id={account_id}, page={page}, customer_id={customer_id}")
    
        # Extract parent_order from relationships
        parent_order_id = extract_id_from_relationship("parent_order")
        
        raw = {
            "order_id": u.get("id"),
            "date_placed": attributes.get("date_placed"),
            "location": attributes.get("location"),
            "location_id": location_id, 
            "payment_sources_labels": extract_payment_labels(attributes.get("payment_sources")),
            "status": attributes.get("status"),
            "order_lines_id": extract_id_from_relationship("order_lines"),
            "customer_ref_id": None,
            "customer_id": extract_id_from_relationship("user"),
            "parent_order": parent_order_id,  # Add parent_order field
            "account_id": account_id,
            "created_at": None,
            "created_by": None,
            "updated_at": crm_downloaded_at,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None
        }
        try:
            order = Order(**raw)
            valid_orders.append(order.model_dump())
        except Exception as e:
            logger.warning(f"[VALIDATION] Skipping order id={u.get('id')} for location={location_id}, account_id={account_id}, page={page}: {e}")
    logger.info(f"[PAGE PROCESSED] location={location_id}, account_id={account_id}, page={page}, orders_found={len(valid_orders)}")
    return valid_orders, resp

async def write_failed_entries(failed_entries, location_id, account_id):
    """Write failed page entries to DLQ temp file."""
    if not failed_entries:
        return None
    os.makedirs("data", exist_ok=True)
    dlq_file = f"data/failed_orders_{location_id}_{account_id}.json"
    with open(dlq_file, "w") as f:
        for entry in failed_entries:
            f.write(json.dumps(entry) + "\n")
    logger.warning(f"[DLQ WRITE] Location={location_id}: {len(failed_entries)} pages written to {dlq_file}")
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
                logger.info(f"[DLQ RETRY] Location={location_id}: Retrying page {page}")
                orders, _ = await fetch_orders_page(location_id, page, account_id, api_base_url)
                logger.info(f"[DLQ RETRY] Location={location_id}, page {page}: Success on retry - {len(orders)} orders recovered")
                return {"success": True, "page": page, "orders": orders}
            except Exception as e:
                logger.error(f"[DLQ RETRY] Location={location_id}, page {page}: Failed again: {e}")
                return {"success": False, "page": page, "error": str(e)}
    
    # Process all retries in parallel
    tasks = [retry_single_page(entry) for entry in failed_entries]
    results = await asyncio.gather(*tasks)
    
    # Collect results
    recovered_orders = []
    new_failed = []
    
    for result in results:
        if result["success"]:
            recovered_orders.extend(result["orders"])
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
        logger.error(f"[DLQ FINAL] Location={location_id}: {len(new_failed)} pages still failing after retry")
    else:
        os.remove(dlq_file)
        logger.info(f"[DLQ SUCCESS] Location={location_id}: All failed pages recovered, DLQ file removed")
    
    return recovered_orders, len(new_failed)

async def process_orders_for_location(location_id: str, account_id: str, api_base_url: str, max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch order pages in parallel with failure handling and DLQ retry.
    """
    start_time = time.time()
    logger.info(f"[START] Processing orders for location={location_id} (account {account_id})")
    
    # --- Get the first page to know total pages ---
    try:
        first_orders, first_resp = await fetch_orders_page(location_id, 1, account_id, api_base_url)
    except Exception as e:
        logger.critical(f"[FATAL] Location={location_id}: Failed to fetch first page: {e}")
        raise e
        
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_orders))

    logger.info(f"[INFO] Location={location_id}, Account={account_id}: Expected {total_records} records across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate orders into batches
    batch_data = []
    batch_num = 1
    total_processed = 0
    failed_pages = []  # In-memory failure collection

    # Save any accumulated batch to S3
    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data:
            prefix = settings.S3_PREFIXES["orders"]
            logger.info(f"[S3 WRITE] Writing batch {batch_num} for location={location_id}, account_id={account_id}, records={len(batch_data)}")
            await write_parquet_to_s3(batch_data, location_id, batch_num, account_id, s3_prefix=prefix)
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []

    # Process first page
    batch_data.extend(first_orders)
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
                orders, _ = await fetch_orders_page(location_id, page, account_id, api_base_url)
                return orders, page, None
            except Exception as e:
                import traceback
                logger.error(f"[ERROR] Location={location_id}, Account={account_id}, page={page} failed: {repr(e)}\nTraceback: {traceback.format_exc()}")
                # Store failure in memory
                failed_pages.append({"page": page, "error": str(e), "timestamp": time.time()})
                return [], page, e

    # Process remaining pages in parallel
    if remaining_pages:
        tasks = [fetch_and_process(p) for p in remaining_pages]
        results = await asyncio.gather(*tasks)

        # Collect orders from successful pages
        for orders, page_no, error in results:
            batch_data.extend(orders)
            logger.info(f"[BATCH] location={location_id}, account_id={account_id}, page={page_no}, records_added={len(orders)}")
            if len(batch_data) >= parquet_batch_size:
                await flush_batch()

    # Flush any remaining data from initial processing
    await flush_batch()

    # Handle failures with DLQ retry
    dlq_file = None
    final_failed_count = 0
    
    if failed_pages:
        logger.warning(f"[PARTIAL SUCCESS] Location={location_id}: {len(failed_pages)} pages failed initially")
        dlq_file = await write_failed_entries(failed_pages, location_id, account_id)
        
        # Retry failed pages
        logger.info(f"[DLQ RETRY] Location={location_id}: Retrying {len(failed_pages)} failed pages")
        recovered_orders, final_failed_count = await retry_failed_entries(dlq_file, location_id, account_id, api_base_url, concurrency_limit)
        
        # Process recovered orders with proper S3 batching
        if recovered_orders:
            logger.info(f"[DLQ RECOVERY] Location={location_id}: Processing {len(recovered_orders)} recovered orders")
            for order in recovered_orders:
                batch_data.append(order)
                # Use the same flush logic as main processing
                if len(batch_data) >= parquet_batch_size:
                    await flush_batch()
            
            logger.info(f"[DLQ RECOVERY] Location={location_id}: {len(recovered_orders)} orders processed from retry")

    # Final flush for any remaining data (including recovered data)
    await flush_batch()

    elapsed = time.time() - start_time
    
    # Final success/failure determination
    missing_records = total_records - total_processed
    
    logger.info(f"[DONE] Location={location_id}, Account={account_id} processed in {elapsed:.2f} seconds. Records written to S3: {total_processed}")
    
    if final_failed_count > 0:
        logger.error(f"[FINAL FAILURE] Location={location_id}: {final_failed_count} pages permanently failed after retry. Check {dlq_file}")
        raise Exception(f"Order processing failed: {final_failed_count} pages could not be processed after retry")
    
    if missing_records > 0:
        logger.warning(f"[MISSING] Location={location_id}, Account={account_id}: Expected {total_records}, but only {total_processed} written to S3. Missing: {missing_records}")
        # Don't raise error for missing records due to validation failures
    else:
        logger.info(f"[FULL SUCCESS] Location={location_id}: All expected records processed successfully")
    
    return total_processed, total_records
