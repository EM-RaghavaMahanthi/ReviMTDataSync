import math
import logging
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

logger = logging.getLogger(__name__)

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
        else: 
            logger.info(f"CUSTOMER_ID FOUND order id={u.get('id')} for location={location_id}, account_id={account_id}, page={page}, customer_id={customer_id}")
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
            "account_id": account_id,
            "created_by": None,
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

async def process_orders_for_location(location_id: str, account_id: str, api_base_url: str, max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch order pages in parallel (async page-level concurrency),
    batching into PARQUET_BATCH_SIZE records per file.
    """
    import time
    start_time = time.time()
    # --- Get the first page to know total pages ---
    first_orders, first_resp = await fetch_orders_page(location_id, 1, account_id, api_base_url)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_orders))

    logger.info(f"[START] Location={location_id}, Account={account_id}: Expected {total_records} records across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate orders into batches
    batch_data = []
    batch_num = 1
    total_processed = 0

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

    # process first page (sequential)
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
                return [], page, e

    tasks = [fetch_and_process(p) for p in remaining_pages]
    results = await asyncio.gather(*tasks)

    for orders, page_no, error in results:
        batch_data.extend(orders)
        logger.info(f"[BATCH] location={location_id}, account_id={account_id}, page={page_no}, records_added={len(orders)}")
        if len(batch_data) >= parquet_batch_size:
            await flush_batch()

    await flush_batch()

    elapsed = time.time() - start_time
    logger.info(f"[DONE] Location={location_id}, Account={account_id} processed in {elapsed:.2f} seconds. Records written to S3: {total_processed}")
    if total_processed != total_records:
        logger.warning(f"[MISSING] Location={location_id}, Account={account_id}: Expected {total_records}, but only {total_processed} written to S3. Missing: {total_records - total_processed}")
    return total_processed, total_records
