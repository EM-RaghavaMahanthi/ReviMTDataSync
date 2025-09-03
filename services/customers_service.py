import math
import logging
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

async def process_customers_for_location(location_id: str, account_id: str, api_base_url: str, max_workers: int = 8, concurrency_limit: int = None, save_to_s3: bool = True):
    """
    Fetch customer pages in parallel (async page-level concurrency),
    batching into PARQUET_BATCH_SIZE records per file.
    """
    import time
    start_time = time.time()
    logger.info(f"[START] Location {location_id}, Account {account_id}: Starting customer processing")
    # --- Get the first page to know total pages ---
    first_customers, first_resp = await fetch_customers_page(location_id, 1, account_id, api_base_url)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_customers))

    logger.info(f"[INFO] Location {location_id}, Account {account_id}: Expected {total_records} records across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate customers into batches
    batch_data = []
    batch_num = 1
    total_processed = 0

    # Save any accumulated batch to S3 (no log file)
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

    # process first page (sequential)
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
                return [], page, e

    tasks = [fetch_and_process(p) for p in remaining_pages]
    results = await asyncio.gather(*tasks)

    user_ids = [c["customer_id"] for c in first_customers]
    for customers, page_no, error in results:
        batch_data.extend(customers)
        user_ids.extend([c["customer_id"] for c in customers])
        if len(batch_data) >= parquet_batch_size:
            await flush_batch()

    await flush_batch()

    elapsed = time.time() - start_time
    if save_to_s3:
        logger.info(f"[DONE] Location {location_id}, Account {account_id}: Processed in {elapsed:.2f} seconds. Records written to S3: {total_processed}")
        if total_processed != total_records:
            logger.warning(f"[MISSING] Location {location_id}, Account {account_id}: Expected {total_records}, but only {total_processed} written to S3. Missing: {total_records - total_processed}")
    return total_processed, total_records, user_ids
