import math
import logging

from pydantic_core import ValidationError
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

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
        if len(options) < 1 or any(opt.get("value") == "bill_on_purchase" for opt in options):
            processed_by = False
        else:
            processed_by = True

        transaction_type, credit_transactions_id, membership_transactions_id = parse_single_transaction(attributes.get("transaction_data"))

        raw = {
            "order_line_id": order_line_id,
            "order_id": order_ref_id,
            "transaction_type": transaction_type,
            "location": str(location_id),
            "title": attributes.get("title"),
            "created_at": None,
            "created_by": None,
            "updated_at": None,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "credit_transactions_id": credit_transactions_id,
            "membership_transactions_id": membership_transactions_id,
            "processed_by": processed_by,
            "account_id": account_id,
            "order_ref_id": None,
            "credit_transactions_ref_id": None,
            "membership_transactions_ref_id": None
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



async def process_order_lines_for_location(location_id: str, account_id: str, api_base_url: str, max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch order line pages in parallel (async page-level concurrency),
    batching into PARQUET_BATCH_SIZE records per file.
    """
    import time
    start_time = time.time()
    logger.info(f"[START] Location {location_id}, Account {account_id}: Starting order lines processing.")
    # --- Get the first page to know total pages ---
    first_order_lines, first_resp = await fetch_order_lines_page(location_id, 1, account_id, api_base_url)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_order_lines))

    logger.info(f"[INFO] Location {location_id}, Account {account_id}: Expected {total_records} order lines across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate order lines into batches
    batch_data = []
    batch_num = 1
    total_processed = 0

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

    # process first page (sequential)
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
                return [], page, e

    tasks = [fetch_and_process(p) for p in remaining_pages]
    results = await asyncio.gather(*tasks)

    for order_lines, page_no, error in results:
        batch_data.extend(order_lines)
        if len(batch_data) >= parquet_batch_size:
            await flush_batch()

    # Flush any remaining order lines
    await flush_batch()

    elapsed = time.time() - start_time
    logger.info(f"[DONE] Location {location_id}, Account {account_id}: Processed in {elapsed:.2f} seconds. Order lines written to S3: {total_processed}")
    if total_processed != total_records:
        logger.warning(f"[MISSING] Location {location_id}, Account {account_id}: Expected {total_records}, but only {total_processed} order lines written to S3. Missing: {total_records - total_processed}")
    return total_processed, total_records
