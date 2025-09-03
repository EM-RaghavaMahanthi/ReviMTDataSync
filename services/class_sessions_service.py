import math
import logging
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

logger = logging.getLogger(__name__)

from pydantic import ValidationError

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

async def process_class_sessions_for_location(location_id: str, account_id: str, api_base_url: str, max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch class session pages in parallel (async page-level concurrency),
    batching into PARQUET_BATCH_SIZE records per file.
    """
    # Ensure location_id is string
    location_id = str(location_id)
    import time
    start_time = time.time()
    logger.info(f"[START] Processing class sessions for location {location_id} (account {account_id})")
    # --- Get the first page to know total pages ---
    first_class_sessions, first_resp = await fetch_class_sessions_page(location_id, 1, account_id, api_base_url)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_class_sessions))

    logger.info(f"[INFO] Location {location_id} (account {account_id}): Expected {total_records} class sessions across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate class sessions into batches
    batch_data = []
    batch_num = 1
    total_processed = 0

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

    # process first page (sequential)
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
                return [], page, e

    tasks = [fetch_and_process(p) for p in remaining_pages]
    results = await asyncio.gather(*tasks)

    for class_sessions, page_no, error in results:
        batch_data.extend(class_sessions)
        logger.info(f"[BATCH] Added {len(class_sessions)} records from page {page_no} to batch for location {location_id} (account {account_id})")
        if len(batch_data) >= parquet_batch_size:
            await flush_batch()

    # Flush any remaining class sessions
    await flush_batch()

    elapsed = time.time() - start_time
    logger.info(f"[DONE] Location {location_id} (account {account_id}) processed in {elapsed:.2f} seconds. Class sessions written to S3: {total_processed}")
    if total_processed != total_records:
        logger.warning(f"[MISSING] Location {location_id} (account {account_id}): Expected {total_records}, but only {total_processed} class sessions written to S3. Missing: {total_records - total_processed}")
    return total_processed, total_records
