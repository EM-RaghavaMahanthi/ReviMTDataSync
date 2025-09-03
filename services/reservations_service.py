import math
import logging
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

logger = logging.getLogger(__name__)

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
            "customer_ref_id": None,
            "class_session_ref_id": None,
            "credit_transactions_id": None,
            "membership_transactions_id": None,
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

        try:
            reservation = Reservation(**raw)
            valid_reservations.append(reservation.dict())
        except Exception as e:
            logger.warning(f"[VALIDATION] Skipping reservation id={raw.get('reservations_id')} for location={entity_id}, account_id={account_id}: {e}")

    logger.info(f"[PAGE] Processed page={page} for location={entity_id}, account_id={account_id}, reservations_found={len(valid_reservations)}")
    return valid_reservations, resp

async def process_reservations_for_entity(entity_id: str, account_id: str, api_base_url: str, entity_type: str = "location", max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch reservation pages in parallel (async page-level concurrency),
    batching into PARQUET_BATCH_SIZE records per file.
    """
    import time
    start_time = time.time()
    logger.info(f"[START] Processing reservations for location={entity_id}, account_id={account_id}")

    first_reservations, first_resp = await fetch_reservations_page(entity_id, 1, entity_type, account_id, api_base_url)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_reservations))

    logger.info(f"[INFO] location={entity_id}, account_id={account_id}: Expected {total_records} reservations across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)
    batch_data = []
    batch_num = 1
    total_processed = 0

    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data:
            prefix = settings.S3_PREFIXES.get("reservations", "reservations-details")
            logger.info(f"[S3] Writing batch {batch_num} for location={entity_id}, account_id={account_id} with {len(batch_data)} reservations to S3")
            await write_parquet_to_s3(batch_data, entity_id, batch_num, account_id, s3_prefix=prefix, entity_type=entity_type)
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []

    batch_data.extend(first_reservations)
    if len(batch_data) >= parquet_batch_size:
        await flush_batch()

    remaining_pages = list(range(2, total_pages + 1))
    if concurrency_limit is None:
        concurrency_limit = settings.CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def fetch_and_process(page):
        async with semaphore:
            try:
                reservations, _ = await fetch_reservations_page(entity_id, page, entity_type, account_id, api_base_url)
                logger.info(f"[PAGE] Completed page={page} for location={entity_id}, account_id={account_id}, reservations_found={len(reservations)}")
                return reservations, page, None
            except Exception as e:
                logger.error(f"[ERROR] location={entity_id}, account_id={account_id}, page={page} failed: {e}")
                return [], page, e

    tasks = [fetch_and_process(p) for p in remaining_pages]
    results = await asyncio.gather(*tasks)

    for reservations, page_no, error in results:
        batch_data.extend(reservations)
        if len(batch_data) >= parquet_batch_size:
            await flush_batch()

    await flush_batch()

    elapsed = time.time() - start_time
    logger.info(f"[DONE] location={entity_id}, account_id={account_id} processed in {elapsed:.2f} seconds. Reservations written to S3: {total_processed}")
    if total_processed != total_records:
        logger.warning(f"[MISSING] location={entity_id}, account_id={account_id}: Expected {total_records}, but only {total_processed} reservations written to S3. Missing: {total_records - total_processed}")
    return total_processed, total_records
