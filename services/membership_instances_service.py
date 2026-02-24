import logging
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

from datetime import datetime, timezone



logger = logging.getLogger(__name__)

from pydantic import ValidationError

async def fetch_membership_instances_page(user_id: str, page: int, account_id: str, api_base_url: str, location_id: int):
    """
    Fetches one page of membership instances for a given user.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"user": user_id, "page": page, "page_size": page_size}
    logger.info(f"[FETCH] Fetching membership instances for user {user_id}, account {account_id}, location {location_id}, page {page}")
    resp = await api_get("/membership_instances", api_base_url, params)
    from schemas.revi_schema import MembershipInstance
    crm_downloaded_at = datetime.now(timezone.utc)

    def extract_id(rel):
        if not rel:
            return None
        data = rel.get("data")
        if isinstance(data, dict):
            return data.get("id")
        elif isinstance(data, list) and data:
            return data[0].get("id")
        return None

    valid_instances = []
    for u in resp.get("data", []):
        attributes = u.get("attributes", {})
        relationships = u.get("relationships", {})

        # Use the passed location_id parameter instead of extracting from API
        api_location_id = extract_id(relationships.get("purchase_location"))
        if api_location_id is not None:
            logger.info(f"[LOCATION] MembershipInstance id={u.get('id')} API location={api_location_id}, using passed location={location_id} account_id={account_id}")

        # Map fields to DB schema; supply only what is in your table
        raw = {
            "id": None,  # DB-generated, not supplied on insert
            "membership_instances_id": int(u.get("id")) if u.get("id") is not None else None,
            "purchase_date": attributes.get("purchase_date"),
            "membership_name": attributes.get("membership_name"),
            "renewal_rate_incl_tax": attributes.get("renewal_rate_incl_tax"),
            "status": attributes.get("status"),
            "location": int(location_id) if location_id is not None else None,  # Use passed location_id
            "renewal_count": attributes.get("renewal_count"),
            "next_charge_date": attributes.get("next_charge_date"),
            "created_at": None,         # Leave for DB default
            "created_by": None,
            "updated_at": crm_downloaded_at,         # Leave for DB default
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "account_id": account_id
        }
        try:
            validated = MembershipInstance(**raw)
            valid_instances.append(validated.model_dump())
        except ValidationError as ve:
            logger.warning(f"[VALIDATION] Skipping membership_instance id={u.get('id')} due to validation errors: {ve.errors()}")
        except Exception as e:
            logger.error(f"[ERROR] Unexpected error processing membership_instance id={u.get('id')}: {e}", exc_info=True)
    logger.info(f"[PAGE] Processed page {page} for user {user_id}, account {account_id}: {len(valid_instances)} valid instances")
    return valid_instances, resp


async def process_membership_instances_for_user(user_id: str, account_id: str, api_base_url: str, entity_type: str = "user", max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch membership instance pages in parallel (async page-level concurrency),
    batching into PARQUET_BATCH_SIZE records per file.
    """
    import time
    start_time = time.time()
    # --- Get the first page to know total pages ---
    first_instances, first_resp = await fetch_membership_instances_page(user_id, 1, account_id)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_instances))

    logger.info(f"[INFO] User {user_id}, account {account_id}: Expected {total_records} membership instances across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate instances into batches
    batch_data = []
    batch_num = 1
    total_processed = 0

    # Save any accumulated batch to S3
    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data:
            prefix = settings.S3_PREFIXES.get("membership_instances", "membership-instances-details")
            logger.info(f"[S3] Flushing batch {batch_num} for user {user_id}, account {account_id} with {len(batch_data)} records to S3")
            await write_parquet_to_s3(batch_data, user_id, batch_num, account_id, s3_prefix=prefix, entity_type=entity_type)
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []

    # process first page (sequential)
    batch_data.extend(first_instances)
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
                instances, _ = await fetch_membership_instances_page(user_id, page, account_id)
                return instances, page, None
            except Exception as e:
                logger.exception(f"[ERROR] User {user_id}, account {account_id} page {page} failed:")
                return [], page, e

    tasks = [fetch_and_process(p) for p in remaining_pages]
    results = await asyncio.gather(*tasks)

    for instances, page_no, error in results:
        batch_data.extend(instances)
        logger.info(f"[BATCH] Added {len(instances)} instances from page {page_no} for user {user_id}, account {account_id}")
        if len(batch_data) >= parquet_batch_size:
            await flush_batch()

    # Flush any remaining instances
    await flush_batch()

    elapsed = time.time() - start_time
    logger.info(f"[DONE] User {user_id}, account {account_id} processed in {elapsed:.2f} seconds. Membership instances written to S3: {total_processed}")
    if total_processed != total_records:
        logger.warning(f"[MISSING] User {user_id}, account {account_id}: Expected {total_records}, but only {total_processed} membership instances written to S3. Missing: {total_records - total_processed}")
    return total_processed, total_records
