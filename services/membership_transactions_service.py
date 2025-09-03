import logging
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

logger = logging.getLogger(__name__)

from pydantic import ValidationError

async def fetch_membership_transactions_page(user_id: str, page: int, account_id: str, api_base_url: str, location_id: int):
    """
    Fetches one page of membership transactions for a given user.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"user": user_id, "page": page, "page_size": page_size}
    logger.info(f"[FETCH] Fetching page {page} for user {user_id} (account_id={account_id}, location_id={location_id}) with page_size={page_size}")
    resp = await api_get("/membership_transactions", api_base_url, params)
    from schemas.revi_schema import MembershipTransaction

    def extract_id(rel):
        if not rel:
            return None
        data = rel.get("data")
        if isinstance(data, dict):
            return data.get("id")
        elif isinstance(data, list) and data:
            return data[0].get("id")
        return None

    valid_transactions = []
    for u in resp.get("data", []):
        attributes = u.get("attributes", {})
        relationships = u.get("relationships", {})

        raw = {
            "id": None, 
            "membership_transactions_id": int(u.get("id")) if u.get("id") is not None else None,
            "transaction_date": attributes.get("transaction_datetime"),   
            "membership_name": attributes.get("membership_name"),
            "parent_membership_transaction_id": int(extract_id(relationships.get("parent_membership_transaction"))) if extract_id(relationships.get("parent_membership_transaction")) is not None else None,
            "membership_instances_id": int(extract_id(relationships.get("membership_instance"))) if extract_id(relationships.get("membership_instance")) is not None else None,
            "customer_id": extract_id(relationships.get("user")),   
            "location": location_id,  # Set location to location_id
            "payment_interval_end_date": attributes.get("payment_interval_end_date"),
            "created_at": None,
            "created_by": None,
            "updated_at": None,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "customer_ref_id": None,  
            "account_id": account_id,
            "membership_instances_ref_id": None,  
            "next_charge_date": attributes.get("next_charge_date"),
        }

        try:
            validated = MembershipTransaction(**raw)
            valid_transactions.append(validated.model_dump())
        except ValidationError as ve:
            logger.warning(f"[VALIDATION] Skipping membership_transaction id={u.get('id')} due to validation errors: {ve.errors()}")
        except Exception as e:
            logger.error(f"[ERROR] Unexpected error processing membership_transaction id={u.get('id')}: {e}", exc_info=True)

    logger.info(f"[PAGE PROCESSED] Page {page} for user {user_id} (account_id={account_id}, location_id={location_id}) processed with {len(valid_transactions)} valid transactions.")
    return valid_transactions, resp


async def process_membership_transactions_for_user(user_id: str, account_id: str, location_id: int, entity_type: str = "user", max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch membership transaction pages in parallel (async page-level concurrency),
    batching into PARQUET_BATCH_SIZE records per file.
    """
    import time
    start_time = time.time()
    # --- Get the first page to know total pages ---
    first_transactions, first_resp = await fetch_membership_transactions_page(user_id, 1, account_id, location_id)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_transactions))

    logger.info(f"[START] User {user_id} (account_id={account_id}): Expected {total_records} membership transactions across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate transactions into batches
    batch_data = []
    batch_num = 1
    total_processed = 0

    # Save any accumulated batch to S3
    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data:
            prefix = settings.S3_PREFIXES.get("membership_transactions", "membership-transactions-details")
            logger.info(f"[S3 WRITE] Writing batch {batch_num} for user {user_id} (account_id={account_id}, location_id={location_id}) with {len(batch_data)} transactions to S3.")
            await write_parquet_to_s3(batch_data, user_id, batch_num, account_id, s3_prefix=prefix, entity_type=entity_type)
            total_processed += len(batch_data)
            batch_num += 1
            batch_data = []

    # process first page (sequential)
    batch_data.extend(first_transactions)
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
                transactions, _ = await fetch_membership_transactions_page(user_id, page, account_id, location_id)
                logger.info(f"[PAGE DONE] User {user_id} (account_id={account_id}, location_id={location_id}) page {page} processed with {len(transactions)} transactions.")
                return transactions, page, None
            except Exception as e:
                logger.error(f"[ERROR] User {user_id} (account_id={account_id}, location_id={location_id}) page {page} failed: {e}")
                return [], page, e

    tasks = [fetch_and_process(p) for p in remaining_pages]
    results = await asyncio.gather(*tasks)

    for transactions, page_no, error in results:
        batch_data.extend(transactions)
        if len(batch_data) >= parquet_batch_size:
            await flush_batch()

    # Flush any remaining transactions
    await flush_batch()

    elapsed = time.time() - start_time
    logger.info(f"[DONE] User {user_id} (account_id={account_id}, location_id={location_id}) processed in {elapsed:.2f} seconds. Membership transactions written to S3: {total_processed}")
    if total_processed != total_records:
        logger.warning(f"[MISSING] User {user_id} (account_id={account_id}, location_id={location_id}): Expected {total_records}, but only {total_processed} membership transactions written to S3. Missing: {total_records - total_processed}")
    return total_processed, total_records

