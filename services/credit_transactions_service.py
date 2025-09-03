import math
import logging
from utils.api_client import api_get
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio

logger = logging.getLogger(__name__)

from pydantic import ValidationError

async def fetch_credit_transactions_page(user_id: str, page: int, account_id: str, api_base_url: str, location_id: int):
    """
    Fetches one page of credit transactions for a given user.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"user": user_id, "page": page, "page_size": page_size}
    logger.info(f"[FETCH] Fetching credit transactions for user_id={user_id}, account_id={account_id}, page={page}")
    resp = await api_get("/credit_transactions", api_base_url, params)
    from schemas.revi_schema import CreditTransactionOrder
    valid_transactions = []

    def extract_id(rel):
        if not rel:
            return None
        data = rel.get("data")
        if isinstance(data, dict):
            return data.get("id")
        elif isinstance(data, list) and data:
            return data[0].get("id")
        return None
    
    def safe_get_type(relationships, key):
        rel = relationships.get(key)
        if not rel:
            return None
        data = rel.get("data")
        if isinstance(data, dict):
            return data.get("type")
        elif isinstance(data, list) and data:
            return data[0].get("type")
        return None
    


    for u in resp.get("data", []):
        attributes = u.get("attributes", {})
        relationships = u.get("relationships", {})

        # Prepare raw dict for Pydantic model
        raw = {
            "id": None,  # DB-generated, so leave None on insert
            "credit_transactions_id": u.get("id"),
            "transaction_date": attributes.get("transaction_datetime"),
            "credit_name": attributes.get("credit_name"),
            "is_expired": attributes.get("is_expired"),
            "remaining_credits_cache": attributes.get("remaining_credits_cache"),
            "is_intro_offer": attributes.get("is_intro_offer"),
            "parent_credit_transaction_type": safe_get_type(relationships, "parent_credit_transaction"),   # Optional, can extract if needed
            "parent_credit_transaction_id": extract_id(relationships.get("parent_credit_transaction")),
            "customer_id": extract_id(relationships.get("user")),                     # Will usually be populated from 'user'/internal logic
            "location": location_id,                        # Can map from context or a relationship if present
            "created_at": None,                      # DB default
            "created_by": None,
            "updated_at": None,                      # DB default
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "customer_ref_id": None,  # External user reference
            "account_id": account_id,                # Set if needed
        }

        # Log location and account_id if location is present
        if raw["location"] is not None:
            logger.info(f"[LOCATION] Location found for transaction_id={raw['credit_transactions_id']}, account_id={account_id}: {raw['location']}")

        try:
            transaction = CreditTransactionOrder(**raw)
            valid_transactions.append(transaction.model_dump())
        except ValidationError as ve:
            logger.warning(f"[VALIDATION] Skipping credit_transaction id={u.get('id')} due to validation errors: {ve.errors()}")
        except Exception as e:
            logger.error(f"[ERROR] Unexpected error processing credit_transaction id={u.get('id')}: {e}", exc_info=True)
    logger.info(f"[PAGE PROCESSED] user_id={user_id}, account_id={account_id}, page={page}, transactions={len(valid_transactions)}")
    return valid_transactions, resp


async def process_credit_transactions_for_user(user_id: str, account_id: str, location_id: int, entity_type: str = "user", max_workers: int = 8, concurrency_limit: int = None):
    """
    Fetch credit transaction pages in parallel (async page-level concurrency),
    batching into PARQUET_BATCH_SIZE records per file.
    """
    import time
    start_time = time.time()
    logger.info(f"[START] Processing credit transactions for user_id={user_id}, account_id={account_id}, location_id={location_id}")
    # --- Get the first page to know total pages ---
    first_transactions, first_resp = await fetch_credit_transactions_page(user_id, 1, account_id, location_id, )
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_transactions))

    logger.info(f"[INFO] User {user_id}, Account {account_id}: Expected {total_records} credit transactions across {total_pages} pages.")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    # Accumulate transactions into batches
    batch_data = []
    batch_num = 1
    total_processed = 0

    # Save any accumulated batch to S3
    async def flush_batch():
        nonlocal batch_data, batch_num, total_processed
        if batch_data:
            prefix = settings.S3_PREFIXES.get("credit_transactions", "credit-transactions-details")
            logger.info(f"[S3 WRITE] Writing batch {batch_num} for user_id={user_id}, account_id={account_id}, location_id={location_id}, records={len(batch_data)}")
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
                transactions, _ = await fetch_credit_transactions_page(user_id, page, account_id, location_id)
                logger.info(f"[PAGE DONE] user_id={user_id}, account_id={account_id}, location_id={location_id}, page={page}, transactions={len(transactions)}")
                return transactions, page, None
            except Exception as e:
                logger.error(f"[ERROR] User {user_id}, Account {account_id}, location_id={location_id}, page {page} failed: {e}")
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
    logger.info(f"[DONE] User {user_id}, Account {account_id} processed in {elapsed:.2f} seconds. Credit transactions written to S3: {total_processed}")
    if total_processed != total_records:
        logger.warning(f"[MISSING] User {user_id}, Account {account_id}: Expected {total_records}, but only {total_processed} credit transactions written to S3. Missing: {total_records - total_processed}")
    return total_processed, total_records

