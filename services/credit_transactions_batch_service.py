import logging
from utils.s3_writer import write_parquet_to_s3
from core.config import settings
import asyncio
import time
from services.credit_transactions_service import fetch_credit_transactions_page

logger = logging.getLogger(__name__)

async def fetch_credit_transactions_for_user(user_id: str, account_id: str, api_base_url: str, location_id: int):
    """
    Fetch all credit transactions for a user without saving to S3.
    Returns all data for aggregation.
    """
    logger.info(f"[FETCH] Starting credit transactions fetch for user {user_id} (account: {account_id})")
    
    # Get first page to know total pages
    first_transactions, first_resp = await fetch_credit_transactions_page(user_id, 1, account_id, api_base_url, location_id)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_transactions))
    
    logger.info(f"[FETCH] User {user_id} (account: {account_id}): Expected {total_records} credit transactions across {total_pages} pages.")
    
    all_transactions = []
    all_transactions.extend(first_transactions)
    logger.info(f"[FETCH] User {user_id} (account: {account_id}): Processed page 1 with {len(first_transactions)} records.")

    # Fetch remaining pages
    if total_pages > 1:
        concurrency_limit = getattr(settings, "CONCURRENCY_LIMIT", 16)
        semaphore = asyncio.Semaphore(concurrency_limit)
        async def fetch_page(page):
            async with semaphore:
                try:
                    transactions, _ = await fetch_credit_transactions_page(user_id, page, account_id, api_base_url, location_id)
                    logger.info(f"[FETCH] User {user_id} (account: {account_id}): Processed page {page} with {len(transactions)} records.")
                    return transactions
                except Exception as e:
                    logger.error(f"[ERROR] User {user_id} (account: {account_id}) page {page} failed: {e}")
                    return []
        remaining_pages = list(range(2, total_pages + 1))
        tasks = [fetch_page(p) for p in remaining_pages]
        results = await asyncio.gather(*tasks)
        for transactions in results:
            all_transactions.extend(transactions)
    
    logger.info(f"[FETCH] User {user_id} (account: {account_id}) completed. Fetched {len(all_transactions)} credit transactions.")
    return all_transactions, total_records

async def process_credit_transactions_batch(user_ids: list, batch_id: int, account_id: str, api_base_url: str, location_id: int):
    """
    Process multiple users and save aggregated data with batch_id.
    """
    start_time = time.time()
    logger.info(f"[BATCH {batch_id}] Processing {len(user_ids)} users for credit transactions (account: {account_id})")
    
    # Fetch all users in parallel
    concurrency_limit = getattr(settings, "CONCURRENCY_LIMIT", 16)
    semaphore = asyncio.Semaphore(concurrency_limit)
    
    async def guarded_fetch_user(user_id):
        async with semaphore:
            try:
                logger.info(f"[BATCH {batch_id}] Starting fetch for user {user_id} (account: {account_id}, location: {location_id})")
                return await fetch_credit_transactions_for_user(user_id, account_id, api_base_url, location_id)
            except Exception as e:
                logger.error(f"[BATCH {batch_id}] User {user_id} (account: {account_id}, location: {location_id}) failed: {e}")
                return [], 0

    tasks = [guarded_fetch_user(uid) for uid in user_ids]
    results = await asyncio.gather(*tasks)
    
    # Aggregate all data
    all_data = []
    total_expected = 0
    for user_data, expected in results:
        all_data.extend(user_data)
        total_expected += expected
    
    logger.info(f"[BATCH {batch_id}] Aggregated {len(all_data)} credit transactions from {len(user_ids)} users (account: {account_id})")
    
    # Save in chunks with batch_id
    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)
    prefix = settings.S3_PREFIXES.get("credit_transactions", "credit-transactions-details")
    
    file_num = 1
    total_processed = 0
    
    for i in range(0, len(all_data), parquet_batch_size):
        batch = all_data[i:i+parquet_batch_size]
        await write_parquet_to_s3(batch, str(batch_id), file_num, account_id, s3_prefix=prefix, entity_type="user_batch")
        total_processed += len(batch)
        logger.info(f"[BATCH {batch_id}] Saved file {file_num} with {len(batch)} records (account: {account_id})")
        file_num += 1
    
    elapsed = time.time() - start_time
    logger.info(f"[BATCH {batch_id}] Completed in {elapsed:.2f} seconds. Total processed: {total_processed}, Expected: {total_expected} (account: {account_id})")
    
    return total_processed, total_expected
