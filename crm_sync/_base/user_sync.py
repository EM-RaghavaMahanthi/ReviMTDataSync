"""
Generic batch sync orchestrator for user-based resources.

credit_transactions, membership_instances, membership_transactions all use
the same pattern: a batch of user_ids → fetch all pages per user in parallel
→ aggregate → DLQ retry → write to S3.

fetch_page_fn signature:
  async (user_id, page, account_id, api_base_url, *, location_id) -> (data: list, resp: dict)
"""

import time
import asyncio
import logging

from core.config import settings
from utils.s3_writer import write_parquet_to_s3
from crm_sync._base.dlq import write_failed, retry_failed

logger = logging.getLogger(__name__)


async def _fetch_all_pages_for_user(
    user_id: str,
    account_id: str,
    api_base_url: str,
    location_id: int,
    fetch_page_fn,
) -> tuple[list, int, list]:
    """
    Fetch all pages for a single user.
    Returns (all_data, total_expected, failed_pages).
    """
    failed_pages = []
    try:
        first_data, first_resp = await fetch_page_fn(
            user_id, 1, account_id, api_base_url, location_id=location_id
        )
    except Exception as e:
        logger.error(f"[USER SYNC] user={user_id}: first page failed: {e}")
        return [], 0, [{
            "entity_id": user_id, "page": "all",
            "error": str(e), "timestamp": time.time(),
            "extra": {"location_id": location_id},
        }]

    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_data))

    all_data = list(first_data)

    if total_pages > 1:
        concurrency_limit = getattr(settings, "CONCURRENCY_LIMIT", 16)
        semaphore = asyncio.Semaphore(concurrency_limit)

        async def fetch_page(page):
            async with semaphore:
                try:
                    data, _ = await fetch_page_fn(
                        user_id, page, account_id, api_base_url, location_id=location_id
                    )
                    return data, page, None
                except Exception as e:
                    logger.error(f"[USER SYNC] user={user_id} page={page} failed: {e}")
                    failed_pages.append({
                        "entity_id": user_id, "page": page,
                        "error": str(e), "timestamp": time.time(),
                        "extra": {"location_id": location_id},
                    })
                    return [], page, e

        results = await asyncio.gather(*[fetch_page(p) for p in range(2, total_pages + 1)])
        for data, _, _ in results:
            all_data.extend(data)

    return all_data, total_records, failed_pages


async def run_batch(
    user_ids: list,
    batch_id: int,
    account_id: str,
    api_base_url: str,
    location_id: int,
    fetch_page_fn,
    s3_prefix: str,
    resource: str,
    concurrency_limit: int = None,
) -> tuple[int, int]:
    """
    Process a batch of users: fetch all pages in parallel, aggregate, DLQ retry, write to S3.
    Returns (total_processed, total_expected).
    """
    start_time = time.time()
    tag = resource.upper()
    logger.info(f"[{tag}] BATCH {batch_id}: {len(user_ids)} users (account={account_id}, location={location_id})")

    if concurrency_limit is None:
        concurrency_limit = getattr(settings, "CONCURRENCY_LIMIT", 16)
    semaphore = asyncio.Semaphore(concurrency_limit)
    all_batch_failures: list = []

    async def fetch_user(user_id):
        async with semaphore:
            try:
                data, expected, failures = await _fetch_all_pages_for_user(
                    user_id, account_id, api_base_url, location_id, fetch_page_fn
                )
                all_batch_failures.extend(failures)
                return data, expected
            except Exception as e:
                logger.error(f"[{tag}] BATCH {batch_id}: user={user_id} completely failed: {e}")
                all_batch_failures.append({
                    "entity_id": user_id, "page": "all",
                    "error": str(e), "timestamp": time.time(),
                    "extra": {"location_id": location_id},
                })
                return [], 0

    results = await asyncio.gather(*[fetch_user(uid) for uid in user_ids])

    all_data = []
    total_expected = 0
    for data, expected in results:
        all_data.extend(data)
        total_expected += expected

    logger.info(f"[{tag}] BATCH {batch_id}: fetched {len(all_data)} records, expected={total_expected}")

    # DLQ retry for failed user pages
    final_failed_count = 0
    if all_batch_failures:
        dlq_file = await write_failed(all_batch_failures, resource, f"batch_{batch_id}", account_id)
        recovered, final_failed_count = await retry_failed(
            dlq_file, fetch_page_fn, account_id, api_base_url, concurrency_limit
        )
        if recovered:
            all_data.extend(recovered)
            logger.info(f"[{tag}] BATCH {batch_id}: {len(recovered)} records recovered from DLQ")

    # Write to S3 in parquet chunks
    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)
    file_num = 1
    total_processed = 0
    for i in range(0, len(all_data), parquet_batch_size):
        chunk = all_data[i:i + parquet_batch_size]
        await write_parquet_to_s3(
            chunk, str(batch_id), file_num, account_id,
            s3_prefix=s3_prefix, entity_type="user_batch",
        )
        total_processed += len(chunk)
        logger.info(f"[{tag}] BATCH {batch_id}: file {file_num} → {len(chunk)} records")
        file_num += 1

    elapsed = time.time() - start_time
    missing = total_expected - total_processed
    logger.info(f"[{tag}] BATCH {batch_id} DONE: processed={total_processed}, expected={total_expected}, elapsed={elapsed:.2f}s")

    if final_failed_count > 0:
        raise Exception(f"{resource} batch {batch_id} failed: {final_failed_count} user pages permanently failed after retry")
    if missing > 0:
        logger.warning(f"[{tag}] BATCH {batch_id} MISSING: expected={total_expected}, processed={total_processed}")

    return total_processed, total_expected
