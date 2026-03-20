"""
Generic paginated sync orchestrator for location-based resources.

All location-based services (customers, orders, order_lines, class_sessions,
reservations) share the same pagination + S3 batching + DLQ retry pattern.
This module implements it once.

fetch_page_fn signature:
  async (entity_id, page, account_id, api_base_url) -> (data: list, resp: dict)
"""

import time
import asyncio
import logging

from core.config import settings
from utils.s3_writer import write_parquet_to_s3
from crm_sync._base.dlq import write_failed, retry_failed

logger = logging.getLogger(__name__)


async def run(
    entity_id: str,
    account_id: str,
    api_base_url: str,
    fetch_page_fn,
    s3_prefix: str,
    resource: str,
    entity_type: str = "location",
    save_to_s3: bool = True,
    concurrency_limit: int = None,
) -> tuple[int, int]:
    """
    Paginate through all pages for entity_id, batch-write to S3, retry failures via DLQ.
    Returns (total_processed, total_expected).
    """
    start_time = time.time()
    tag = resource.upper()
    logger.info(f"[{tag}] START entity={entity_id}, account={account_id}")

    try:
        first_data, first_resp = await fetch_page_fn(entity_id, 1, account_id, api_base_url)
    except Exception as e:
        logger.critical(f"[{tag}] FATAL entity={entity_id}: first page failed: {e}")
        raise

    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_data))
    logger.info(f"[{tag}] entity={entity_id}: {total_records} records across {total_pages} pages")

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)
    batch_data: list = []
    batch_num = 1
    total_processed = 0
    failed_pages: list = []

    async def flush():
        nonlocal batch_data, batch_num, total_processed
        if not batch_data:
            return
        if save_to_s3:
            await write_parquet_to_s3(
                batch_data, entity_id, batch_num, account_id,
                s3_prefix=s3_prefix, entity_type=entity_type,
            )
            logger.info(f"[{tag}] S3 batch {batch_num}: {len(batch_data)} records, entity={entity_id}")
        total_processed += len(batch_data)
        batch_num += 1
        batch_data = []

    batch_data.extend(first_data)
    if len(batch_data) >= parquet_batch_size:
        await flush()

    if concurrency_limit is None:
        concurrency_limit = settings.CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def fetch_page(page):
        async with semaphore:
            try:
                data, _ = await fetch_page_fn(entity_id, page, account_id, api_base_url)
                return data, page, None
            except Exception as e:
                logger.error(f"[{tag}] entity={entity_id} page={page} failed: {e}")
                failed_pages.append({
                    "entity_id": entity_id, "page": page,
                    "error": str(e), "timestamp": time.time(), "extra": {},
                })
                return [], page, e

    if total_pages > 1:
        results = await asyncio.gather(*[fetch_page(p) for p in range(2, total_pages + 1)])
        for data, _, _ in results:
            batch_data.extend(data)
            if len(batch_data) >= parquet_batch_size:
                await flush()

    await flush()

    # DLQ retry for any failed pages
    final_failed_count = 0
    if failed_pages:
        logger.warning(f"[{tag}] {len(failed_pages)} pages failed, retrying via DLQ...")
        dlq_file = await write_failed(failed_pages, resource, entity_id, account_id)
        recovered, final_failed_count = await retry_failed(
            dlq_file, fetch_page_fn, account_id, api_base_url, concurrency_limit
        )
        if recovered:
            for rec in recovered:
                batch_data.append(rec)
                if len(batch_data) >= parquet_batch_size:
                    await flush()
            await flush()

    elapsed = time.time() - start_time
    missing = total_records - total_processed
    logger.info(f"[{tag}] DONE entity={entity_id}: processed={total_processed}, expected={total_records}, elapsed={elapsed:.2f}s")

    if final_failed_count > 0:
        raise Exception(f"{resource} processing failed: {final_failed_count} pages permanently failed after retry")
    if missing > 0:
        logger.warning(f"[{tag}] MISSING: expected={total_records}, processed={total_processed}, missing={missing}")

    return total_processed, total_records
