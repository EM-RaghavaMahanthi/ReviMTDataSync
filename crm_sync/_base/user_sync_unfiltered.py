"""
Unfiltered sync orchestrator for small user-based resources (membership_instances).

Instead of one API call per user, the whole tenant is downloaded by paginating the
endpoint filtered only by `location`, then rows are filtered client-side against the
customer_ids collected during the customers sync. Suits small tables where the total
page count is low relative to the number of users.

fetch_unfiltered_page_fn signature (provided by the resource module):
  async (location_id, page, account_id, api_base_url) -> (rows: list[dict], resp: dict)
Each returned row is the resource's MAPPED dict, carrying a temporary "_uid" key
(the CRM user id) used for the client-side filter and stripped before the S3 write.

Two modes (mirrors location_sync):
  page_end=None — probe page 1 to discover total_pages, then fetch all.
  page_end set  — sharded: fetch only [page_start, page_end], no probe.
"""

import time
import asyncio
import logging

import aiohttp

from core.config import settings
from utils.s3_writer import write_parquet_to_s3
from crm_sync._base.dlq import write_failed, retry_failed

logger = logging.getLogger(__name__)


async def run(
    resource: str,
    fetch_unfiltered_page_fn,
    account_id: str,
    location_id: int,
    api_base_url: str,
    s3_prefix: str,
    customer_ids: list,
    page_start: int = 1,
    page_end: int = None,
    concurrency_limit: int = None,
    parquet_batch_size: int = None,
) -> tuple[int, int]:
    """
    Fetch pages unfiltered, keep rows whose _uid is in customer_ids, write to S3.
    Returns (total_processed, total_fetched).
    """
    tag = resource.upper()
    start_time = time.time()
    sharded = page_end is not None
    if concurrency_limit is None:
        concurrency_limit = settings.CONCURRENCY_LIMIT
    if parquet_batch_size is None:
        parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    id_set = set(str(c) for c in customer_ids)
    if not id_set:
        logger.warning(f"[{tag}] no customer_ids provided — skipping resource")
        return 0, 0
    logger.info(
        f"[{tag}] START location={location_id}, account={account_id}, "
        f"pages={page_start}-{page_end or 'all'}, customers={len(id_set)}"
    )

    semaphore = asyncio.Semaphore(concurrency_limit)
    failed_pages: list = []

    async def fetch_one(page):
        async with semaphore:
            try:
                rows, _ = await fetch_unfiltered_page_fn(location_id, page, account_id, api_base_url)
                return rows, None
            except aiohttp.ClientResponseError as e:
                if e.status in (403, 404):
                    logger.info(f"[{tag}] page={page}: HTTP {e.status} — past last page, skipping")
                    return [], None
                failed_pages.append({
                    "entity_id": str(location_id), "page": page,
                    "error": str(e), "timestamp": time.time(), "extra": {},
                })
                return [], e
            except Exception as e:
                logger.error(f"[{tag}] page={page} failed: {e}")
                failed_pages.append({
                    "entity_id": str(location_id), "page": page,
                    "error": str(e), "timestamp": time.time(), "extra": {},
                })
                return [], e

    total_fetched = 0
    matched: list = []

    if sharded:
        pages = list(range(page_start, page_end + 1))
        logger.info(f"[{tag}] sharded pages {page_start}-{page_end} ({len(pages)} pages)")
        results = await asyncio.gather(*[fetch_one(p) for p in pages])
    else:
        try:
            first_rows, first_resp = await fetch_unfiltered_page_fn(location_id, 1, account_id, api_base_url)
        except Exception as e:
            logger.critical(f"[{tag}] probe page 1 failed: {e}")
            raise
        total_pages = first_resp.get("meta", {}).get("pagination", {}).get("pages", 1)
        logger.info(f"[{tag}] {total_pages} total pages")
        total_fetched += len(first_rows)
        matched.extend(r for r in first_rows if r.get("_uid") in id_set)
        results = (
            await asyncio.gather(*[fetch_one(p) for p in range(2, total_pages + 1)])
            if total_pages > 1 else []
        )

    for rows, err in results:
        if err is None:
            total_fetched += len(rows)
            matched.extend(r for r in rows if r.get("_uid") in id_set)

    # DLQ retry for failed pages
    final_failed_count = 0
    if failed_pages:
        logger.warning(f"[{tag}] {len(failed_pages)} pages failed — retrying via DLQ...")
        dlq_file = await write_failed(failed_pages, resource, str(location_id), account_id)
        recovered, final_failed_count = await retry_failed(
            dlq_file, fetch_unfiltered_page_fn, account_id, api_base_url, concurrency_limit
        )
        if recovered:
            total_fetched += len(recovered)
            matched.extend(r for r in recovered if r.get("_uid") in id_set)

    # Strip temp filter key before write, then batch to S3.
    for r in matched:
        r.pop("_uid", None)

    file_num = page_start if sharded else 1
    total_processed = 0
    for i in range(0, len(matched), parquet_batch_size):
        chunk = matched[i:i + parquet_batch_size]
        if chunk:
            await write_parquet_to_s3(
                chunk, str(location_id), file_num, account_id,
                s3_prefix=s3_prefix, entity_type=resource,
            )
            total_processed += len(chunk)
            file_num += 1

    elapsed = time.time() - start_time
    logger.info(f"[{tag}] DONE: fetched={total_fetched}, matched={total_processed}, elapsed={elapsed:.2f}s")

    if final_failed_count > 0:
        raise Exception(f"{resource}: {final_failed_count} pages permanently failed after retry")

    return total_processed, total_fetched
