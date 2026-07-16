"""
Generic paginated sync orchestrator for location-based resources.

All location-based services (customers, orders, order_lines, class_sessions,
reservations) share the same pagination + S3 batching + DLQ retry pattern.
This module implements it once.

fetch_page_fn signature:
  async (entity_id, page, account_id, api_base_url) -> (data: list, resp: dict)

Two modes:
  page_end=None (default) — probe page 1 to discover total_pages, then fetch all.
  page_end set            — sharded: fetch only [page_start, page_end], no probe.
                            Used by the Step Functions LocationMap fan-out; each
                            shard is one Lambda invocation over a fixed page range.
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
    entity_id: str,
    account_id: str,
    api_base_url: str,
    fetch_page_fn,
    s3_prefix: str,
    resource: str,
    entity_type: str = "location",
    save_to_s3: bool = True,
    concurrency_limit: int = None,
    page_start: int = 1,
    page_end: int = None,
    side_map_fn=None,
    side_s3_prefix: str = None,
    side_entity_type: str = "side",
) -> tuple[int, int]:
    """
    Paginate through pages for entity_id, batch-write to S3, retry failures via DLQ.
    Returns (total_processed, total_expected). In sharded mode total_expected is 0
    (unknown — no probe).

    Optional side output: if side_map_fn is given, each page's RAW response is passed to
    side_map_fn(resp, account_id, entity_id, api_base_url) -> list[dict]; those rows are batched and
    written to side_s3_prefix (entity_type=side_entity_type). Used to derive a second
    dataset (e.g. customer tag assignments) from the same fetch — no extra API call.
    """
    start_time = time.time()
    tag = resource.upper()
    sharded = page_end is not None
    logger.info(
        f"[{tag}] START entity={entity_id}, account={account_id}, "
        f"pages={page_start}-{page_end or 'all'}"
    )

    parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)
    batch_data: list = []
    # batch_num starts at page_start so shards of the same resource never write
    # colliding S3 keys (shard p1-200 uses batch 1,2,3; shard p201-400 uses 201,202,...).
    batch_num = page_start
    total_processed = 0
    total_records = 0
    failed_pages: list = []

    side_batch: list = []
    side_num = page_start
    side_total = 0

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

    async def flush_side():
        nonlocal side_batch, side_num, side_total
        if not side_batch:
            return
        if save_to_s3 and side_s3_prefix:
            await write_parquet_to_s3(
                side_batch, entity_id, side_num, account_id,
                s3_prefix=side_s3_prefix, entity_type=side_entity_type,
            )
            logger.info(f"[{tag}] side S3 batch {side_num}: {len(side_batch)} rows → {side_entity_type}")
        side_total += len(side_batch)
        side_num += 1
        side_batch = []

    def extract_side(resp):
        if side_map_fn is None or not resp:
            return
        try:
            side_batch.extend(side_map_fn(resp, account_id, entity_id, api_base_url))
        except Exception as e:
            logger.error(f"[{tag}] side_map_fn failed on a page: {e}")

    if not sharded:
        # Probe page 1 to discover total_pages.
        try:
            first_data, first_resp = await fetch_page_fn(entity_id, 1, account_id, api_base_url)
        except Exception as e:
            logger.critical(f"[{tag}] FATAL entity={entity_id}: first page failed: {e}")
            raise
        pagination = first_resp.get("meta", {}).get("pagination", {})
        page_end = pagination.get("pages", 1)
        total_records = pagination.get("count", len(first_data))
        logger.info(f"[{tag}] entity={entity_id}: {total_records} records across {page_end} pages")
        batch_data.extend(first_data)
        extract_side(first_resp)
        if len(batch_data) >= parquet_batch_size:
            await flush()
        concurrent_pages = list(range(2, page_end + 1))
    else:
        # Sharded: fetch the fixed page range concurrently — no probe.
        concurrent_pages = list(range(page_start, page_end + 1))

    if concurrency_limit is None:
        concurrency_limit = settings.CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def fetch_page(page):
        async with semaphore:
            try:
                data, resp = await fetch_page_fn(entity_id, page, account_id, api_base_url)
                return data, resp, page, None
            except aiohttp.ClientResponseError as e:
                # 403/404 on a paginated fetch = API over-reported total pages; this
                # page has no data. (MarianaTek returns 403 for out-of-range pages.)
                if e.status in (403, 404):
                    logger.info(f"[{tag}] entity={entity_id} page={page}: HTTP {e.status} — past last page, skipping")
                    return [], None, page, None
                logger.error(f"[{tag}] entity={entity_id} page={page} failed: {e}")
                failed_pages.append({
                    "entity_id": entity_id, "page": page,
                    "error": str(e), "timestamp": time.time(), "extra": {},
                })
                return [], None, page, e
            except Exception as e:
                logger.error(f"[{tag}] entity={entity_id} page={page} failed: {e}")
                failed_pages.append({
                    "entity_id": entity_id, "page": page,
                    "error": str(e), "timestamp": time.time(), "extra": {},
                })
                return [], None, page, e

    if concurrent_pages:
        results = await asyncio.gather(*[fetch_page(p) for p in concurrent_pages])
        for data, resp, _, _ in results:
            batch_data.extend(data)
            extract_side(resp)
            if len(batch_data) >= parquet_batch_size:
                await flush()
            if len(side_batch) >= parquet_batch_size:
                await flush_side()

    await flush()
    await flush_side()

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
    side_note = f", side_rows={side_total}" if side_map_fn else ""
    logger.info(
        f"[{tag}] DONE entity={entity_id}: processed={total_processed}, "
        f"expected={total_records or 'unknown'}, elapsed={elapsed:.2f}s, pages={page_start}-{page_end}{side_note}"
    )

    if final_failed_count > 0:
        raise Exception(f"{resource} processing failed: {final_failed_count} pages permanently failed after retry")
    if total_records > 0 and total_records != total_processed:
        logger.warning(f"[{tag}] MISSING: expected={total_records}, processed={total_processed}")

    return total_processed, total_records
