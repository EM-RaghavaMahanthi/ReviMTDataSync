"""
Generic Dead Letter Queue (DLQ) helpers for failed page retries.

DLQ entry format:
  {
    "entity_id": str,       # location_id or user_id
    "page": int | "all",    # page number, or "all" for complete entity failure
    "error": str,
    "timestamp": float,
    "extra": dict           # extra kwargs passed to fetch_fn (e.g. {"location_id": 123})
  }

fetch_fn signature expected by retry_failed:
  async (entity_id, page, account_id, api_base_url, **extra) -> (data, resp)
"""

import os
import json
import time
import asyncio
import logging

from core.config import settings

logger = logging.getLogger(__name__)

# /tmp is the only writable path in Lambda's filesystem (everything else, including the
# deployment package's cwd, is read-only) — a relative "data" dir here would crash every
# DLQ retry with "Read-only file system".
_DLQ_DIR = "/tmp/dlq"


async def write_failed(entries: list, resource: str, entity_id: str, account_id: str) -> str | None:
    """Write failed page entries to a local DLQ JSON file. Returns file path or None."""
    if not entries:
        return None
    os.makedirs(_DLQ_DIR, exist_ok=True)
    path = f"{_DLQ_DIR}/failed_{resource}_{entity_id}_{account_id}.json"
    with open(path, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
    logger.warning(f"[DLQ] {resource}: {len(entries)} failures written → {path}")
    return path


async def retry_failed(
    dlq_file: str,
    fetch_fn,
    account_id: str,
    api_base_url: str,
    concurrency_limit: int = None,
) -> tuple[list, int]:
    """
    Retry all entries in dlq_file. Skips entries with page="all" (complete entity failures).
    Returns (recovered_data, remaining_failure_count).
    """
    if not dlq_file or not os.path.exists(dlq_file):
        return [], 0

    entries = []
    with open(dlq_file) as f:
        for line in f:
            entries.append(json.loads(line.strip()))

    if not entries:
        return [], 0

    if concurrency_limit is None:
        concurrency_limit = settings.CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def retry_one(entry):
        if entry.get("page") == "all":
            logger.error(f"[DLQ RETRY] entity={entry['entity_id']}: complete failure — skipping (manual intervention needed)")
            return [], entry

        async with semaphore:
            entity_id = entry["entity_id"]
            page = entry["page"]
            extra = entry.get("extra", {})
            try:
                data, _ = await fetch_fn(entity_id, page, account_id, api_base_url, **extra)
                logger.info(f"[DLQ RETRY] entity={entity_id} page={page}: recovered {len(data)} records")
                return data, None
            except Exception as e:
                logger.error(f"[DLQ RETRY] entity={entity_id} page={page}: failed again: {e}")
                return [], {**entry, "error": str(e), "timestamp": time.time()}

    results = await asyncio.gather(*[retry_one(e) for e in entries])

    recovered = []
    new_failures = []
    for data, failure in results:
        recovered.extend(data)
        if failure:
            new_failures.append(failure)

    if new_failures:
        with open(dlq_file, "w") as f:
            for e in new_failures:
                f.write(json.dumps(e) + "\n")
        logger.error(f"[DLQ] {len(new_failures)} entries still failing after retry → {dlq_file}")
    else:
        os.remove(dlq_file)
        logger.info(f"[DLQ] All entries recovered, {dlq_file} removed")

    return recovered, len(new_failures)
