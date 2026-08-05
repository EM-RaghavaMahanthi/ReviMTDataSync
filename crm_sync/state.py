"""
CRM sync shard planner — probes page 1 of each resource to compute total_pages,
then splits the page range into fixed-size shards for the Step Functions fan-out.

Each shard dict is a self-contained Lambda payload consumed by handlers/crm_to_s3.py:
  location_shard    {mode, resource, page_start, page_end, account_id, location_id, api_base_url}
  user_shard        {mode, resource, page_start, page_end, account_id, location_id, api_base_url}
  user_batch_shard  {mode, resource, user_offset, user_limit, account_id, location_id, api_base_url}
  id_batch_shard    {mode, resource, id_offset, id_limit, account_id, location_id, api_base_url}
  tenant_shard      {mode, resource,                       account_id, location_id, api_base_url}

The probe is a raw api_get (no field mapping) — we only need meta.pagination / links.last.
"""

import os
import asyncio
import logging
from urllib.parse import parse_qs, urlparse

from core.config import settings
from utils.api_client import api_get
from crm_sync.config import (
    RESOURCE_CONFIG, LOCATION_RESOURCES, USER_RESOURCES, NOTES_RESOURCES, TENANT_RESOURCES,
)

logger = logging.getLogger(__name__)

_PAGES_PER_SHARD = int(os.environ.get("PAGES_PER_SHARD", getattr(settings, "PAGES_PER_SHARD", 200)))
_USER_BATCHES_PER_SHARD = int(os.environ.get("USER_BATCHES_PER_SHARD", getattr(settings, "USER_BATCHES_PER_SHARD", 200)))
# Ids per shard, derived rather than configured: the same 200 requests per Lambda the page
# and user shards already use, times the ids each request carries. One knob fewer, and it
# tracks automatically if the request budget changes.
_REQUESTS_PER_SHARD = _PAGES_PER_SHARD


def _actual_last_page(resp: dict, reported: int) -> int:
    """
    Parse the true last page from links.last. MarianaTek's links.last reflects
    actual data; meta.pagination.pages can be over-reported. Falls back to reported.
    """
    last_url = (resp.get("links") or {}).get("last")
    if last_url:
        try:
            page_vals = parse_qs(urlparse(last_url).query).get("page", [])
            if page_vals:
                return int(page_vals[0])
        except (ValueError, TypeError):
            pass
    return reported


async def _probe_total_pages(resource: str, location_id: int, api_base_url: str) -> int:
    """Fetch page 1 and return the actual last page. probe_param=None → no scoping filter."""
    cfg = RESOURCE_CONFIG[resource]
    params = {"page": 1, "page_size": getattr(settings, "PAGE_SIZE", 100)}
    if cfg.get("probe_param"):
        params[cfg["probe_param"]] = location_id
    try:
        resp = await api_get(cfg["endpoint"], api_base_url, params)
        reported = int(resp.get("meta", {}).get("pagination", {}).get("pages", 1))
        actual = _actual_last_page(resp, reported)
        if actual != reported:
            logger.info(f"[plan] {resource}: reported={reported} pages, actual={actual} (links.last)")
        else:
            logger.info(f"[plan] {resource}: {actual} total pages")
        return actual
    except Exception as e:
        logger.error(f"[plan] {resource} probe failed: {e} — defaulting to 1 page")
        return 1


def _page_shards(mode: str, resource: str, total_pages: int, base: dict, pages_per_shard: int) -> list:
    shards = []
    for start in range(1, total_pages + 1, pages_per_shard):
        end = min(start + pages_per_shard - 1, total_pages)
        shards.append({**base, "mode": mode, "resource": resource, "page_start": start, "page_end": end})
    return shards


async def plan_location_shards(
    account_id, location_id, api_base_url, tables: list = None, pages_per_shard: int = None,
) -> list:
    """Probe each location resource and split [1..total_pages] into page-range shards."""
    if pages_per_shard is None:
        pages_per_shard = _PAGES_PER_SHARD
    loc_tables = [t for t in LOCATION_RESOURCES if (tables is None or t in tables)]
    base = {"account_id": account_id, "location_id": location_id, "api_base_url": api_base_url}

    probes = await asyncio.gather(*[_probe_total_pages(r, location_id, api_base_url) for r in loc_tables])

    shards = []
    for resource, total_pages in zip(loc_tables, probes):
        shards += _page_shards("location_shard", resource, total_pages, base, pages_per_shard)

    logger.info(f"[plan] {len(shards)} location shards across {len(loc_tables)} resources")
    return shards


async def plan_user_shards(
    account_id, location_id, api_base_url,
    pages_per_shard: int = None, batches_per_shard: int = None, tables: list = None,
) -> list:
    """
    "user" resources (membership_instances) → fixed page-range shards (whole tenant
    downloaded unfiltered, filtered client-side later).
    "user_batch" resources (credit_transactions, membership_transactions) → sharded by
    100-user batches: the number of batches == the customers page count (100 users/page
    == 100 users/batch), which we probe here. Each shard covers `batches_per_shard`
    consecutive batches (a user-id offset/limit slice resolved at run time). Since each
    batch is usually ~1 page, this ≈ pages_per_shard pages/shard, overshooting when a
    batch's users span multiple pages.
    """
    if pages_per_shard is None:
        pages_per_shard = _PAGES_PER_SHARD
    if batches_per_shard is None:
        batches_per_shard = _USER_BATCHES_PER_SHARD
    base = {"account_id": account_id, "location_id": location_id, "api_base_url": api_base_url}

    def _want(r):
        return tables is None or r in tables

    batch_resources = [r for r in USER_RESOURCES if RESOURCE_CONFIG[r]["fetch_type"] == "user_batch" and _want(r)]
    # "user" (unfiltered, page-range) resources: membership_instances + user_notes.
    paged_resources = (
        [r for r in USER_RESOURCES if RESOURCE_CONFIG[r]["fetch_type"] == "user" and _want(r)]
        + [r for r in NOTES_RESOURCES if _want(r)]
    )

    shards = []

    # user_batch: probe customers to get the batch count (= customers page count), then
    # split the customer space into user-offset/limit shards of `batches_per_shard` batches.
    if batch_resources:
        total_batches = await _probe_total_pages("customers", location_id, api_base_url)
        users_per_shard = batches_per_shard * 100
        total_users = total_batches * 100  # ceil(count/100)*100 — >= actual customer count
        for resource in batch_resources:
            for offset in range(0, max(total_users, 1), users_per_shard):
                shards.append({
                    **base, "mode": "user_batch_shard", "resource": resource,
                    "user_offset": offset, "user_limit": users_per_shard,
                })

    # Page-range shards for unfiltered "user" resources (membership_instances, user_notes).
    probes = await asyncio.gather(*[_probe_total_pages(r, location_id, api_base_url) for r in paged_resources])
    for resource, total_pages in zip(paged_resources, probes):
        shards += _page_shards("user_shard", resource, total_pages, base, pages_per_shard)

    # Tenant-wide lookups (user_tags) — one tiny shard each, no page range.
    for resource in TENANT_RESOURCES:
        if _want(resource):
            shards.append({**base, "mode": "tenant_shard", "resource": resource})

    logger.info(f"[plan] {len(shards)} user/notes/tenant shards")
    return shards


async def plan_transaction_shards(
    account_id, location_id, api_base_url, tables: list = None,
) -> list:
    """
    Phase 2 of planning — shards for the "id_batch" resources.

    Must run AFTER Stage1_LocationMap: the ids come from order_lines and reservations
    parquet, which the location phase writes. That ordering already exists in the state
    machine, which runs LocationMap to completion before UserMap.

    For each id_batch resource: union its id_sources, dedupe, drop NULLs, write the list to
    S3, and emit offset/limit shards over it. The ids themselves never enter the shard
    payload — 20k ids is ~156KB against a 256KB Step Functions limit, and 200k would fail
    outright. See utils.s3_writer.write_id_list_to_s3.
    """
    from utils.s3_writer import read_distinct_ids_from_s3, write_id_list_to_s3

    base = {"account_id": account_id, "location_id": location_id, "api_base_url": api_base_url}
    resources = [
        r for r, c in RESOURCE_CONFIG.items()
        if c.get("fetch_type") == "id_batch" and (tables is None or r in tables)
    ]

    shards = []
    for resource in resources:
        conf = RESOURCE_CONFIG[resource]
        batch_size = conf.get("batch_size") or int(getattr(settings, "MAX_IDS", 200))
        ids_per_shard = _REQUESTS_PER_SHARD * batch_size

        sources = [
            (settings.S3_PREFIXES[src_resource], column)
            for src_resource, column in conf["id_sources"]
        ]
        ids = await read_distinct_ids_from_s3(account_id, sources)

        if not ids:
            logger.info(f"[plan] {resource}: no referenced ids — no shards")
            continue

        await write_id_list_to_s3(account_id, location_id, resource, ids)

        for offset in range(0, len(ids), ids_per_shard):
            shards.append({
                **base, "mode": "id_batch_shard", "resource": resource,
                "id_offset": offset, "id_limit": ids_per_shard,
            })

        logger.info(
            f"[plan] {resource}: {len(ids)} ids -> "
            f"{-(-len(ids) // ids_per_shard)} shards "
            f"({-(-len(ids) // batch_size)} requests total)"
        )

    logger.info(f"[plan] {len(shards)} transaction shards across {len(resources)} resources")
    return shards
