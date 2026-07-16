import logging
from datetime import datetime, timezone
from pydantic import ValidationError

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import extract_rel_id
from crm_sync._base import user_sync, user_sync_unfiltered

logger = logging.getLogger(__name__)
_RESOURCE = "membership_instances"


def _map_record(u, location_id, account_id, crm_downloaded_at):
    from schemas.revi_schema import MembershipInstance
    attrs = u.get("attributes", {})
    raw = {
        "id": None,
        "membership_instances_id": int(u["id"]) if u.get("id") is not None else None,
        "purchase_date": attrs.get("purchase_date"),
        "membership_name": attrs.get("membership_name"),
        "renewal_rate_incl_tax": attrs.get("renewal_rate_incl_tax"),
        "status": attrs.get("status"),
        "location": int(location_id) if location_id is not None else None,
        "renewal_count": attrs.get("renewal_count"),
        "next_charge_date": attrs.get("next_charge_date"),
        "created_at": None,
        "created_by": None,
        "updated_at": crm_downloaded_at,
        "updated_by": None,
        "deleted_at": None,
        "deleted_by": None,
        "account_id": account_id,
    }
    return MembershipInstance(**raw).model_dump()


async def fetch_page(
    user_id: str, page: int, account_id: str, api_base_url: str, location_id: int = None
):
    """Legacy per-user fetch (kept for backward-compat / direct invoke)."""
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"user": user_id, "page": page, "page_size": page_size}
    resp = await api_get("/membership_instances", api_base_url, params)

    crm_downloaded_at = datetime.now(timezone.utc)
    valid = []
    for u in resp.get("data", []):
        try:
            valid.append(_map_record(u, location_id, account_id, crm_downloaded_at))
        except ValidationError as ve:
            logger.warning(f"[membership_instances] Skipping id={u.get('id')}: {ve.errors()}")
        except Exception as e:
            logger.error(f"[membership_instances] Unexpected error id={u.get('id')}: {e}")

    logger.info(f"[membership_instances] user={user_id} page={page}: {len(valid)} valid records")
    return valid, resp


async def fetch_page_unfiltered(location_id, page: int, account_id: str, api_base_url: str):
    """
    Unfiltered fetch — one page of the whole tenant's membership_instances, scoped by
    location only. Each mapped row carries a temporary "_uid" (CRM user id) so the
    orchestrator can filter to this tenant's customers before writing.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"location": location_id, "page": page, "page_size": page_size}
    resp = await api_get("/membership_instances", api_base_url, params)

    crm_downloaded_at = datetime.now(timezone.utc)
    rows = []
    for u in resp.get("data", []):
        rels = u.get("relationships", {})
        try:
            row = _map_record(u, location_id, account_id, crm_downloaded_at)
        except ValidationError as ve:
            logger.warning(f"[membership_instances] Skipping id={u.get('id')}: {ve.errors()}")
            continue
        except Exception as e:
            logger.error(f"[membership_instances] Unexpected error id={u.get('id')}: {e}")
            continue
        uid = extract_rel_id(rels.get("user"))
        row["_uid"] = str(uid) if uid is not None else None
        rows.append(row)

    logger.info(f"[membership_instances] location={location_id} page={page}: {len(rows)} rows")
    return rows, resp


async def process_unfiltered_shard(
    account_id: str, location_id, api_base_url: str, customer_ids: list,
    page_start: int = 1, page_end: int = None,
) -> tuple[int, int]:
    """UserMap 'user_shard' entry point — fixed page range, filtered by customer_ids."""
    return await user_sync_unfiltered.run(
        resource=_RESOURCE,
        fetch_unfiltered_page_fn=fetch_page_unfiltered,
        account_id=account_id,
        location_id=location_id,
        api_base_url=api_base_url,
        s3_prefix=settings.S3_PREFIXES["membership_instances"],
        customer_ids=customer_ids,
        page_start=page_start,
        page_end=page_end,
    )


async def process_batch(
    user_ids: list, batch_id: int, account_id: str, api_base_url: str, location_id: int
) -> tuple[int, int]:
    return await user_sync.run_batch(
        user_ids, batch_id, account_id, api_base_url, location_id,
        fetch_page_fn=fetch_page,
        s3_prefix=settings.S3_PREFIXES.get("membership_instances", "membership-instances-details"),
        resource=_RESOURCE,
    )


process_membership_instances_batch = process_batch
