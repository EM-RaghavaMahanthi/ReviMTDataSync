import logging
from datetime import datetime, timezone
from pydantic import ValidationError

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import location_sync

logger = logging.getLogger(__name__)
_RESOURCE = "user_tags"


def _map_record(u, account_id, location_id):
    """Map one /user_tags item to the UserTag schema (keep all attributes)."""
    from schemas.revi_schema import UserTag
    attrs = u.get("attributes", {})
    raw = {
        "id": None,
        "tag_id": str(u["id"]) if u.get("id") is not None else None,
        "account_id": account_id,
        "location": int(location_id) if location_id is not None else None,
        "name": attrs.get("name"),
        "slug": attrs.get("slug"),
        "user_tag_type": attrs.get("user_tag_type"),
        "description": attrs.get("description"),
        "tag_type": attrs.get("tag_type"),          # "manual" => custom, "system" => default
        "weight": attrs.get("weight"),
        "created_at": None,
        "created_by": None,
        "updated_at": datetime.now(timezone.utc),
        "updated_by": None,
        "deleted_at": None,
        "deleted_by": None,
    }
    return UserTag(**raw).model_dump()


async def fetch_page(entity_id, page: int, account_id: str, api_base_url: str):
    """
    Tenant-wide fetch — no location/user filter, keep every tag. entity_id (the
    location) is used only for the row's `location` column and S3 key, not the query.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"page": page, "page_size": page_size}
    resp = await api_get("/user_tags", api_base_url, params)

    valid = []
    for u in resp.get("data", []):
        try:
            valid.append(_map_record(u, account_id, entity_id))
        except ValidationError as ve:
            logger.warning(f"[user_tags] Skipping id={u.get('id')}: {ve.errors()}")
        except Exception as e:
            logger.error(f"[user_tags] Unexpected error id={u.get('id')}: {e}")

    logger.info(f"[user_tags] page={page}: {len(valid)} valid tags")
    return valid, resp


async def process_tenant(account_id: str, location_id, api_base_url: str) -> tuple[int, int]:
    """
    'tenant_shard' entry point — one shard, non-sharded pagination (probe page 1,
    fetch all remaining). The table is tiny (a few dozen tags), so a single
    invocation fetches everything.
    """
    s3_prefix = settings.S3_PREFIXES.get("user_tags", "mariana-tek/user_tags-details")
    return await location_sync.run(
        str(location_id), account_id, api_base_url,
        fetch_page_fn=fetch_page,
        s3_prefix=s3_prefix,
        resource=_RESOURCE,
        entity_type=_RESOURCE,
    )
