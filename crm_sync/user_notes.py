import logging
from datetime import datetime, timezone
from pydantic import ValidationError

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import extract_rel_id
from crm_sync._base import user_sync_unfiltered

logger = logging.getLogger(__name__)
_RESOURCE = "user_notes"


def _map_record(u, location_id, account_id, crm_downloaded_at):
    """Map one /user_notes item to the CustomerNote schema."""
    from schemas.revi_schema import CustomerNote
    attrs = u.get("attributes", {})
    rels = u.get("relationships", {})
    user_id = extract_rel_id(rels.get("user"))
    author_id = extract_rel_id(rels.get("author"))
    raw = {
        "id": None,
        "account_id": account_id,
        "customer_id": str(user_id) if user_id is not None else None,   # the customer the note is about
        "customer_ref_id": None,                                        # resolved in Stage 3
        "note_id": str(u["id"]) if u.get("id") is not None else None,
        "note": attrs.get("text"),
        "note_datetime": attrs.get("note_datetime"),
        "is_pinned": attrs.get("is_pinned"),
        "author_id": str(author_id) if author_id is not None else None,  # MT staff → created_by (Stage 3)
        "location": int(location_id) if location_id is not None else None,
        "created_at": None,
        "created_by": None,
        "updated_at": crm_downloaded_at,
        "updated_by": None,
        "deleted_at": None,
        "deleted_by": None,
    }
    return CustomerNote(**raw).model_dump()


async def fetch_page_unfiltered(location_id, page: int, account_id: str, api_base_url: str):
    """
    Fetch one page of the whole tenant's notes — NO location/user filter (the endpoint
    doesn't filter). Each mapped row carries a temporary "_uid" (the note's customer id)
    so the orchestrator keeps only this tenant's customers before writing.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"page": page, "page_size": page_size}
    resp = await api_get("/user_notes", api_base_url, params)

    crm_downloaded_at = datetime.now(timezone.utc)
    rows = []
    for u in resp.get("data", []):
        try:
            row = _map_record(u, location_id, account_id, crm_downloaded_at)
        except ValidationError as ve:
            logger.warning(f"[user_notes] Skipping id={u.get('id')}: {ve.errors()}")
            continue
        except Exception as e:
            logger.error(f"[user_notes] Unexpected error id={u.get('id')}: {e}")
            continue
        row["_uid"] = row.get("customer_id")
        rows.append(row)

    logger.info(f"[user_notes] page={page}: {len(rows)} rows")
    return rows, resp


async def process_unfiltered_shard(
    account_id: str, location_id, api_base_url: str, customer_ids: list,
    page_start: int = 1, page_end: int = None,
) -> tuple[int, int]:
    """UserMap 'user_shard' entry point — fixed page range, filtered by customer_ids."""
    s3_prefix = settings.S3_PREFIXES.get("user_notes", "mariana-tek/user_notes-details")
    return await user_sync_unfiltered.run(
        resource=_RESOURCE,
        fetch_unfiltered_page_fn=fetch_page_unfiltered,
        account_id=account_id,
        location_id=location_id,
        api_base_url=api_base_url,
        s3_prefix=s3_prefix,
        customer_ids=customer_ids,
        page_start=page_start,
        page_end=page_end,
    )
