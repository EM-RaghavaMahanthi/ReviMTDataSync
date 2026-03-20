import logging
from datetime import datetime, timezone
from pydantic import ValidationError

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import location_sync

logger = logging.getLogger(__name__)
_RESOURCE = "class_sessions"


async def fetch_page(location_id: str, page: int, account_id: str, api_base_url: str):
    location_id = str(location_id)
    page_size = settings.PAGE_SIZE or 100
    params = {"location": location_id, "page": page, "page_size": page_size}
    resp = await api_get("/class_sessions", api_base_url, params)

    from schemas.revi_schema import ClassSessions
    crm_downloaded_at = datetime.now(timezone.utc)
    valid = []

    for u in resp.get("data", []):
        attrs = u.get("attributes", {})
        raw = {
            "class_session_id": str(u.get("id")),
            "start_datetime": attrs.get("start_datetime"),
            "start_date": attrs.get("start_date"),
            "location": str(location_id),
            "end_datetime": attrs.get("end_datetime"),
            "cancellation_datetime": attrs.get("cancellation_datetime"),
            "created_at": None,
            "created_by": None,
            "updated_at": crm_downloaded_at,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "account_id": account_id,
        }
        try:
            valid.append(ClassSessions(**raw).model_dump())
        except ValidationError as ve:
            logger.warning(f"[class_sessions] Skipping id={u.get('id')} location={location_id}: {ve.errors()}")
        except Exception as e:
            logger.error(f"[class_sessions] Unexpected error id={u.get('id')} location={location_id}: {e}")

    logger.info(f"[class_sessions] location={location_id} page={page}: {len(valid)} valid records")
    return valid, resp


async def process_for_location(location_id: str, account_id: str, api_base_url: str) -> tuple[int, int]:
    return await location_sync.run(
        str(location_id), account_id, api_base_url,
        fetch_page_fn=fetch_page,
        s3_prefix=settings.S3_PREFIXES.get("class_sessions", "class_sessions-details"),
        resource=_RESOURCE,
    )


process_class_sessions_for_location = process_for_location
