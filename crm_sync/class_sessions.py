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
            "class_name": attrs.get("classroom_display"),
            "class_type_name": attrs.get("class_type_display"),
            "capacity": attrs.get("capacity"),
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


async def fetch_by_ids(
    ids: list[str],
    api_base_url: str,
    account_id: str,
    concurrency: int = 10,
) -> tuple[list, list]:
    """
    Fetch class_sessions one at a time by ID: GET /class_sessions/{id}
    Returns (found, totally_disappeared).
    """
    import asyncio

    semaphore = asyncio.Semaphore(concurrency)
    found:       list = []
    disappeared: list = []

    async def _fetch_one(cs_id: str):
        async with semaphore:
            try:
                resp = await api_get(f"/class_sessions/{cs_id}", api_base_url)
                data = resp.get("data")
                if not data:
                    return cs_id, None
                u = data if isinstance(data, dict) else data[0]
                attrs = u.get("attributes", {})
                crm_downloaded_at = datetime.now(timezone.utc)
                from schemas.revi_schema import ClassSessions
                from pydantic import ValidationError as VE
                raw = {
                    "class_session_id":      str(u.get("id")),
                    "start_datetime":        attrs.get("start_datetime"),
                    "start_date":            attrs.get("start_date"),
                    "location":              str(attrs.get("location", "")),
                    "end_datetime":          attrs.get("end_datetime"),
                    "cancellation_datetime": attrs.get("cancellation_datetime"),
                    "class_name":            attrs.get("classroom_display"),
                    "class_type_name":       attrs.get("class_type_display"),
                    "capacity":              attrs.get("capacity"),
                    "created_at":  None,
                    "created_by":  None,
                    "updated_at":  crm_downloaded_at,
                    "updated_by":  None,
                    "deleted_at":  None,
                    "deleted_by":  None,
                    "account_id":  account_id,
                }
                return cs_id, ClassSessions(**raw).model_dump()
            except Exception as e:
                logger.warning(f"[class_sessions.fetch_by_ids] id={cs_id} error: {e}")
                return cs_id, None

    results = await asyncio.gather(*[_fetch_one(cs_id) for cs_id in ids])
    for cs_id, record in results:
        if record:
            found.append(record)
        else:
            disappeared.append(cs_id)

    logger.info(
        f"[class_sessions.fetch_by_ids] requested={len(ids)}, "
        f"found={len(found)}, totally_disappeared={len(disappeared)}"
    )
    return found, disappeared


async def process_for_location(location_id: str, account_id: str, api_base_url: str) -> tuple[int, int]:
    return await location_sync.run(
        str(location_id), account_id, api_base_url,
        fetch_page_fn=fetch_page,
        s3_prefix=settings.S3_PREFIXES.get("class_sessions", "class_sessions-details"),
        resource=_RESOURCE,
    )


process_class_sessions_for_location = process_for_location
