import logging
from datetime import datetime, timezone

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import extract_rel_id
from crm_sync._base import location_sync

logger = logging.getLogger(__name__)
_RESOURCE = "orders"


def _extract_payment_labels(payment_sources):
    if not payment_sources:
        return None
    return ", ".join(ps.get("label", "") for ps in payment_sources if ps.get("label"))


async def fetch_page(location_id: str, page: int, account_id: str, api_base_url: str):
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"location": location_id, "page": page, "page_size": page_size}
    resp = await api_get("/orders", api_base_url, params)

    from schemas.revi_schema import Order
    crm_downloaded_at = datetime.now(timezone.utc)
    valid = []

    data_list = resp.get("data", [])
    if not isinstance(data_list, list):
        raise ValueError(f"API 'data' is not a list for location={location_id} page={page}")

    for u in data_list:
        attrs = u.get("attributes", {})
        rels = u.get("relationships", {})

        def rel_id(name):
            return extract_rel_id(rels.get(name, {}))

        raw = {
            "order_id": u.get("id"),
            "date_placed": attrs.get("date_placed"),
            "location": attrs.get("location"),
            "location_id": location_id,
            "payment_sources_labels": _extract_payment_labels(attrs.get("payment_sources")),
            "status": attrs.get("status"),
            "order_lines_id": rel_id("order_lines"),
            "customer_ref_id": None,
            "customer_id": rel_id("user"),
            "parent_order": rel_id("parent_order"),
            "account_id": account_id,
            "created_at": None,
            "created_by": None,
            "updated_at": crm_downloaded_at,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
        }
        try:
            valid.append(Order(**raw).model_dump())
        except Exception as e:
            logger.warning(f"[orders] Skipping order id={u.get('id')} location={location_id}: {e}")

    logger.info(f"[orders] location={location_id} page={page}: {len(valid)} valid records")
    return valid, resp


async def process_for_location(location_id: str, account_id: str, api_base_url: str) -> tuple[int, int]:
    return await location_sync.run(
        location_id, account_id, api_base_url,
        fetch_page_fn=fetch_page,
        s3_prefix=settings.S3_PREFIXES["orders"],
        resource=_RESOURCE,
    )


process_orders_for_location = process_for_location
