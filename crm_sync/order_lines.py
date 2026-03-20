import logging
from datetime import datetime, timezone

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import location_sync

logger = logging.getLogger(__name__)
_RESOURCE = "order_lines"


def _parse_transaction(transaction_data):
    """Extract transaction_type, credit_id, membership_id from transaction_data list."""
    if not transaction_data:
        return None, None, None
    t = transaction_data[0]
    t_type = t.get("transaction_type")
    t_id = t.get("transaction_id")
    credit_id = t_id if t_type == "CreditTransaction" else None
    membership_id = t_id if t_type == "MembershipTransaction" else None
    return t_type, credit_id, membership_id


async def fetch_page(location_id: str, page: int, account_id: str, api_base_url: str):
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"location": location_id, "page": page, "page_size": page_size}
    resp = await api_get("/order_lines", api_base_url, params)

    from schemas.revi_schema import OrderLines
    from pydantic_core import ValidationError
    crm_downloaded_at = datetime.now(timezone.utc)
    valid = []

    for u in resp.get("data", []):
        attrs = u.get("attributes", {})
        rels = u.get("relationships", {})

        options = attrs.get("options", [])
        processed_by = not (len(options) < 1 or any(opt.get("value") == "bill_on_purchase" for opt in options))

        child_orders = [
            child["id"] for child in rels.get("child_orders", {}).get("data", [])
            if child.get("type") == "orders" and child.get("id")
        ]

        transaction_type, credit_id, membership_id = _parse_transaction(attrs.get("transaction_data"))

        raw = {
            "order_line_id": u["id"],
            "order_id": rels.get("order", {}).get("data", {}).get("id"),
            "transaction_type": transaction_type,
            "location": str(location_id),
            "title": attrs.get("title"),
            "created_at": None,
            "created_by": None,
            "updated_at": crm_downloaded_at,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "credit_transactions_id": credit_id,
            "membership_transactions_id": membership_id,
            "processed_by": processed_by,
            "account_id": account_id,
            "order_ref_id": None,
            "credit_transactions_ref_id": None,
            "membership_transactions_ref_id": None,
            "child_orders": child_orders,
            "is_valid": True,
        }
        try:
            valid.append(OrderLines(**raw).model_dump())
        except ValidationError as ve:
            logger.warning(f"[order_lines] Skipping id={u.get('id')} location={location_id}: {ve.errors()}")
        except Exception as e:
            logger.error(f"[order_lines] Unexpected error id={u.get('id')} location={location_id}: {e}")

    logger.info(f"[order_lines] location={location_id} page={page}: {len(valid)} valid records")
    return valid, resp


async def process_for_location(location_id: str, account_id: str, api_base_url: str) -> tuple[int, int]:
    return await location_sync.run(
        location_id, account_id, api_base_url,
        fetch_page_fn=fetch_page,
        s3_prefix=settings.S3_PREFIXES["order_lines"],
        resource=_RESOURCE,
    )


process_order_lines_for_location = process_for_location
