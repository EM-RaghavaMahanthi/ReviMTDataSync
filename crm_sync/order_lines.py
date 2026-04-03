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

    valid = []
    for u in resp.get("data", []):
        record = _parse_record(u, account_id, location_override=str(location_id))
        if record:
            valid.append(record)

    logger.info(f"[order_lines] location={location_id} page={page}: {len(valid)} valid records")
    return valid, resp


def _parse_record(u: dict, account_id: str, location_override: str = None):
    """Parse a single CRM order_line item into a validated dict. Returns None on failure."""
    from schemas.revi_schema import OrderLines
    from pydantic_core import ValidationError

    attrs = u.get("attributes", {})
    rels  = u.get("relationships", {})

    options = attrs.get("options", [])
    processed_by = not (len(options) < 1 or any(opt.get("value") == "bill_on_purchase" for opt in options))

    child_orders = [
        child["id"] for child in rels.get("child_orders", {}).get("data", [])
        if child.get("type") == "orders" and child.get("id")
    ]

    transaction_type, credit_id, membership_id = _parse_transaction(attrs.get("transaction_data"))

    # location comes from the record's own relationship when fetching by ID
    location = location_override or str(
        rels.get("location", {}).get("data", {}).get("id", "")
    )

    raw = {
        "order_line_id":                u["id"],
        "order_id":                     rels.get("order", {}).get("data", {}).get("id"),
        "transaction_type":             transaction_type,
        "location":                     location,
        "title":                        attrs.get("title"),
        "line_total":                   attrs.get("line_total"),
        "created_at":                   None,
        "created_by":                   None,
        "updated_at":                   datetime.now(timezone.utc),
        "updated_by":                   None,
        "deleted_at":                   None,
        "deleted_by":                   None,
        "credit_transactions_id":       credit_id,
        "membership_transactions_id":   membership_id,
        "processed_by":                 processed_by,
        "account_id":                   account_id,
        "order_ref_id":                 None,
        "credit_transactions_ref_id":   None,
        "membership_transactions_ref_id": None,
        "child_orders":                 child_orders,
        "is_valid":                     True,
    }
    try:
        return OrderLines(**raw).model_dump()
    except ValidationError as ve:
        logger.warning(f"[order_lines] Skipping id={u.get('id')}: {ve.errors()}")
        return None
    except Exception as e:
        logger.error(f"[order_lines] Unexpected error id={u.get('id')}: {e}")
        return None


async def fetch_by_ids(
    ids: list[str],
    api_base_url: str,
    account_id: str,
    concurrency: int = 10,
) -> tuple[list, list]:
    """
    Fetch order_lines one at a time by ID: GET /order_lines/{id}
    The CRM API does not support batch ID queries — each record needs its own request.
    Fires requests concurrently (bounded by semaphore).

    Returns:
        (moved_location, totally_disappeared)
        moved_location       — found by ID (record moved to another location)
        totally_disappeared  — 404 / empty response (gone from CRM entirely)
    """
    import asyncio

    semaphore      = asyncio.Semaphore(concurrency)
    moved_location: list = []
    disappeared:    list = []

    async def _fetch_one(order_line_id: str):
        async with semaphore:
            try:
                resp = await api_get(f"/order_lines/{order_line_id}", api_base_url)
                data = resp.get("data")
                if not data:
                    return order_line_id, None
                # Single-resource endpoint returns an object, not a list
                u = data if isinstance(data, dict) else data[0]
                record = _parse_record(u, account_id)
                return order_line_id, record
            except Exception as e:
                logger.warning(f"[order_lines.fetch_by_ids] id={order_line_id} error: {e}")
                return order_line_id, None

    results = await asyncio.gather(*[_fetch_one(oid) for oid in ids])

    for oid, record in results:
        if record:
            moved_location.append(record)
        else:
            disappeared.append(oid)

    logger.info(
        f"[order_lines.fetch_by_ids] requested={len(ids)}, "
        f"moved_location={len(moved_location)}, "
        f"totally_disappeared={len(disappeared)}"
    )
    return moved_location, disappeared


async def process_for_location(location_id: str, account_id: str, api_base_url: str) -> tuple[int, int]:
    return await location_sync.run(
        location_id, account_id, api_base_url,
        fetch_page_fn=fetch_page,
        s3_prefix=settings.S3_PREFIXES["order_lines"],
        resource=_RESOURCE,
    )


process_order_lines_for_location = process_for_location
