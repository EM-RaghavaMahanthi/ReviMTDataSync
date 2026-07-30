import logging
from datetime import datetime, timezone

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import extract_rel_id, extract_rel_type
from crm_sync._base import location_sync

logger = logging.getLogger(__name__)
_RESOURCE = "reservations"


async def fetch_page(location_id: str, page: int, account_id: str, api_base_url: str):
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"location": location_id, "page": page, "page_size": page_size}
    resp = await api_get("/reservations", api_base_url, params)

    from schemas.revi_schema import Reservation
    crm_downloaded_at = datetime.now(timezone.utc)
    valid = []

    for u in resp.get("data", []):
        attrs = u.get("attributes", {})
        rels = u.get("relationships", {})

        credit_id = extract_rel_id(rels.get("credit_transactions", {}))
        credit_type = extract_rel_type(rels.get("credit_transactions", {}))
        membership_id = extract_rel_id(rels.get("membership_transactions", {}))
        membership_type = extract_rel_type(rels.get("membership_transactions", {}))

        raw = {
            "reservations_id": u.get("id"),
            "cancel_date": attrs.get("cancel_date"),
            "check_in_date": attrs.get("check_in_date"),
            "creation_date": attrs.get("creation_date"),
            "status": attrs.get("status"),
            # A reservation is a guest booking iff MT gave it a guest_email. .strip() so a
            # blank-but-not-empty value (" ") counts as absent, unlike a bare truthiness check.
            "guest": bool((attrs.get("guest_email") or "").strip()),
            "reservation_type": attrs.get("reservation_type"),
            "first_timer": attrs.get("first_timer", False),
            "location": location_id,
            "credit_transactions_id": credit_id,
            "credit_transactions_type": credit_type,
            "membership_transactions_id": membership_id,
            "membership_transactions_type": membership_type,
            "transaction_type": credit_type or membership_type,
            "customer_id": rels.get("user", {}).get("data", {}).get("id"),
            "class_session_id": rels.get("class_session", {}).get("data", {}).get("id"),
            "credit_transactions_ref_id": None,
            "membership_transactions_ref_id": None,
            "customer_ref_id": None,
            "class_session_ref_id": None,
            "created_at": None,
            "updated_at": crm_downloaded_at,
            "created_by": None,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "account_id": account_id,
        }
        try:
            valid.append(Reservation(**raw).dict())
        except Exception as e:
            logger.warning(f"[reservations] Skipping id={u.get('id')} location={location_id}: {e}")

    logger.info(f"[reservations] location={location_id} page={page}: {len(valid)} valid records")
    return valid, resp


async def process_for_entity(
    entity_id: str, account_id: str, api_base_url: str, entity_type: str = "location"
) -> tuple[int, int]:
    return await location_sync.run(
        entity_id, account_id, api_base_url,
        fetch_page_fn=fetch_page,
        s3_prefix=settings.S3_PREFIXES.get("reservations", "reservations-details"),
        resource=_RESOURCE,
        entity_type=entity_type,
        concurrency_limit=64,
    )


process_reservations_for_entity = process_for_entity
