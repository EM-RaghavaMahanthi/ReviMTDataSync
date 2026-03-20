import logging
from datetime import datetime, timezone
from pydantic import ValidationError

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import extract_rel_id, extract_rel_type
from crm_sync._base import user_sync

logger = logging.getLogger(__name__)
_RESOURCE = "credit_transactions"


async def fetch_page(
    user_id: str, page: int, account_id: str, api_base_url: str, location_id: int = None
):
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"user": user_id, "page": page, "page_size": page_size}
    resp = await api_get("/credit_transactions", api_base_url, params)

    from schemas.revi_schema import CreditTransactionOrder
    crm_downloaded_at = datetime.now(timezone.utc)
    valid = []

    for u in resp.get("data", []):
        attrs = u.get("attributes", {})
        rels = u.get("relationships", {})

        raw = {
            "id": None,
            "credit_transactions_id": u.get("id"),
            "transaction_date": attrs.get("transaction_datetime"),
            "credit_name": attrs.get("credit_name"),
            "is_expired": attrs.get("is_expired"),
            "remaining_credits_cache": attrs.get("remaining_credits_cache"),
            "is_intro_offer": attrs.get("is_intro_offer"),
            "parent_credit_transaction_type": extract_rel_type(rels.get("parent_credit_transaction")),
            "parent_credit_transaction_id": extract_rel_id(rels.get("parent_credit_transaction")),
            "customer_id": extract_rel_id(rels.get("user")),
            "location": location_id,
            "created_at": None,
            "created_by": None,
            "updated_at": crm_downloaded_at,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
            "customer_ref_id": None,
            "account_id": account_id,
        }
        try:
            valid.append(CreditTransactionOrder(**raw).model_dump())
        except ValidationError as ve:
            logger.warning(f"[credit_transactions] Skipping id={u.get('id')}: {ve.errors()}")
        except Exception as e:
            logger.error(f"[credit_transactions] Unexpected error id={u.get('id')}: {e}")

    logger.info(f"[credit_transactions] user={user_id} page={page}: {len(valid)} valid records")
    return valid, resp


async def process_batch(
    user_ids: list, batch_id: int, account_id: str, api_base_url: str, location_id: int
) -> tuple[int, int]:
    return await user_sync.run_batch(
        user_ids, batch_id, account_id, api_base_url, location_id,
        fetch_page_fn=fetch_page,
        s3_prefix=settings.S3_PREFIXES.get("credit_transactions", "credit-transactions-details"),
        resource=_RESOURCE,
    )


# Alias for backward compatibility with handler
process_credit_transactions_batch = process_batch
