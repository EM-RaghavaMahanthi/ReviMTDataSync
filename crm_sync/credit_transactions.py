import logging
from datetime import datetime, timezone
from pydantic import ValidationError

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import extract_rel_id, extract_rel_type
from crm_sync._base import user_sync

logger = logging.getLogger(__name__)
_RESOURCE = "credit_transactions"


def _map_record(u, location_id, account_id, crm_downloaded_at):
    from schemas.revi_schema import CreditTransactionOrder
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
    return CreditTransactionOrder(**raw).model_dump()


def _map_data(resp, location_id, account_id):
    crm_downloaded_at = datetime.now(timezone.utc)
    valid = []
    for u in resp.get("data", []):
        try:
            valid.append(_map_record(u, location_id, account_id, crm_downloaded_at))
        except ValidationError as ve:
            logger.warning(f"[credit_transactions] Skipping id={u.get('id')}: {ve.errors()}")
        except Exception as e:
            logger.error(f"[credit_transactions] Unexpected error id={u.get('id')}: {e}")
    return valid


async def fetch_page(
    user_id: str, page: int, account_id: str, api_base_url: str, location_id: int = None
):
    """Legacy per-user fetch (kept for backward-compat / direct invoke)."""
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"user": user_id, "page": page, "page_size": page_size}
    resp = await api_get("/credit_transactions", api_base_url, params)
    valid = _map_data(resp, location_id, account_id)
    logger.info(f"[credit_transactions] user={user_id} page={page}: {len(valid)} valid records")
    return valid, resp


async def fetch_page_batched(
    user_ids: list, page: int, account_id: str, api_base_url: str, location_id: int = None
):
    """Batched fetch — up to 100 user ids per call via repeated &user= params."""
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = [("user", str(uid)) for uid in user_ids] + [("page", page), ("page_size", page_size)]
    resp = await api_get("/credit_transactions", api_base_url, params)
    valid = _map_data(resp, location_id, account_id)
    logger.info(f"[credit_transactions] {len(user_ids)} users page={page}: {len(valid)} valid records")
    return valid, resp


async def process_resource_batched(
    all_user_ids: list, account_id: str, api_base_url: str, location_id: int,
    shard_tag: str = None,
) -> tuple[int, int]:
    """UserMap 'user_batch_shard' entry point — batch 100 ids/call."""
    return await user_sync.run_resource_batched(
        resource=_RESOURCE,
        all_user_ids=all_user_ids,
        account_id=account_id,
        api_base_url=api_base_url,
        location_id=location_id,
        fetch_page_fn=fetch_page_batched,
        s3_prefix=settings.S3_PREFIXES["credit_transactions"],
        entity_id=shard_tag,
    )


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
