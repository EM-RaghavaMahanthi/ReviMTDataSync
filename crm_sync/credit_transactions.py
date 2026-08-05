import logging
from datetime import datetime, timezone
from pydantic import ValidationError

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import extract_rel_id, extract_rel_type
from crm_sync._base import user_sync, user_sync_unfiltered
from crm_sync._base import id_sync

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


async def fetch_page_unfiltered(location_id, page: int, account_id: str, api_base_url: str):
    """
    Unfiltered fetch — one page of the whole tenant's credit_transactions, scoped by
    location only. Each mapped row carries a temporary "_uid" (CRM user id) so the
    orchestrator can filter to this tenant's customers before writing.
    """
    page_size = getattr(settings, "PAGE_SIZE", 100)
    params = {"location": location_id, "page": page, "page_size": page_size}
    resp = await api_get("/credit_transactions", api_base_url, params)

    crm_downloaded_at = datetime.now(timezone.utc)
    rows = []
    for u in resp.get("data", []):
        rels = u.get("relationships", {})
        try:
            row = _map_record(u, location_id, account_id, crm_downloaded_at)
        except ValidationError as ve:
            logger.warning(f"[credit_transactions] Skipping id={u.get('id')}: {ve.errors()}")
            continue
        except Exception as e:
            logger.error(f"[credit_transactions] Unexpected error id={u.get('id')}: {e}")
            continue
        uid = extract_rel_id(rels.get("user"))
        row["_uid"] = str(uid) if uid is not None else None
        rows.append(row)

    logger.info(f"[credit_transactions] location={location_id} page={page}: {len(rows)} rows")
    return rows, resp


async def fetch_by_ids(ids: list, account_id: str, api_base_url: str, location_id: int):
    """
    One request for up to ~100 ids via filter[id] — a single comma-joined parameter, not
    repeated params. page_size is raised to cover the batch so a chunk always comes back in
    one page: unique ids return at most one record each, so len(ids) is the ceiling.
    """
    params = {
        "filter[id]": ",".join(str(i) for i in ids),
        "page_size": max(len(ids), getattr(settings, "PAGE_SIZE", 100)),
    }
    resp = await api_get("/credit_transactions", api_base_url, params)
    valid = _map_data(resp, location_id, account_id)
    logger.info(f"[credit_transactions] filter[id] x{len(ids)}: {len(valid)} valid records")
    return valid, resp


async def process_id_shard(
    ids: list, account_id: str, location_id, api_base_url: str, shard_tag: str = None,
) -> tuple[int, int]:
    """TransactionMap 'id_batch_shard' entry point — fetch exactly this slice of ids."""
    return await id_sync.run(
        resource=_RESOURCE,
        fetch_by_ids_fn=fetch_by_ids,
        ids=ids,
        account_id=account_id,
        location_id=location_id,
        api_base_url=api_base_url,
        s3_prefix=settings.S3_PREFIXES["credit_transactions"],
        shard_tag=shard_tag,
    )


async def process_unfiltered_shard(
    account_id: str, location_id, api_base_url: str, customer_ids: list,
    page_start: int = 1, page_end: int = None,
) -> tuple[int, int]:
    """UserMap 'user_shard' entry point — fixed page range, filtered by customer_ids."""
    return await user_sync_unfiltered.run(
        resource=_RESOURCE,
        fetch_unfiltered_page_fn=fetch_page_unfiltered,
        account_id=account_id,
        location_id=location_id,
        api_base_url=api_base_url,
        s3_prefix=settings.S3_PREFIXES["credit_transactions"],
        customer_ids=customer_ids,
        page_start=page_start,
        page_end=page_end,
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
