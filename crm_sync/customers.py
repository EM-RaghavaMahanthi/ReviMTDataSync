import logging
import asyncio
from datetime import datetime, timezone

from utils.api_client import api_get
from core.config import settings
from crm_sync._base import location_sync

logger = logging.getLogger(__name__)
_RESOURCE = "customers"


async def fetch_page(location_id: str, page: int, account_id: str, api_base_url: str):
    page_size = getattr(settings, "PAGE_SIZE", 500)
    params = {"home_location": location_id, "page": page, "page_size": page_size}
    resp = await api_get("/users", api_base_url, params)

    from schemas.revi_schema import Customer
    crm_downloaded_at = datetime.now(timezone.utc)
    valid = []

    for u in resp.get("data", []):
        attrs = u.get("attributes", {})

        birth_date = attrs.get("birth_date")
        birth_day = birth_month = None
        if birth_date:
            try:
                dt = datetime.fromisoformat(birth_date.replace("Z", "+00:00"))
                birth_day, birth_month = dt.day, dt.month
            except Exception:
                pass

        raw = {
            "customer_id": str(u.get("id")),
            "location_id": str(location_id),
            "account_id": account_id,
            "first_name": attrs.get("first_name"),
            "last_name": attrs.get("last_name"),
            "email": attrs.get("email"),
            "full_name": attrs.get("full_name"),
            "birth_date": birth_date,
            "birth_day": birth_day,
            "birth_month": birth_month,
            "phone_number": attrs.get("phone_number"),
            "address_line1": attrs.get("address_line1"),
            "address_line2": attrs.get("address_line2"),
            "address_line3": attrs.get("address_line3"),
            "city": attrs.get("city"),
            "country": attrs.get("country"),
            "state_province": attrs.get("state_province"),
            "customer_state": attrs.get("customer_state"),
            "postal_code": attrs.get("postal_code"),
            "gender": attrs.get("gender"),
            "date_joined": attrs.get("date_joined"),
            "is_opted_in_to_sms": attrs.get("is_opted_in_to_sms"),
            "completed_class_count": attrs.get("completed_class_count"),
            "state_id": None,
            "created_at": None,
            "created_by": None,
            "updated_at": crm_downloaded_at,
            "updated_by": None,
            "deleted_at": None,
            "deleted_by": None,
        }
        try:
            valid.append(Customer(**raw).model_dump())
        except Exception as e:
            logger.warning(f"[customers] Skipping customer id={u.get('id')}: {e}")

    logger.info(f"[customers] location={location_id} page={page}: {len(valid)} valid records")
    return valid, resp


async def process_for_location(
    location_id: str, account_id: str, api_base_url: str, save_to_s3: bool = True
) -> tuple[int, int, list]:
    """
    Returns (total_processed, total_expected, user_ids).
    user_ids populated only when save_to_s3=False (used to seed user-based syncs downstream).
    """
    if save_to_s3:
        processed, expected = await location_sync.run(
            location_id, account_id, api_base_url,
            fetch_page_fn=fetch_page,
            s3_prefix=settings.S3_PREFIXES["customers"],
            resource=_RESOURCE,
        )
        return processed, expected, []

    # Collect-only mode: fetch all pages, return user_ids without writing to S3
    first_data, first_resp = await fetch_page(location_id, 1, account_id, api_base_url)
    pagination = first_resp.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("pages", 1)
    total_records = pagination.get("count", len(first_data))

    all_data = list(first_data)
    if total_pages > 1:
        semaphore = asyncio.Semaphore(settings.CONCURRENCY_LIMIT)

        async def _fetch(page):
            async with semaphore:
                data, _ = await fetch_page(location_id, page, account_id, api_base_url)
                return data

        results = await asyncio.gather(*[_fetch(p) for p in range(2, total_pages + 1)])
        for data in results:
            all_data.extend(data)

    user_ids = [c["customer_id"] for c in all_data]
    return len(all_data), total_records, user_ids


# Alias for backward compatibility with handler
process_customers_for_location = process_for_location
