"""
event_type = "refresh_recent_notes" — pulls user_notes from MarianaTek within a recent time
window (MT's own min_note_datetime/max_note_datetime filter params), upserts them into
mt_user_notes_details_dlk (insert new note_ids, update existing ones whose content changed),
then reflects that into customer_notes: new notes get inserted (via the existing
process_customer_notes), and existing customer_notes rows whose note text changed get updated
(process_customer_notes alone is insert-only and would otherwise never see an edit).

Unlike backfill_notes_and_tags, this job calls the live MarianaTek API directly - it needs
api_base_url, resolved here from accounts.crm_api_end_point. Reuses utils.api_client.api_get
(per-tenant token bucket + tenacity retry, same as Stage 1) and crm_sync.user_notes's row
mapper rather than reimplementing either. That pulls in core.config, which only requires
DATABASE_URL to be set (S3_BUCKET now defaults to "" - this job never touches S3).

Window length: NOTES_WINDOW_HOURS env var, default 24 (temporary - drop back down after the
first run is evaluated). End is always "now" (UTC) at invocation time -
min_note_datetime = now - NOTES_WINDOW_HOURS, max_note_datetime = now.

Event payload:
  { "event_type": "refresh_recent_notes", "account_id": 4809 }
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import text

from utils.api_client import api_get
from crm_sync.user_notes import _map_record
from stg_db_services.customer_notes import process_customer_notes, update_changed_notes

logger = logging.getLogger(__name__)

_DEFAULT_WINDOW_HOURS = 24  # TODO: drop back down after the first run is evaluated


def _get_api_base_url(account_id: str, engine) -> str:
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT crm_api_end_point FROM accounts WHERE id = :account_id
        """), {"account_id": account_id}).fetchone()
    if not row or not row[0]:
        raise ValueError(f"No crm_api_end_point found for account_id={account_id}")
    return row[0]


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


async def _fetch_recent_notes(api_base_url: str, min_dt: datetime, max_dt: datetime) -> list:
    """Paginate /user_notes filtered to [min_dt, max_dt]. Window is small by design, but
    loop on links.next in case a busy tenant fills more than one page."""
    all_data = []
    page = 1
    while True:
        params = {
            "min_note_datetime": _fmt(min_dt),
            "max_note_datetime": _fmt(max_dt),
            "page": page,
        }
        resp = await api_get("/user_notes", api_base_url, params)
        data = resp.get("data", [])
        all_data.extend(data)
        if not data or not (resp.get("links") or {}).get("next"):
            break
        page += 1
    return all_data


async def _upsert_staging(rows: list, account_id: str, engine) -> dict:
    """Insert new note_ids, update existing ones - keyed on note_id alone."""
    inserted = 0
    updated = 0
    with engine.begin() as conn:
        for row in rows:
            params = {**row, "account_id": account_id}
            result = conn.execute(text("""
                UPDATE mt_user_notes_details_dlk
                SET customer_id = :customer_id, note = :note, note_datetime = :note_datetime,
                    is_pinned = :is_pinned, author_id = :author_id, updated_at = :updated_at
                WHERE account_id = :account_id AND note_id = :note_id
            """), params)
            if result.rowcount:
                updated += 1
            else:
                conn.execute(text("""
                    INSERT INTO mt_user_notes_details_dlk (
                      account_id, customer_id, customer_ref_id, note_id, note, note_datetime,
                      is_pinned, author_id, location, created_at, updated_at
                    ) VALUES (
                      :account_id, :customer_id, NULL, :note_id, :note, :note_datetime,
                      :is_pinned, :author_id, :location, NOW(), :updated_at
                    )
                """), params)
                inserted += 1
    return {"staging_inserted": inserted, "staging_updated": updated}


async def run(account_id: str, engine) -> dict:
    window_hours = int(os.environ.get("NOTES_WINDOW_HOURS", _DEFAULT_WINDOW_HOURS))
    max_dt = datetime.now(timezone.utc)
    min_dt = max_dt - timedelta(hours=window_hours)

    api_base_url = _get_api_base_url(account_id, engine)
    logger.info(
        f"[refresh_recent_notes] account_id={account_id}: window={min_dt.isoformat()}..{max_dt.isoformat()}"
    )

    raw_notes = await _fetch_recent_notes(api_base_url, min_dt, max_dt)
    logger.info(f"[refresh_recent_notes] account_id={account_id}: {len(raw_notes)} notes in window")

    mapped = []
    for u in raw_notes:
        try:
            mapped.append(_map_record(u, None, account_id, max_dt))
        except Exception as e:
            logger.warning(f"[refresh_recent_notes] skipping id={u.get('id')}: {e}")

    staging_stats = (
        await _upsert_staging(mapped, account_id, engine) if mapped
        else {"staging_inserted": 0, "staging_updated": 0}
    )
    logger.info(f"[refresh_recent_notes] account_id={account_id}: staging {staging_stats}")

    insert_result = await process_customer_notes(account_id, None, engine)
    update_result = await update_changed_notes(account_id, engine)

    logger.info(
        f"[refresh_recent_notes] DONE for account_id={account_id}: "
        f"{staging_stats}, customer_notes_inserted={insert_result['inserted_records']}, "
        f"customer_notes_updated={update_result['updated_records']}"
    )

    return {
        **staging_stats,
        "customer_notes_inserted": insert_result["inserted_records"],
        "customer_notes_updated": update_result["updated_records"],
    }
