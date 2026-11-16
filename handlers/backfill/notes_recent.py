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

Catch-up: an optional "since" (ISO-8601, UTC if no offset) replaces the window start for one
call, so a gap after an outage can be refilled without changing NOTES_WINDOW_HOURS on the live
function, which the 3-hourly cron also reads. Notes are not lost when a run fails, only unfetched:
MarianaTek still has them, and the upsert is keyed on note_id, so overlapping windows are safe.

Staging table: this job creates mt_user_notes_details_dlk itself if it is missing. It used to
rely on Stage 2 (sql_cmds/create_staging_tables.sql) having left one behind, but Stage 2 only runs
on onboarding. So when something else dropped the table (11-10 and 11-13), every refresh failed
until the next onboarding happened to recreate it.

Event payload:
  { "event_type": "refresh_recent_notes", "account_id": 4809 }
  { "event_type": "refresh_recent_notes", "account_id": 4809, "since": "2026-11-13T05:00:00Z" }
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


# Same columns as mt_user_notes_details_dlk in sql_cmds/create_staging_tables.sql, so whichever
# side creates it, the other finds what it expects. Keep the two in step.
_STAGING_DDL = """
    CREATE TABLE IF NOT EXISTS "mt_user_notes_details_dlk" (
      id integer,
      account_id integer,
      customer_id character varying(255),
      customer_ref_id integer,
      note_id character varying(64),
      note text,
      note_datetime timestamp(3) without time zone,
      is_pinned boolean,
      author_id character varying(255),
      location integer,
      created_at timestamp(3) without time zone,
      created_by integer,
      updated_at timestamp(3) without time zone,
      updated_by integer,
      deleted_at timestamp(3) without time zone,
      deleted_by integer
    )
"""


def _ensure_staging_table(engine) -> None:
    # IF NOT EXISTS is not race-free in Postgres: two sessions creating at once can both pass
    # the check and one fails on pg_type's unique index. The transaction-scoped advisory lock
    # serialises creators. The cron runs accounts one at a time, but a manual catch-up may not.
    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('mt_user_notes_details_dlk'))"))
        conn.execute(text(_STAGING_DDL))


def _parse_since(since: str, now: datetime) -> datetime:
    dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt >= now:
        raise ValueError(f"since={since} is not in the past")
    return dt


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


_PAGE_SIZE = 200  # same as core.config PAGE_SIZE; MT accepts up to 500


async def _fetch_recent_notes(api_base_url: str, min_dt: datetime, max_dt: datetime) -> list:
    """Paginate /user_notes filtered to [min_dt, max_dt].

    /user_notes returns NO `links` block, only meta.pagination {count, pages, page,
    per_page}. Stopping on a missing links.next therefore ended every run after page 1. With
    MT's default per_page of 10, each run kept only the 10 newest notes in its window.
    Follow meta.pagination.pages, still honour links.next if MT ever sends it, and stop on an
    empty page either way.
    """
    all_data = []
    page = 1
    while True:
        params = {
            "min_note_datetime": _fmt(min_dt),
            "max_note_datetime": _fmt(max_dt),
            "page": page,
            "page_size": _PAGE_SIZE,
        }
        resp = await api_get("/user_notes", api_base_url, params)
        data = resp.get("data", [])
        all_data.extend(data)
        pages = ((resp.get("meta") or {}).get("pagination") or {}).get("pages") or 0
        has_next = bool((resp.get("links") or {}).get("next"))
        if not data or not (has_next or page < pages):
            break
        page += 1
    logger.info(f"[refresh_recent_notes] fetched {len(all_data)} notes across {page} page(s)")
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


async def run(account_id: str, engine, write: bool = True, since: str = None) -> dict:
    max_dt = datetime.now(timezone.utc)
    if since:
        min_dt = _parse_since(since, max_dt)
    else:
        window_hours = int(os.environ.get("NOTES_WINDOW_HOURS", _DEFAULT_WINDOW_HOURS))
        min_dt = max_dt - timedelta(hours=window_hours)

    _ensure_staging_table(engine)
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

    # Staging above is always refreshed — it is scratch. `write` gates only the main
    # customer_notes table, both the insert and the edit-propagating update.
    insert_result = await process_customer_notes(account_id, None, engine, write=write)
    if write:
        update_result = await update_changed_notes(account_id, engine)
    else:
        logger.warning(
            "[refresh_recent_notes] WRITE DISABLED — skipping update_changed_notes"
        )
        update_result = {"updated_records": 0}

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
