"""
Backfill Lambda — fills NULL / stale class_name, class_type_name, capacity values
in the class_sessions main table by re-fetching from the CRM.

Flow:
  Step 0   Clear S3              : delete existing parquet files for this account
  Step 1   CRM → S3              : fetch class_sessions by location, write parquet to S3
  Step 2b  Find missing          : DB class_session_ids NOT in S3 (moved location)
  Step 2c  Fetch missing by ID   : call /class_sessions/{id} → write to same S3 prefix
                                   found         = moved location (recovered)
                                   not found     = gone from CRM entirely (log + continue)
  Step 2   S3 → Staging          : create mt_class_sessions_details_dlk and load ALL S3 files
  Step 3   Validate              : count rows eligible for update
                                   (any of the 3 cols differs from staging, staging not NULL)
  Step 3b  Update                : if update=True, set columns + verify count

Event payload:
  {
    "account_id":   "1740",
    "location_id":  "48816",
    "api_base_url": "https://...",
    "update":       false
  }
"""

import asyncio
import time
import logging

import boto3
from core.config import settings
from core.logger import setup_logging
from db_services.main_bulk_insert import stage_class_sessions_only, fetch_from_s3, TABLE_S3_CONFIG
from sqlalchemy import create_engine, text

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step 0 — Clear existing S3 files for this account
# ---------------------------------------------------------------------------

def _clear_s3_prefix(bucket: str, account_id: str) -> int:
    s3 = boto3.client("s3")
    prefix = f"{settings.S3_PREFIXES['class_sessions']}/account_id_{account_id}/"
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            deleted += len(objects)
    logger.info(f"[backfill_cs] Step 0 — deleted {deleted} S3 objects under {prefix}")
    return deleted


# ---------------------------------------------------------------------------
# Step 1 — CRM → S3
# ---------------------------------------------------------------------------

async def _fetch_to_s3(account_id: str, location_id: str, api_base_url: str) -> tuple[int, int]:
    from crm_sync.class_sessions import process_class_sessions_for_location
    return await process_class_sessions_for_location(location_id, account_id, api_base_url)


# ---------------------------------------------------------------------------
# Step 2b — find IDs in DB but NOT in S3 (moved location)
# ---------------------------------------------------------------------------

def _find_missing_ids(bucket: str, account_id: str, engine) -> list[str]:
    config = TABLE_S3_CONFIG["class_sessions"]
    prefix = f"{config['s3_prefix']}/account_id_{account_id}"
    df     = fetch_from_s3(bucket, prefix, account_id)

    s3_ids = set(df["class_session_id"].dropna().astype(str).tolist()) if not df.empty else set()
    logger.info(f"[backfill_cs] S3 has {len(s3_ids)} unique class_session_ids for account {account_id}")

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT class_session_id FROM class_sessions WHERE account_id = :account_id"
        ), {"account_id": account_id}).fetchall()
    db_ids = {str(r[0]) for r in rows}
    logger.info(f"[backfill_cs] DB has {len(db_ids)} class_session_ids for account {account_id}")

    missing = list(db_ids - s3_ids)
    logger.info(f"[backfill_cs] {len(missing)} IDs in DB but not in S3 (missing from location fetch)")
    return missing


# ---------------------------------------------------------------------------
# Step 2c — fetch missing by ID → write to same S3 prefix
# ---------------------------------------------------------------------------

async def _fetch_missing_to_s3(
    missing_ids: list[str],
    api_base_url: str,
    account_id: str,
) -> tuple[list, list]:
    from crm_sync.class_sessions import fetch_by_ids
    from utils.s3_writer import write_parquet_to_s3

    found, totally_disappeared = await fetch_by_ids(missing_ids, api_base_url, account_id)

    if found:
        s3_prefix = settings.S3_PREFIXES["class_sessions"]
        await write_parquet_to_s3(
            found,
            entity_id=account_id,
            batch_num=1,
            account_id=account_id,
            s3_prefix=s3_prefix,
            entity_type="recovered",
        )
        logger.info(
            f"[backfill_cs] Step 2c — {len(found)} moved-location records "
            f"written to S3 (entity_type=recovered)"
        )

    if totally_disappeared:
        logger.warning(
            f"[backfill_cs] {len(totally_disappeared)} class_sessions totally disappeared from CRM: "
            + ", ".join(str(x) for x in totally_disappeared[:20])
            + (" ..." if len(totally_disappeared) > 20 else "")
        )

    return found, totally_disappeared


# ---------------------------------------------------------------------------
# Step 3 — count eligible rows
# ---------------------------------------------------------------------------

def _count_eligible(engine, account_id: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM class_sessions cs
            INNER JOIN mt_class_sessions_details_dlk stg
                ON  stg.class_session_id = cs.class_session_id
                AND stg.account_id       = cs.account_id
            WHERE cs.account_id = :account_id
              AND (
                (stg.class_name      IS NOT NULL AND stg.class_name      IS DISTINCT FROM cs.class_name)
                OR (stg.class_type_name IS NOT NULL AND stg.class_type_name IS DISTINCT FROM cs.class_type_name)
                OR (stg.capacity        IS NOT NULL AND stg.capacity        IS DISTINCT FROM cs.capacity)
              )
        """), {"account_id": account_id})
        return result.fetchone()[0]


# ---------------------------------------------------------------------------
# Step 3b — update
# ---------------------------------------------------------------------------

def _update_class_sessions(engine, account_id: str) -> int:
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE class_sessions cs
            SET
                class_name      = COALESCE(stg.class_name,      cs.class_name),
                class_type_name = COALESCE(stg.class_type_name, cs.class_type_name),
                capacity        = COALESCE(stg.capacity,        cs.capacity)
            FROM mt_class_sessions_details_dlk stg
            WHERE  stg.class_session_id = cs.class_session_id
              AND  stg.account_id       = cs.account_id
              AND  cs.account_id        = :account_id
              AND (
                (stg.class_name      IS NOT NULL AND stg.class_name      IS DISTINCT FROM cs.class_name)
                OR (stg.class_type_name IS NOT NULL AND stg.class_type_name IS DISTINCT FROM cs.class_type_name)
                OR (stg.capacity        IS NOT NULL AND stg.capacity        IS DISTINCT FROM cs.capacity)
              )
        """), {"account_id": account_id})
        return result.rowcount


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context=None):
    account_id   = str(event.get("account_id",  ""))
    location_id  = str(event.get("location_id", ""))
    api_base_url = event.get("api_base_url", "")
    do_update    = bool(event.get("update", False))

    if not account_id:
        return {"status": "error", "error": "account_id is required"}
    if not location_id:
        return {"status": "error", "error": "location_id is required"}
    if not api_base_url:
        return {"status": "error", "error": "api_base_url is required"}

    engine = create_engine(settings.DATABASE_URL)
    start  = time.time()

    result = {
        "account_id":  account_id,
        "location_id": location_id,
        "update":      do_update,
    }

    # ── Step 0: Clear existing S3 files ──────────────────────────────────────
    try:
        logger.info("[backfill_cs] Step 0 — clearing existing S3 files for account")
        deleted = _clear_s3_prefix(settings.S3_BUCKET, account_id)
        result["step0_s3_files_deleted"] = deleted
    except Exception as e:
        logger.error(f"[backfill_cs] Step 0 failed: {e}")
        result.update({"status": "error", "error": f"Step 0 (clear S3) failed: {e}"})
        return result

    # ── Step 1: CRM → S3 (location fetch) ────────────────────────────────────
    try:
        logger.info("[backfill_cs] Step 1 — fetching class_sessions from CRM → S3 (by location)")
        pages_fetched, records_written = asyncio.run(
            _fetch_to_s3(account_id, location_id, api_base_url)
        )
        logger.info(f"[backfill_cs] Step 1 done — pages={pages_fetched}, records={records_written}")
        result["step1_pages_fetched"] = pages_fetched
        result["step1_records_to_s3"] = records_written
    except Exception as e:
        logger.error(f"[backfill_cs] Step 1 failed: {e}")
        result.update({"status": "error", "error": f"Step 1 (CRM→S3) failed: {e}"})
        return result

    # ── Step 2b: Find IDs in DB but not in S3 ────────────────────────────────
    try:
        missing_ids = _find_missing_ids(settings.S3_BUCKET, account_id, engine)
        result["step2b_missing_from_location"] = len(missing_ids)
    except Exception as e:
        logger.error(f"[backfill_cs] Step 2b failed: {e}")
        result.update({"status": "error", "error": f"Step 2b (find missing) failed: {e}"})
        return result

    # ── Step 2c: Fetch missing by ID → S3 ────────────────────────────────────
    result["step2c_recovered"]           = 0
    result["step2c_totally_disappeared"] = 0

    if missing_ids:
        try:
            found, totally_disappeared = asyncio.run(
                _fetch_missing_to_s3(missing_ids, api_base_url, account_id)
            )
            result["step2c_recovered"]           = len(found)
            result["step2c_totally_disappeared"]  = len(totally_disappeared)
            logger.info(
                f"[backfill_cs] Step 2c done — "
                f"recovered={len(found)}, totally_disappeared={len(totally_disappeared)}"
            )
        except Exception as e:
            logger.error(f"[backfill_cs] Step 2c failed: {e}")
            result.update({"status": "error", "error": f"Step 2c (fetch by ID) failed: {e}"})
            return result
    else:
        logger.info("[backfill_cs] Step 2c — skipped, no missing IDs")

    # ── Step 2: S3 → Staging (after ALL S3 writes are done) ──────────────────
    try:
        logger.info("[backfill_cs] Step 2 — creating staging table and loading ALL S3 files")
        staged_count = stage_class_sessions_only(settings.S3_BUCKET, account_id, engine)
        logger.info(f"[backfill_cs] Step 2 done — {staged_count} rows staged (location + recovered)")
        result["step2_staged_rows"] = staged_count
    except Exception as e:
        logger.error(f"[backfill_cs] Step 2 failed: {e}")
        result.update({"status": "error", "error": f"Step 2 (S3→Staging) failed: {e}"})
        return result

    # ── Step 3: Count eligible ────────────────────────────────────────────────
    try:
        eligible = _count_eligible(engine, account_id)
        logger.info(f"[backfill_cs] Step 3 — {eligible} rows eligible for class_sessions update")
        result["step3_eligible_rows"] = eligible
    except Exception as e:
        logger.error(f"[backfill_cs] Step 3 count failed: {e}")
        result.update({"status": "error", "error": f"Step 3 (count) failed: {e}"})
        return result

    if eligible == 0:
        logger.info("[backfill_cs] Nothing to update — columns already filled or no staging data")
        result.update({"status": "success", "elapsed_seconds": round(time.time() - start, 2)})
        return result

    # ── Step 3b: Update ───────────────────────────────────────────────────────
    if not do_update:
        logger.info(f"[backfill_cs] update=False — dry run complete. {eligible} rows would be updated.")
        result.update({"status": "dry_run", "elapsed_seconds": round(time.time() - start, 2)})
        return result

    try:
        updated = _update_class_sessions(engine, account_id)
        logger.info(f"[backfill_cs] Step 3b — updated {updated} rows")
        result["step3b_updated_rows"] = updated

        if updated != eligible:
            logger.error(f"[backfill_cs] Validation failed — expected {eligible}, updated {updated}")
            result.update({
                "status": "error",
                "error":  f"Update count mismatch: expected {eligible}, got {updated}",
            })
            return result

        logger.info(f"[backfill_cs] Validation passed — {updated}/{eligible} rows updated")
        result.update({"status": "success", "elapsed_seconds": round(time.time() - start, 2)})
    except Exception as e:
        logger.error(f"[backfill_cs] Step 3b update failed: {e}")
        result.update({"status": "error", "error": f"Step 3b (update) failed: {e}"})

    return result
