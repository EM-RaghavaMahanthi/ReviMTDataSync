"""
Backfill Lambda — fills NULL line_total values in the order_lines main table,
including recovery of order_lines that moved to a different location in the CRM.

Flow:
  Step 1   CRM → S3              : fetch order_lines by location, write parquet to S3
  Step 2b  Find missing           : DB order_line_ids NOT in S3 (moved location)
  Step 2c  Fetch missing by ID    : call /order_lines?id=... → write to same S3 prefix
                                    found   = moved location (recovered)
                                    not found = totally disappeared from CRM (log + return)
  Step 2   S3 → Staging           : NOW create mt_order_lines_details_dlk and load
                                    ALL S3 files (location batch + recovered batch)
  Step 3   Validate               : count rows eligible for line_total update
  Step 3b  Update                 : if update=True, set line_total + verify count

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
from db_services.main_bulk_insert import stage_order_lines_only, fetch_from_s3, TABLE_S3_CONFIG
from sqlalchemy import create_engine, text

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step 0 — Clear existing S3 files for this account
# ---------------------------------------------------------------------------

def _clear_s3_prefix(bucket: str, account_id: str) -> int:
    """Delete all parquet files under the order_lines prefix for this account."""
    s3 = boto3.client("s3")
    prefix = f"{settings.S3_PREFIXES['order_lines']}/account_id_{account_id}/"
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            deleted += len(objects)
    logger.info(f"[backfill] Step 0 — deleted {deleted} S3 objects under {prefix}")
    return deleted


# ---------------------------------------------------------------------------
# Step 1 — CRM → S3
# ---------------------------------------------------------------------------

async def _fetch_to_s3(account_id: str, location_id: str, api_base_url: str) -> tuple[int, int]:
    from crm_sync.order_lines import process_order_lines_for_location
    return await process_order_lines_for_location(location_id, account_id, api_base_url)


# ---------------------------------------------------------------------------
# Step 2b — find IDs in DB but NOT in S3 (moved location)
# ---------------------------------------------------------------------------

def _find_missing_ids(bucket: str, account_id: str, engine) -> list[str]:
    """
    Compare DB order_line_ids against what's currently in S3 for this account.
    Returns IDs that are in the main table but were not fetched from the CRM
    location endpoint — these likely moved to a different location.
    """
    import pandas as pd

    config  = TABLE_S3_CONFIG["order_lines"]
    prefix  = f"{config['s3_prefix']}/account_id_{account_id}"
    df      = fetch_from_s3(bucket, prefix, account_id)

    s3_ids  = set(df["order_line_id"].dropna().astype(str).tolist()) if not df.empty else set()
    logger.info(f"[backfill] S3 has {len(s3_ids)} unique order_line_ids for account {account_id}")

    with engine.connect() as conn:
        rows   = conn.execute(text(
            "SELECT order_line_id FROM order_lines WHERE account_id = :account_id"
        ), {"account_id": account_id}).fetchall()
    db_ids = {str(r[0]) for r in rows}
    logger.info(f"[backfill] DB has {len(db_ids)} order_line_ids for account {account_id}")

    missing = list(db_ids - s3_ids)
    logger.info(f"[backfill] {len(missing)} IDs in DB but not in S3 (missing from location fetch)")
    return missing


# ---------------------------------------------------------------------------
# Step 2c — fetch missing by ID → write to same S3 prefix
# ---------------------------------------------------------------------------

async def _fetch_missing_to_s3(
    missing_ids: list[str],
    api_base_url: str,
    account_id: str,
) -> tuple[list, list]:
    """
    Fetch order_lines by ID from CRM and write them to S3 under the same
    order_lines prefix so stage_order_lines_only picks them up automatically.

    Returns:
        (moved_location, totally_disappeared)
        moved_location      — found by ID, written to S3 (moved to another location)
        totally_disappeared — not found even by ID (gone from CRM entirely)
    """
    from crm_sync.order_lines import fetch_by_ids
    from utils.s3_writer import write_parquet_to_s3

    moved_location, totally_disappeared = await fetch_by_ids(
        missing_ids, api_base_url, account_id
    )

    if moved_location:
        # Write to the same prefix as regular location files so staging picks them up
        s3_prefix = settings.S3_PREFIXES["order_lines"]
        await write_parquet_to_s3(
            moved_location,
            entity_id=account_id,
            batch_num=1,
            account_id=account_id,
            s3_prefix=s3_prefix,
            entity_type="recovered",
        )
        logger.info(
            f"[backfill] Step 2c — {len(moved_location)} moved-location records "
            f"written to S3 (same prefix, entity_type=recovered)"
        )

    if totally_disappeared:
        logger.warning(
            f"[backfill] {len(totally_disappeared)} order_lines totally disappeared from CRM "
            f"(not found by ID): "
            + ", ".join(str(x) for x in totally_disappeared[:20])
            + (" ..." if len(totally_disappeared) > 20 else "")
        )

    return moved_location, totally_disappeared


# ---------------------------------------------------------------------------
# Step 3 — validate & update
# ---------------------------------------------------------------------------

def _count_eligible(engine, account_id: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM order_lines ol
            INNER JOIN mt_order_lines_details_dlk stg
                ON stg.order_line_id = ol.order_line_id
               AND stg.account_id    = :account_id
            WHERE ol.account_id  = :account_id
              AND ol.line_total  IS NULL
              AND stg.line_total IS NOT NULL
        """), {"account_id": account_id})
        return result.fetchone()[0]


def _update_line_total(engine, account_id: str) -> int:
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE order_lines ol
            SET    line_total = stg.line_total,
                   updated_at = now()
            FROM   mt_order_lines_details_dlk stg
            WHERE  ol.order_line_id = stg.order_line_id
              AND  ol.account_id    = :account_id
              AND  stg.account_id   = :account_id
              AND  ol.line_total   IS NULL
              AND  stg.line_total  IS NOT NULL
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

    engine = create_engine(settings.PROD_DATABASE_URL)
    start  = time.time()

    result = {
        "account_id":  account_id,
        "location_id": location_id,
        "update":      do_update,
    }

    # ── Step 0: Clear existing S3 files for this account ─────────────────────
    try:
        logger.info("[backfill] Step 0 — clearing existing S3 files for account")
        deleted = _clear_s3_prefix(settings.S3_BUCKET, account_id)
        result["step0_s3_files_deleted"] = deleted
    except Exception as e:
        logger.error(f"[backfill] Step 0 failed: {e}")
        result.update({"status": "error", "error": f"Step 0 (clear S3) failed: {e}"})
        return result

    # ── Step 1: CRM → S3 (location fetch) ────────────────────────────────────
    try:
        logger.info("[backfill] Step 1 — fetching order_lines from CRM → S3 (by location)")
        pages_fetched, records_written = asyncio.run(
            _fetch_to_s3(account_id, location_id, api_base_url)
        )
        logger.info(f"[backfill] Step 1 done — pages={pages_fetched}, records={records_written}")
        result["step1_pages_fetched"] = pages_fetched
        result["step1_records_to_s3"] = records_written
    except Exception as e:
        logger.error(f"[backfill] Step 1 failed: {e}")
        result.update({"status": "error", "error": f"Step 1 (CRM→S3) failed: {e}"})
        return result

    # ── Step 2b: Find IDs in DB but not in S3 ────────────────────────────────
    try:
        missing_ids = _find_missing_ids(settings.S3_BUCKET, account_id, engine)
        result["step2b_missing_from_location"] = len(missing_ids)
    except Exception as e:
        logger.error(f"[backfill] Step 2b failed: {e}")
        result.update({"status": "error", "error": f"Step 2b (find missing) failed: {e}"})
        return result

    # ── Step 2c: Fetch missing by ID → S3 ────────────────────────────────────
    result["step2c_moved_location"]      = 0
    result["step2c_totally_disappeared"] = 0
    result["step2c_totally_disappeared_ids"] = []

    if missing_ids:
        try:
            moved_location, totally_disappeared = asyncio.run(
                _fetch_missing_to_s3(missing_ids, api_base_url, account_id)
            )
            result["step2c_moved_location"]          = len(moved_location)
            result["step2c_totally_disappeared"]      = len(totally_disappeared)
            result["step2c_totally_disappeared_ids"]  = totally_disappeared
            logger.info(
                f"[backfill] Step 2c done — "
                f"moved_location={len(moved_location)}, "
                f"totally_disappeared={len(totally_disappeared)}"
            )
        except Exception as e:
            logger.error(f"[backfill] Step 2c failed: {e}")
            result.update({"status": "error", "error": f"Step 2c (fetch by ID) failed: {e}"})
            return result
    else:
        logger.info("[backfill] Step 2c — skipped, no missing IDs")

    # ── Step 2: S3 → Staging (after ALL S3 writes are done) ──────────────────
    try:
        logger.info("[backfill] Step 2 — creating staging table and loading ALL S3 files")
        staged_count = stage_order_lines_only(settings.S3_BUCKET, account_id, engine)
        logger.info(f"[backfill] Step 2 done — {staged_count} rows staged (location + recovered)")
        result["step2_staged_rows"] = staged_count
    except Exception as e:
        logger.error(f"[backfill] Step 2 failed: {e}")
        result.update({"status": "error", "error": f"Step 2 (S3→Staging) failed: {e}"})
        return result

    # ── Step 3: Validate ─────────────────────────────────────────────────────
    try:
        eligible = _count_eligible(engine, account_id)
        logger.info(f"[backfill] Step 3 — {eligible} rows eligible for line_total update")
        result["step3_eligible_rows"] = eligible
    except Exception as e:
        logger.error(f"[backfill] Step 3 count failed: {e}")
        result.update({"status": "error", "error": f"Step 3 (count) failed: {e}"})
        return result

    if eligible == 0:
        logger.info("[backfill] Nothing to update — line_total already filled or no staging data")
        result.update({"status": "success", "elapsed_seconds": round(time.time() - start, 2)})
        return result

    # ── Step 3b: Update ───────────────────────────────────────────────────────
    if not do_update:
        logger.info(f"[backfill] update=False — dry run complete. {eligible} rows would be updated.")
        result.update({"status": "dry_run", "elapsed_seconds": round(time.time() - start, 2)})
        return result

    try:
        updated = _update_line_total(engine, account_id)
        logger.info(f"[backfill] Step 3b — updated {updated} rows")
        result["step3b_updated_rows"] = updated

        if updated != eligible:
            logger.error(f"[backfill] Validation failed — expected {eligible}, updated {updated}")
            result.update({
                "status": "error",
                "error":  f"Update count mismatch: expected {eligible}, got {updated}",
            })
            return result

        logger.info(f"[backfill] Validation passed — {updated}/{eligible} rows updated")
        result.update({"status": "success", "elapsed_seconds": round(time.time() - start, 2)})
    except Exception as e:
        logger.error(f"[backfill] Step 3b update failed: {e}")
        result.update({"status": "error", "error": f"Step 3b (update) failed: {e}"})

    return result
