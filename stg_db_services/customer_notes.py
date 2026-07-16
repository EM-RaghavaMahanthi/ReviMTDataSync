import logging
from sqlalchemy import text
from stg_db_services._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)


async def step_1_count_staging_total(account_id: str, engine):
    """Step 1: Count total entries in staging table (account-scoped — no location filter)."""
    logger.info(f"[STEP 1] Counting total entries in mt_user_notes_details_dlk")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count FROM public.mt_user_notes_details_dlk
                WHERE account_id = :account_id
            """), {"account_id": account_id})
            total_count = result.fetchone()[0]
        logger.info(f"[STEP 1] SUCCESS: Total entries in staging table: {total_count}")
        return total_count
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count staging table entries: {e}")
        raise


async def step_2_count_existing_in_main_table(account_id: str, engine):
    """Step 2: Count entries already in main customer_notes table."""
    logger.info(f"[STEP 2] Counting entries already in main customer_notes table")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM public.customer_notes
                WHERE account_id = :account_id
            """), {"account_id": account_id})
            existing_count = result.fetchone()[0]
        logger.info(f"[STEP 2] SUCCESS: Entries already in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing entries in main table: {e}")
        raise


async def step_3_count_records_already_exist_in_main_table(account_id: str, engine):
    """Step 3: Count staging records whose (account_id, customer_id, note_id) already exists in main table."""
    logger.info(f"[STEP 3] Counting staging records that already exist in main table")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_user_notes_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND EXISTS (
                    SELECT 1 FROM customer_notes cn
                    WHERE cn.account_id = stg.account_id AND cn.customer_id = stg.customer_id
                      AND cn.note_id = stg.note_id
                  )
            """), {"account_id": account_id})
            duplicate_count = result.fetchone()[0]
        logger.info(f"[STEP 3] SUCCESS: Staging records already in main table (duplicates): {duplicate_count}")
        return duplicate_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count duplicate records: {e}")
        raise


async def step_4_count_records_to_insert(account_id: str, engine):
    """Step 4: Count staging records NOT in main table (ready to insert)."""
    logger.info(f"[STEP 4] Counting staging records ready for insertion")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_user_notes_details_dlk stg
                INNER JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
                WHERE stg.account_id = :account_id
                  AND NOT EXISTS (
                    SELECT 1 FROM customer_notes cn
                    WHERE cn.account_id = stg.account_id AND cn.customer_id = stg.customer_id
                      AND cn.note_id = stg.note_id
                  )
            """), {"account_id": account_id})
            insert_ready_count = result.fetchone()[0]
        logger.info(f"[STEP 4] SUCCESS: Records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count records ready for insertion: {e}")
        raise


async def step_5_insert_new_records(account_id: str, engine):
    """
    Step 5: Insert new records from staging to main customer_notes table.
    customer_ref_id resolved via the standard customers join
    (stg.customer_id = c.customer_id AND stg.account_id = c.account_id).
    Idempotency key: (account_id, customer_id, note_id).
    created_by/updated_by hardcoded to 1, created_at/updated_at to NOW() per convention;
    deleted_at/deleted_by left NULL. is_pinned/author_id have no target column — not carried over.
    """
    logger.info(f"[STEP 5] Inserting new records into main customer_notes table")
    try:
        insert_sql = text("""
            INSERT INTO public.customer_notes (
              account_id, customer_id, customer_ref_id, note_id, note, note_datetime,
              created_at, created_by, updated_at, updated_by, deleted_at, deleted_by
            )
            SELECT
              stg.account_id, stg.customer_id, c.id AS customer_ref_id, stg.note_id, stg.note, stg.note_datetime,
              NOW() AS created_at, 1 AS created_by, NOW() AS updated_at, 1 AS updated_by, NULL, NULL
            FROM mt_user_notes_details_dlk stg
            INNER JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
            WHERE stg.account_id = :account_id
              AND NOT EXISTS (
                SELECT 1 FROM customer_notes cn
                WHERE cn.account_id = stg.account_id AND cn.customer_id = stg.customer_id
                  AND cn.note_id = stg.note_id
              )
        """)
        with engine.begin() as conn:
            result = conn.execute(insert_sql, {"account_id": account_id})
            inserted_count = result.rowcount
        logger.info(f"[STEP 5] SUCCESS: Successfully inserted {inserted_count} new customer_notes records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to insert new records: {e}")
        raise


async def process_customer_notes(account_id: str, location_id: int, engine):
    """
    Process customer_notes from staging to final table.
    Not location-scoped (user_notes has no location filter at the CRM layer) — every
    step is account_id-only, location_id is accepted (for PROCESSING_ORDER's uniform
    call signature) but unused.
    """
    logger.info(f"[process_customer_notes] Starting customer_notes processing for account_id={account_id}")

    duplicates_removed = 0
    total_staging = 0
    existing_in_main = 0
    already_exist_in_main = 0
    ready_to_insert = 0
    actual_inserted = 0

    try:
        dup_stats = drop_staging_duplicates(engine, "mt_user_notes_details_dlk", "note_id", account_id)
        duplicates_removed = dup_stats["duplicates_removed"]

        total_staging = await step_1_count_staging_total(account_id, engine)
        existing_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        already_exist_in_main = await step_3_count_records_already_exist_in_main_table(account_id, engine)
        ready_to_insert = await step_4_count_records_to_insert(account_id, engine)

        expected_ready = total_staging - already_exist_in_main
        logger.info(f"[process_customer_notes] Pre-insertion validation: expected={expected_ready}, actual={ready_to_insert}")
        if ready_to_insert != expected_ready:
            raise Exception(f"Count mismatch! Expected {expected_ready}, got {ready_to_insert}. Manual review required.")

        if ready_to_insert == 0:
            logger.info(f"[process_customer_notes] No records to insert - skipping insertion step")
        else:
            actual_inserted = await step_5_insert_new_records(account_id, engine)

        logger.info(
            f"[process_customer_notes] SUCCESS for account_id={account_id}: "
            f"duplicates_removed={duplicates_removed}, total_staging={total_staging}, "
            f"already_exist={already_exist_in_main}, inserted={actual_inserted}"
        )

        return {
            "duplicates_removed": duplicates_removed,
            "total_records": total_staging,
            "existing_records": existing_in_main,
            "already_exist_records": already_exist_in_main,
            "ready_to_insert": ready_to_insert,
            "inserted_records": actual_inserted,
        }

    except Exception as e:
        logger.error(f"[process_customer_notes] ERROR: Failed processing for account_id={account_id}: {e}")
        raise
