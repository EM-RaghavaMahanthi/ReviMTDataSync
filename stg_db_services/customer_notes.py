import logging
from sqlalchemy import text
from stg_db_services._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)

_TABLE = "customer_notes"


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
            result = conn.execute(text(f"""
                SELECT COUNT(*) as count
                FROM public.{_TABLE}
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
            result = conn.execute(text(f"""
                SELECT COUNT(*) as count
                FROM mt_user_notes_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND EXISTS (
                    SELECT 1 FROM {_TABLE} cn
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


async def step_3b_count_missing_customers(account_id: str, engine):
    """
    Step 3b: Count staging rows whose customer_id has no matching row in `customers` for
    this account (removed/merged/never-synced customer) — these are legitimately excluded
    by step_4/step_5's INNER JOIN, not a bug, so they must be subtracted from the expected
    count. Same pattern as credit_transactions.py's step_3_count_transactions_with_missing_customers.
    """
    logger.info(f"[STEP 3b] Counting staging rows with no matching customer")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_user_notes_details_dlk stg
                LEFT JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
                WHERE stg.account_id = :account_id
                  AND c.customer_id IS NULL
            """), {"account_id": account_id})
            missing_count = result.fetchone()[0]
        if missing_count > 0:
            logger.warning(f"[STEP 3b] {missing_count} staging notes reference a customer_id not found in customers")
        logger.info(f"[STEP 3b] SUCCESS: Records with missing customer: {missing_count}")
        return missing_count
    except Exception as e:
        logger.error(f"[STEP 3b] ERROR: Failed to count missing-customer records: {e}")
        raise


async def step_3c_count_null_notes(account_id: str, engine):
    """
    Step 3c: Count staging rows where note text is NULL (MarianaTek sent no `text`
    attribute) — customer_notes.note is NOT NULL, so these are excluded by step_4/step_5's
    filter rather than crashing the whole insert batch. Subtracted from the expected count.
    """
    logger.info(f"[STEP 3c] Counting staging rows with null note text")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_user_notes_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.note IS NULL
            """), {"account_id": account_id})
            null_count = result.fetchone()[0]
        if null_count > 0:
            logger.warning(f"[STEP 3c] {null_count} staging notes have null note text — will be skipped")
        logger.info(f"[STEP 3c] SUCCESS: Records with null note: {null_count}")
        return null_count
    except Exception as e:
        logger.error(f"[STEP 3c] ERROR: Failed to count null-note records: {e}")
        raise


async def step_4_count_records_to_insert(account_id: str, engine):
    """Step 4: Count staging records NOT in main table (ready to insert)."""
    logger.info(f"[STEP 4] Counting staging records ready for insertion")
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) as count
                FROM mt_user_notes_details_dlk stg
                INNER JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
                WHERE stg.account_id = :account_id
                  AND stg.note IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM {_TABLE} cn
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
        insert_sql = text(f"""
            INSERT INTO public.{_TABLE} (
              account_id, customer_id, customer_ref_id, note_id, note, note_datetime,
              created_at, created_by, updated_at, updated_by, deleted_at, deleted_by
            )
            SELECT
              stg.account_id, stg.customer_id, c.id AS customer_ref_id, stg.note_id, stg.note, stg.note_datetime,
              NOW() AS created_at, -1 AS created_by, NOW() AS updated_at, -1 AS updated_by, NULL, NULL
            FROM mt_user_notes_details_dlk stg
            INNER JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
            WHERE stg.account_id = :account_id
              AND stg.note IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM {_TABLE} cn
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


async def process_customer_notes(account_id: str, location_id: int, engine, write: bool = True):
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
    missing_customer_records = 0
    null_note_records = 0
    ready_to_insert = 0
    actual_inserted = 0

    try:
        dup_stats = drop_staging_duplicates(engine, "mt_user_notes_details_dlk", "note_id", account_id)
        duplicates_removed = dup_stats["duplicates_removed"]

        total_staging = await step_1_count_staging_total(account_id, engine)
        existing_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        already_exist_in_main = await step_3_count_records_already_exist_in_main_table(account_id, engine)
        missing_customer_records = await step_3b_count_missing_customers(account_id, engine)
        null_note_records = await step_3c_count_null_notes(account_id, engine)
        ready_to_insert = await step_4_count_records_to_insert(account_id, engine)

        expected_ready = total_staging - already_exist_in_main - missing_customer_records - null_note_records
        logger.info(f"[process_customer_notes] Pre-insertion validation: expected={expected_ready}, actual={ready_to_insert}")
        if ready_to_insert != expected_ready:
            raise Exception(f"Count mismatch! Expected {expected_ready}, got {ready_to_insert}. Manual review required.")

        if ready_to_insert == 0:
            logger.info(f"[process_customer_notes] No records to insert - skipping insertion step")
        else:
            if write:
                actual_inserted = await step_5_insert_new_records(account_id, engine)
            else:
                logger.warning(
                    f"[process_customer_notes] WRITE DISABLED — {ready_to_insert} rows "
                    f"would have been inserted into customer_notes; inserting nothing"
                )

        logger.info(
            f"[process_customer_notes] SUCCESS for account_id={account_id}: "
            f"duplicates_removed={duplicates_removed}, total_staging={total_staging}, "
            f"already_exist={already_exist_in_main}, missing_customer={missing_customer_records}, "
            f"null_note={null_note_records}, inserted={actual_inserted}"
        )

        return {
            "duplicates_removed": duplicates_removed,
            "total_records": total_staging,
            "existing_records": existing_in_main,
            "already_exist_records": already_exist_in_main,
            "missing_customer_records": missing_customer_records,
            "null_note_records": null_note_records,
            "ready_to_insert": ready_to_insert,
            "inserted_records": actual_inserted,
        }

    except Exception as e:
        logger.error(f"[process_customer_notes] ERROR: Failed processing for account_id={account_id}: {e}")
        raise


async def update_changed_notes(account_id: str, engine) -> dict:
    """
    Update customer_notes rows whose note text changed since they were first synced
    (note_id already present in both staging and main, but with different `note` content).
    process_customer_notes above is insert-only by design (re-checking every historical note
    for drift on every regular Stage 3 run would be wasteful) - this is only meant to be
    called by the refresh_recent_notes backfill job, which only ever looks at a small recent
    time window, so a full comparison scan here is cheap.
    """
    logger.info(f"[update_changed_notes] Checking for changed notes for account_id={account_id}")
    try:
        update_sql = text(f"""
            UPDATE public.{_TABLE} cn
            SET note = stg.note, note_datetime = stg.note_datetime,
                updated_at = NOW(), updated_by = -1
            FROM mt_user_notes_details_dlk stg
            WHERE cn.account_id = :account_id AND stg.account_id = :account_id
              AND cn.note_id = stg.note_id
              AND cn.note IS DISTINCT FROM stg.note
        """)
        with engine.begin() as conn:
            result = conn.execute(update_sql, {"account_id": account_id})
            updated_count = result.rowcount
        logger.info(f"[update_changed_notes] SUCCESS: {updated_count} notes updated for account_id={account_id}")
        return {"updated_records": updated_count}
    except Exception as e:
        logger.error(f"[update_changed_notes] ERROR: Failed updating changed notes for account_id={account_id}: {e}")
        raise
