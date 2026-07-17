"""
customer_tags_custom — manual (tag_type='manual') tags. ONE row per (customer, tag name) —
this is NOT a shared tenant-wide definition table like customer_tags_default; each customer's
manual tag gets its own row. Unique key: (account_id, customer_id, name).

mt_customer_tags_details_dlk's natural key is composite (customer_id, tag_id) — drop_staging_
duplicates only supports a single column, so DISTINCT in the insert guards against exact-row
duplicates instead (the Stage 1 stale-clear already prevents this in normal operation).
"""

import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

# TEMP: writing to customer_tags_custom_temp instead of customer_tags_custom to validate
# writes are correct before pointing at the real table. Flip back once verified.
_TABLE = "customer_tags_custom_temp"


async def step_1_count_staging_total(account_id: str, engine):
    """Step 1: Count manual-tag assignment rows in staging for this account."""
    logger.info(f"[STEP 1] Counting manual tag rows in mt_customer_tags_details_dlk")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_customer_tags_details_dlk stg
                INNER JOIN mt_user_tags_details_dlk ut
                  ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
                WHERE stg.account_id = :account_id AND ut.tag_type = 'manual'
            """), {"account_id": account_id})
            total_count = result.fetchone()[0]
        logger.info(f"[STEP 1] SUCCESS: Total manual tag rows in staging: {total_count}")
        return total_count
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count staging table entries: {e}")
        raise


async def step_2_count_existing_in_main_table(account_id: str, engine):
    """Step 2: Count entries already in main customer_tags_custom table."""
    logger.info(f"[STEP 2] Counting entries already in main customer_tags_custom table")
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) as count FROM {_TABLE} WHERE account_id = :account_id
            """), {"account_id": account_id})
            existing_count = result.fetchone()[0]
        logger.info(f"[STEP 2] SUCCESS: Entries already in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing entries in main table: {e}")
        raise


async def step_3_count_records_already_exist_in_main_table(account_id: str, engine):
    """Step 3: Count staging rows whose (account_id, customer_id, name) already exists in customer_tags_custom."""
    logger.info(f"[STEP 3] Counting staging records that already exist in main table")
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) as count
                FROM mt_customer_tags_details_dlk stg
                INNER JOIN mt_user_tags_details_dlk ut
                  ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
                WHERE stg.account_id = :account_id AND ut.tag_type = 'manual'
                  AND EXISTS (
                    SELECT 1 FROM {_TABLE} cust
                    WHERE cust.account_id = stg.account_id AND cust.customer_id = stg.customer_id
                      AND cust.name = ut.name
                  )
            """), {"account_id": account_id})
            duplicate_count = result.fetchone()[0]
        logger.info(f"[STEP 3] SUCCESS: Staging records already in main table (duplicates): {duplicate_count}")
        return duplicate_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count duplicate records: {e}")
        raise


async def step_count_missing_customers(account_id: str, engine):
    """
    Count staging rows (manual tags) whose customer_id has no matching row in `customers`
    for this account — legitimately excluded by step_4/step_5's INNER JOIN, reported here
    for visibility (same pattern as credit_transactions.py / customer_notes.py).
    """
    logger.info(f"[STEP] Counting manual-tag rows with no matching customer")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_customer_tags_details_dlk stg
                INNER JOIN mt_user_tags_details_dlk ut
                  ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
                LEFT JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
                WHERE stg.account_id = :account_id AND ut.tag_type = 'manual'
                  AND c.customer_id IS NULL
            """), {"account_id": account_id})
            missing_count = result.fetchone()[0]
        if missing_count > 0:
            logger.warning(f"[STEP] {missing_count} manual-tag rows reference a customer_id not found in customers")
        return missing_count
    except Exception as e:
        logger.error(f"[STEP] ERROR: Failed to count missing-customer records: {e}")
        raise


async def step_4_count_records_to_insert(account_id: str, engine):
    """Step 4: Count staging rows ready to insert (distinct (account_id, customer_id, name), not already present)."""
    logger.info(f"[STEP 4] Counting staging records ready for insertion")
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(DISTINCT (stg.account_id, stg.customer_id, ut.name)) as count
                FROM mt_customer_tags_details_dlk stg
                INNER JOIN mt_user_tags_details_dlk ut
                  ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
                WHERE stg.account_id = :account_id AND ut.tag_type = 'manual'
                  AND NOT EXISTS (
                    SELECT 1 FROM {_TABLE} cust
                    WHERE cust.account_id = stg.account_id AND cust.customer_id = stg.customer_id
                      AND cust.name = ut.name
                  )
            """), {"account_id": account_id})
            insert_ready_count = result.fetchone()[0]
        logger.info(f"[STEP 4] SUCCESS: Records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count records ready for insertion: {e}")
        raise


async def step_5_insert_new_records(account_id: str, engine):
    """Step 5: Insert new customer_tags_custom rows. customer_ref_id via the standard customers join."""
    logger.info(f"[STEP 5] Inserting new records into main customer_tags_custom table")
    try:
        insert_sql = text(f"""
            INSERT INTO public.{_TABLE} (
              account_id, customer_ref_id, customer_id, name, created_at, created_by
            )
            SELECT DISTINCT
              stg.account_id, c.id AS customer_ref_id, stg.customer_id, ut.name,
              NOW() AS created_at, 1 AS created_by
            FROM mt_customer_tags_details_dlk stg
            INNER JOIN mt_user_tags_details_dlk ut
              ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
            INNER JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
            WHERE stg.account_id = :account_id AND ut.tag_type = 'manual'
              AND NOT EXISTS (
                SELECT 1 FROM {_TABLE} cust
                WHERE cust.account_id = stg.account_id AND cust.customer_id = stg.customer_id
                  AND cust.name = ut.name
              )
        """)
        with engine.begin() as conn:
            result = conn.execute(insert_sql, {"account_id": account_id})
            inserted_count = result.rowcount
        logger.info(f"[STEP 5] SUCCESS: Successfully inserted {inserted_count} new customer_tags_custom records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to insert new records: {e}")
        raise


async def process_customer_tags_custom(account_id: str, engine):
    """Process manual tags from mt_customer_tags_details_dlk into customer_tags_custom."""
    logger.info(f"[process_customer_tags_custom] Starting for account_id={account_id}")

    total_staging = 0
    existing_in_main = 0
    already_exist_in_main = 0
    missing_customer_records = 0
    ready_to_insert = 0
    actual_inserted = 0

    try:
        total_staging = await step_1_count_staging_total(account_id, engine)
        existing_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        already_exist_in_main = await step_3_count_records_already_exist_in_main_table(account_id, engine)
        missing_customer_records = await step_count_missing_customers(account_id, engine)
        ready_to_insert = await step_4_count_records_to_insert(account_id, engine)

        if ready_to_insert == 0:
            logger.info(f"[process_customer_tags_custom] No records to insert - skipping insertion step")
        else:
            actual_inserted = await step_5_insert_new_records(account_id, engine)

        logger.info(
            f"[process_customer_tags_custom] SUCCESS for account_id={account_id}: "
            f"total_staging={total_staging}, already_exist={already_exist_in_main}, "
            f"missing_customer={missing_customer_records}, inserted={actual_inserted}"
        )

        return {
            "duplicates_removed": 0,
            "total_records": total_staging,
            "existing_records": existing_in_main,
            "already_exist_records": already_exist_in_main,
            "missing_customer_records": missing_customer_records,
            "ready_to_insert": ready_to_insert,
            "inserted_records": actual_inserted,
        }

    except Exception as e:
        logger.error(f"[process_customer_tags_custom] ERROR: Failed processing for account_id={account_id}: {e}")
        raise
