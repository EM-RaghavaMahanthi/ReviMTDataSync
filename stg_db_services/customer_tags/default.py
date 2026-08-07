"""
customer_tags_default — ALL tag definitions (both tag_type='system' and tag_type='manual'),
shared per-tenant, NOT per-account: no account_id column, keyed by (tenant_name, crm_tag_id).

crm_tag_id (the raw MT tag id) is the natural key, not name — name could theoretically change
while the MT tag id stays the same. No @@unique constraint exists on this table in the Prisma
schema either — idempotency is enforced entirely here via NOT EXISTS / DISTINCT, not a DB
constraint.

customer_tags_custom is no longer populated by this sync — every tag definition, regardless of
MT's tag_type, lives here now. customer_tag_assignments always resolves default_tag_id for
CRM-sourced tags (custom_tag_id stays NULL).
"""

import logging
from sqlalchemy import text
from stg_db_services._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)

_TABLE = "customer_tags_default"


async def step_1_count_staging_total(account_id: str, engine):
    """Step 1: Count tag rows in staging for this account."""
    logger.info(f"[STEP 1] Counting tag rows in mt_user_tags_details_dlk")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count FROM mt_user_tags_details_dlk
                WHERE account_id = :account_id
            """), {"account_id": account_id})
            total_count = result.fetchone()[0]
        logger.info(f"[STEP 1] SUCCESS: Total tag rows in staging: {total_count}")
        return total_count
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count staging table entries: {e}")
        raise


async def step_2_count_existing_in_main_table(account_id: str, engine):
    """Step 2: Count customer_tags_default rows already present for this account's tenant(s)."""
    logger.info(f"[STEP 2] Counting entries already in main customer_tags_default table")
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) as count
                FROM {_TABLE} d
                WHERE d.tenant_name IN (
                  SELECT DISTINCT tenant_name FROM mt_user_tags_details_dlk
                  WHERE account_id = :account_id
                )
            """), {"account_id": account_id})
            existing_count = result.fetchone()[0]
        logger.info(f"[STEP 2] SUCCESS: Entries already in main table for this tenant: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing entries in main table: {e}")
        raise


async def step_3_count_records_already_exist_in_main_table(account_id: str, engine):
    """Step 3: Count staging rows whose (tenant_name, crm_tag_id) already exists in customer_tags_default."""
    logger.info(f"[STEP 3] Counting staging records that already exist in main table")
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) as count
                FROM mt_user_tags_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND EXISTS (
                    SELECT 1 FROM {_TABLE} d
                    WHERE d.tenant_name = stg.tenant_name AND d.crm_tag_id = stg.tag_id
                  )
            """), {"account_id": account_id})
            duplicate_count = result.fetchone()[0]
        logger.info(f"[STEP 3] SUCCESS: Staging records already in main table (duplicates): {duplicate_count}")
        return duplicate_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count duplicate records: {e}")
        raise


async def step_3b_count_null_names(account_id: str, engine):
    """
    Step 3b: Count staging rows where tag name is NULL (MarianaTek sent no `name`
    attribute) — customer_tags_default.name is NOT NULL, so these are excluded by
    step_4/step_5's filter rather than crashing the whole insert batch.
    """
    logger.info(f"[STEP 3b] Counting staging rows with null tag name")
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_user_tags_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.name IS NULL
            """), {"account_id": account_id})
            null_count = result.fetchone()[0]
        if null_count > 0:
            logger.warning(f"[STEP 3b] {null_count} staging tags have null name — will be skipped")
        logger.info(f"[STEP 3b] SUCCESS: Records with null name: {null_count}")
        return null_count
    except Exception as e:
        logger.error(f"[STEP 3b] ERROR: Failed to count null-name records: {e}")
        raise


async def step_4_count_records_to_insert(account_id: str, engine):
    """Step 4: Count staging rows ready to insert (distinct (tenant_name, crm_tag_id), not already present)."""
    logger.info(f"[STEP 4] Counting staging records ready for insertion")
    try:
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(DISTINCT (stg.tenant_name, stg.tag_id)) as count
                FROM mt_user_tags_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.name IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM {_TABLE} d
                    WHERE d.tenant_name = stg.tenant_name AND d.crm_tag_id = stg.tag_id
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
    Step 5: Insert new customer_tags_default rows. DISTINCT guards against two different
    staging rows sharing the same (tenant_name, crm_tag_id) within this same insert batch
    (no DB unique constraint enforces this, unlike the other tag/note tables).
    No updated_at/updated_by on this table (unlike customer_notes) — not included.
    """
    logger.info(f"[STEP 5] Inserting new records into main customer_tags_default table")
    try:
        insert_sql = text(f"""
            INSERT INTO {_TABLE} (
              name, tenant_name, crm_tag_id, crm_tag_description,
              created_at, created_by, deleted_at, deleted_by
            )
            SELECT DISTINCT
              stg.name, stg.tenant_name, stg.tag_id AS crm_tag_id, stg.description AS crm_tag_description,
              NOW() AS created_at, 1 AS created_by, NULL::timestamp, NULL::integer
            FROM mt_user_tags_details_dlk stg
            WHERE stg.account_id = :account_id
              AND stg.name IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM {_TABLE} d
                WHERE d.tenant_name = stg.tenant_name AND d.crm_tag_id = stg.tag_id
              )
        """)
        with engine.begin() as conn:
            result = conn.execute(insert_sql, {"account_id": account_id})
            inserted_count = result.rowcount
        logger.info(f"[STEP 5] SUCCESS: Successfully inserted {inserted_count} new customer_tags_default records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to insert new records: {e}")
        raise


async def process_customer_tags_default(account_id: str, engine, write: bool = True):
    """Process all tags from mt_user_tags_details_dlk into customer_tags_default."""
    logger.info(f"[process_customer_tags_default] Starting for account_id={account_id}")

    duplicates_removed = 0
    total_staging = 0
    existing_in_main = 0
    already_exist_in_main = 0
    null_name_records = 0
    ready_to_insert = 0
    actual_inserted = 0

    try:
        # Dedup staging on tag_id (the natural per-account key) — NOT (tenant_name, crm_tag_id),
        # which is enforced separately below since it's a cross-account concept.
        dup_stats = drop_staging_duplicates(engine, "mt_user_tags_details_dlk", "tag_id", account_id)
        duplicates_removed = dup_stats["duplicates_removed"]

        total_staging = await step_1_count_staging_total(account_id, engine)
        existing_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        already_exist_in_main = await step_3_count_records_already_exist_in_main_table(account_id, engine)
        null_name_records = await step_3b_count_null_names(account_id, engine)
        ready_to_insert = await step_4_count_records_to_insert(account_id, engine)

        if ready_to_insert == 0:
            logger.info(f"[process_customer_tags_default] No records to insert - skipping insertion step")
        else:
            if write:
                actual_inserted = await step_5_insert_new_records(account_id, engine)
            else:
                logger.warning(
                    f"[process_customer_tags_default] WRITE DISABLED — {ready_to_insert} rows "
                    f"would have been inserted into customer_tags_default; inserting nothing"
                )

        logger.info(
            f"[process_customer_tags_default] SUCCESS for account_id={account_id}: "
            f"duplicates_removed={duplicates_removed}, total_staging={total_staging}, "
            f"already_exist={already_exist_in_main}, null_name={null_name_records}, inserted={actual_inserted}"
        )

        return {
            "duplicates_removed": duplicates_removed,
            "total_records": total_staging,
            "existing_records": existing_in_main,
            "already_exist_records": already_exist_in_main,
            "null_name_records": null_name_records,
            "ready_to_insert": ready_to_insert,
            "inserted_records": actual_inserted,
        }

    except Exception as e:
        logger.error(f"[process_customer_tags_default] ERROR: Failed processing for account_id={account_id}: {e}")
        raise
