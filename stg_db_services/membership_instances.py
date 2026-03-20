import logging
from sqlalchemy import text
from stg_db_services._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)

async def step_1_count_staging_total(account_id: str, location_id: int, engine):
    """
    Step 1: Count total entries in staging table
    """
    logger.info(f"[STEP 1] Counting total entries in mt_membership_instances_details_dlk")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("SELECT COUNT(*) as count FROM public.mt_membership_instances_details_dlk"))
            total_count = result.fetchone()[0]
            
        logger.info(f"[STEP 1] SUCCESS: Total entries in staging table: {total_count}")
        return total_count
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count staging table entries: {e}")
        raise

async def step_2_count_existing_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 2: Count entries already in main membership_instances table
    """
    logger.info(f"[STEP 2] Counting entries already in main membership_instances table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM public.membership_instances 
                WHERE account_id = :account_id 
                  AND location = :location_id
            """), {"account_id": account_id, "location_id": location_id})
            existing_count = result.fetchone()[0]
            
        logger.info(f"[STEP 2] SUCCESS: Entries already in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing entries in main table: {e}")
        raise

async def step_3_count_records_already_exist_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 3: Count staging records that already exist in main table
    """
    logger.info(f"[STEP 3] Counting staging records that already exist in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_membership_instances_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.membership_instances_id IN (
                    SELECT membership_instances_id FROM membership_instances 
                    WHERE account_id = :account_id AND location = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            duplicate_count = result.fetchone()[0]
            
        logger.info(f"[STEP 3] SUCCESS: Staging records already in main table (duplicates): {duplicate_count}")
        return duplicate_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count duplicate records: {e}")
        raise

async def step_4_count_records_to_insert(account_id: str, location_id: int, engine):
    """
    Step 4: Count staging records that are NOT in main table (ready to insert)
    """
    logger.info(f"[STEP 4] Counting staging records ready for insertion")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_membership_instances_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.membership_instances_id NOT IN (
                    SELECT membership_instances_id FROM membership_instances 
                    WHERE account_id = :account_id AND location = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            insert_ready_count = result.fetchone()[0]
            
        logger.info(f"[STEP 4] SUCCESS: Records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count records ready for insertion: {e}")
        raise

async def step_5_insert_new_records(account_id: str, location_id: int, engine):
    """
    Step 5: Insert new records from staging to main membership_instances table
    """
    logger.info(f"[STEP 5] Inserting new records into main membership_instances table")
    
    try:
        insert_sql = text("""
            INSERT INTO public.membership_instances (
              membership_instances_id, purchase_date, membership_name, renewal_rate_incl_tax, status,
              location, renewal_count, next_charge_date, created_at, created_by, updated_at,
              updated_by, deleted_at, deleted_by, account_id
            )
            SELECT
              membership_instances_id, purchase_date, membership_name, renewal_rate_incl_tax, status,
              location, renewal_count, next_charge_date, NOW() AS created_at, 1 AS created_by, NOW() AS updated_at,
              updated_by, deleted_at, deleted_by, account_id
            FROM mt_membership_instances_details_dlk
            WHERE account_id = :account_id
              AND location = :location_id
              AND membership_instances_id NOT IN (
                SELECT membership_instances_id FROM membership_instances 
                WHERE account_id = :account_id AND location = :location_id
              )
        """)
        
        with engine.begin() as conn:
            result = conn.execute(insert_sql, {"account_id": account_id, "location_id": location_id})
            inserted_count = result.rowcount
            
        logger.info(f"[STEP 5] SUCCESS: Successfully inserted {inserted_count} new membership_instances records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to insert new records: {e}")
        raise

async def process_membership_instances(account_id: str, location_id: int, engine):
    """
    Process membership_instances from staging to final table.
    Executes the transformation in 6 detailed steps with comprehensive logging.
    Step 0: Drop duplicates in staging table to clean data
    """
    logger.info(f"[process_membership_instances] Starting membership_instances processing for account_id={account_id}, location_id={location_id}")
    
    # Initialize variables for error handling
    duplicates_removed = 0
    total_staging = 0
    existing_in_main = 0
    already_exist_in_main = 0
    ready_to_insert = 0
    actual_inserted = 0
    
    try:
        # Step 0: Drop duplicates in staging table
        try:
            dup_stats = drop_staging_duplicates(engine, "mt_membership_instances_details_dlk", "membership_instances_id", account_id, location_id)
            duplicates_removed = dup_stats["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_membership_instances] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Count total staging records (after duplicate removal)
        try:
            total_staging = await step_1_count_staging_total(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_instances] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 2: Count existing records in main table
        try:
            existing_in_main = await step_2_count_existing_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_instances] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Count records already exist in main table
        try:
            already_exist_in_main = await step_3_count_records_already_exist_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_instances] ERROR: Step 3 failed: {e}")
            raise
        
        # Step 4: Count records ready for insertion
        try:
            ready_to_insert = await step_4_count_records_to_insert(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_instances] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Simple validation before insertion
        expected_ready = total_staging - already_exist_in_main
        
        logger.info(f"[process_membership_instances] Pre-insertion validation:")
        logger.info(f"   Expected ready to insert: {expected_ready} (total - already_exist_in_main)")
        logger.info(f"   Actual ready to insert: {ready_to_insert}")
        
        if ready_to_insert != expected_ready:
            error_msg = f"Count mismatch! Expected {expected_ready}, got {ready_to_insert}. Manual review required."
            logger.error(f"[process_membership_instances] ERROR: {error_msg}")
            raise Exception(error_msg)
        
        logger.info(f"[process_membership_instances] Validation passed - proceeding with insertion")
        
        # Step 5: Insert new records (optimize for zero records)
        if ready_to_insert == 0:
            logger.info(f"[process_membership_instances] No records to insert - skipping insertion step")
            actual_inserted = 0
        else:
            try:
                actual_inserted = await step_5_insert_new_records(account_id, location_id, engine)
            except Exception as e:
                logger.error(f"[process_membership_instances] ERROR: Step 5 failed: {e}")
                raise
        
        # Summary logging
        logger.info(f"[process_membership_instances] SUMMARY for account_id={account_id}, location_id={location_id}:")
        logger.info(f"   Duplicates removed from staging: {duplicates_removed}")
        logger.info(f"   Total staging records (after cleanup): {total_staging}")
        logger.info(f"   Existing records in main table: {existing_in_main}")
        logger.info(f"   Records already exist in main: {already_exist_in_main}")
        logger.info(f"   Records ready for insertion: {ready_to_insert}")
        logger.info(f"   Actually inserted: {actual_inserted}")
        
        logger.info(f"[process_membership_instances] SUCCESS: Completed membership_instances processing for account_id={account_id}, location_id={location_id}")
        
        return {
            "duplicates_removed": duplicates_removed,
            "total_records": total_staging,
            "existing_records": existing_in_main,
            "already_exist_records": already_exist_in_main,
            "ready_to_insert": ready_to_insert,
            "inserted_records": actual_inserted
        }
        
    except Exception as e:
        logger.error(f"[process_membership_instances] ERROR: Failed processing for account_id={account_id}, location_id={location_id}: {e}")
        logger.error(f"[process_membership_instances] ERROR: Error occurred during membership_instances processing. Partial results:")
        logger.error(f"[process_membership_instances]   Duplicates removed from staging: {duplicates_removed}")
        logger.error(f"[process_membership_instances]   Total staging records (after cleanup): {total_staging}")
        logger.error(f"[process_membership_instances]   Existing records in main table: {existing_in_main}")
        logger.error(f"[process_membership_instances]   Records already exist in main: {already_exist_in_main}")
        logger.error(f"[process_membership_instances]   Records ready for insertion: {ready_to_insert}")
        logger.error(f"[process_membership_instances]   Actually inserted: {actual_inserted}")
        raise
