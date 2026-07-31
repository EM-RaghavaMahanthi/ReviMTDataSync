import logging
from sqlalchemy import text
from stg_to_main_bulk._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)

async def step_1_count_staging_total(account_id: str, engine):
    """
    Step 1: Count total entries in staging table
    """
    logger.info(f"[STEP 1] Counting total entries in stg_class_sessions_bulk")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) as count FROM public.stg_class_sessions_bulk"))
            total_count = result.fetchone()[0]
            
        logger.info(f"[STEP 1] SUCCESS: Total entries in staging table: {total_count}")
        return total_count
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count staging table entries: {e}")
        raise

async def step_2_count_existing_in_main_table(account_id: str, engine):
    """
    Step 2: Count entries already in main class_sessions table
    """
    logger.info(f"[STEP 2] Counting entries already in main class_sessions table")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM public.class_sessions 
                WHERE account_id = :account_id 
            """), {"account_id": account_id})
            existing_count = result.fetchone()[0]
            
        logger.info(f"[STEP 2] SUCCESS: Entries already in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing entries in main table: {e}")
        raise

async def step_3_count_records_already_exist_in_main_table(account_id: str, engine):
    """
    Step 3: Count staging records that already exist in main table
    """
    logger.info(f"[STEP 3] Counting staging records that already exist in main table")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_class_sessions_bulk stg
                WHERE stg.account_id = :account_id
                  AND stg.class_session_id IN (
                    SELECT class_session_id FROM class_sessions 
                    WHERE account_id = :account_id
                  )
            """), {"account_id": account_id})
            duplicate_count = result.fetchone()[0]
            
        logger.info(f"[STEP 3] SUCCESS: Staging records already in main table (duplicates): {duplicate_count}")
        return duplicate_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count duplicate records: {e}")
        raise

async def step_4_count_records_to_insert(account_id: str, engine):
    """
    Step 4: Count staging records that are NOT in main table (ready to insert)
    """
    logger.info(f"[STEP 4] Counting staging records ready for insertion")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_class_sessions_bulk stg
                WHERE stg.account_id = :account_id
                  AND stg.class_session_id NOT IN (
                    SELECT class_session_id FROM class_sessions 
                    WHERE account_id = :account_id
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
    Step 5: Insert new records from staging to main class_sessions table
    """
    logger.info(f"[STEP 5] Inserting new records into main class_sessions table")
    
    try:
        insert_sql = text("""
            INSERT INTO public.class_sessions (
              class_session_id, start_datetime, start_date, location, end_datetime, cancellation_datetime,
              class_name, class_type_name, capacity,
              created_at, created_by, updated_at, updated_by, deleted_at, deleted_by, account_id
            )
            SELECT
              class_session_id, start_datetime, start_date, location, end_datetime, cancellation_datetime,
              class_name, class_type_name, capacity,
              NOW() AS created_at, 1 AS created_by, NOW() AS updated_at, updated_by, deleted_at, deleted_by, account_id
            FROM stg_class_sessions_bulk
            WHERE account_id = :account_id
              AND class_session_id NOT IN (
                SELECT class_session_id FROM class_sessions WHERE account_id = :account_id
              )
        """)
        
        with engine.begin() as conn:
            result = conn.execute(insert_sql, {"account_id": account_id})
            inserted_count = result.rowcount
            
        logger.info(f"[STEP 5] SUCCESS: Successfully inserted {inserted_count} new class_sessions records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to insert new records: {e}")
        raise

async def process_class_sessions(account_id: str, engine):
    """
    Process class_sessions from staging to final table.
    Executes the transformation in 6 detailed steps with comprehensive logging.
    Step 0: Drop duplicates in staging table to clean data
    """
    logger.info(f"[process_class_sessions] Starting class_sessions processing for account_id={account_id}")
    
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
            dup_stats = drop_staging_duplicates(engine, "stg_class_sessions_bulk", "class_session_id", account_id)
            duplicates_removed = dup_stats["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_class_sessions] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Count total staging records (after duplicate removal)
        try:
            total_staging = await step_1_count_staging_total(account_id, engine)
        except Exception as e:
            logger.error(f"[process_class_sessions] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 2: Count existing records in main table
        try:
            existing_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        except Exception as e:
            logger.error(f"[process_class_sessions] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Count records already exist in main table
        try:
            already_exist_in_main = await step_3_count_records_already_exist_in_main_table(account_id, engine)
        except Exception as e:
            logger.error(f"[process_class_sessions] ERROR: Step 3 failed: {e}")
            raise
        
        # Step 4: Count records ready for insertion
        try:
            ready_to_insert = await step_4_count_records_to_insert(account_id, engine)
        except Exception as e:
            logger.error(f"[process_class_sessions] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Simple validation before insertion
        expected_ready = total_staging - already_exist_in_main
        
        logger.info(f"[process_class_sessions] Pre-insertion validation:")
        logger.info(f"   Expected ready to insert: {expected_ready} (total - already_exist_in_main)")
        logger.info(f"   Actual ready to insert: {ready_to_insert}")
        
        if ready_to_insert != expected_ready:
            error_msg = f"Count mismatch! Expected {expected_ready}, got {ready_to_insert}. Manual review required."
            logger.error(f"[process_class_sessions] ERROR: {error_msg}")
            raise Exception(error_msg)
        
        logger.info(f"[process_class_sessions] Validation passed - proceeding with insertion")
        
        # Step 5: Insert new records (optimize for zero records)
        if ready_to_insert == 0:
            logger.info(f"[process_class_sessions] No records to insert - skipping insertion step")
            actual_inserted = 0
        else:
            try:
                actual_inserted = await step_5_insert_new_records(account_id, engine)
            except Exception as e:
                logger.error(f"[process_class_sessions] ERROR: Step 5 failed: {e}")
                raise
        
        # Summary logging
        logger.info(f"[process_class_sessions] SUMMARY for account_id={account_id}:")
        logger.info(f"   Duplicates removed from staging: {duplicates_removed}")
        logger.info(f"   Total staging records (after cleanup): {total_staging}")
        logger.info(f"   Existing records in main table: {existing_in_main}")
        logger.info(f"   Records already exist in main: {already_exist_in_main}")
        logger.info(f"   Records ready for insertion: {ready_to_insert}")
        logger.info(f"   Actually inserted: {actual_inserted}")
        
        logger.info(f"[process_class_sessions] SUCCESS: Completed class_sessions processing for account_id={account_id}")
        
        return {
            "duplicates_removed": duplicates_removed,
            "total_records": total_staging,
            "existing_records": existing_in_main,
            "already_exist_records": already_exist_in_main,
            "ready_to_insert": ready_to_insert,
            "inserted_records": actual_inserted
        }
        
    except Exception as e:
        logger.error(f"[process_class_sessions] ERROR: Failed processing for account_id={account_id}: {e}")
        logger.error(f"[process_class_sessions] ERROR: Error occurred during class_sessions processing. Partial results:")
        logger.error(f"[process_class_sessions]   Duplicates removed from staging: {duplicates_removed}")
        logger.error(f"[process_class_sessions]   Total staging records (after cleanup): {total_staging}")
        logger.error(f"[process_class_sessions]   Existing records in main table: {existing_in_main}")
        logger.error(f"[process_class_sessions]   Records already exist in main: {already_exist_in_main}")
        logger.error(f"[process_class_sessions]   Records ready for insertion: {ready_to_insert}")
        logger.error(f"[process_class_sessions]   Actually inserted: {actual_inserted}")
        raise
