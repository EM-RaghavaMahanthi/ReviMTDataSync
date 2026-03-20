import logging
from sqlalchemy import text
from stg_db_services._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)

async def step_1_count_staging_total(account_id: str, location_id: int, engine):
    """
    Step 1: Count total entries in staging table
    """
    logger.info(f"[STEP 1] Counting total entries in mt_customers_details_dlk")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) as count FROM public.mt_customers_details_dlk"))
            total_count = result.fetchone()[0]
            
        logger.info(f"[STEP 1] SUCCESS: Total entries in staging table: {total_count}")
        return total_count
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count staging table entries: {e}")
        raise

async def step_2_count_null_mandatory_names(account_id: str, location_id: int, engine):
    """
    Step 2: Count entries with first_name or last_name as null in staging table
    """
    logger.info(f"[STEP 2] Counting entries with first_name or last_name as null in staging table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM public.mt_customers_details_dlk 
                WHERE account_id = :account_id 
                  AND location_id = :location_id 
                  AND (first_name IS NULL OR last_name IS NULL)
            """), {"account_id": account_id, "location_id": location_id})
            null_names_count = result.fetchone()[0]
            
        logger.info(f"[STEP 2] SUCCESS: Entries with null first_name or last_name: {null_names_count}")
        return null_names_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count null name entries: {e}")
        raise

async def step_3_count_existing_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 3: Count entries already in main customers table
    """
    logger.info(f"[STEP 3] Counting entries already in main customers table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM public.customers 
                WHERE account_id = :account_id 
                  AND location_id = :location_id
            """), {"account_id": account_id, "location_id": location_id})
            existing_count = result.fetchone()[0]
            
        logger.info(f"[STEP 3] SUCCESS: Entries already in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count existing entries in main table: {e}")
        raise

async def step_4_count_records_already_exist(account_id: str, location_id: int, engine):
    """
    Step 4: Count staging records that already exist in main table (duplicates)
    """
    logger.info(f"[STEP 4] Counting staging records that already exist in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_customers_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location_id = :location_id
                  AND stg.first_name IS NOT NULL
                  AND stg.last_name IS NOT NULL
                  AND stg.customer_id IN (
                    SELECT customer_id FROM customers 
                    WHERE account_id = :account_id AND location_id = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            duplicate_count = result.fetchone()[0]
            
        logger.info(f"[STEP 4] SUCCESS: Staging records already in main table (duplicates): {duplicate_count}")
        return duplicate_count
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count duplicate records: {e}")
        raise

async def step_5_count_duplicates_in_staging(account_id: str, location_id: int, engine):
    """
    Step 5: Count duplicate records within staging table itself (source duplicates from CRM)
    """
    logger.info(f"[STEP 5] Counting duplicate records within staging table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_eligible,
                    COUNT(DISTINCT customer_id) as unique_customers
                FROM mt_customers_details_dlk
                WHERE account_id = :account_id
                  AND location_id = :location_id
                  AND first_name IS NOT NULL
                  AND last_name IS NOT NULL
            """), {"account_id": account_id, "location_id": location_id})
            stats = result.fetchone()
            total_eligible = stats[0]
            unique_customers = stats[1]
            duplicate_in_stg = total_eligible - unique_customers
            
        logger.info(f"[STEP 5] SUCCESS: Duplicates within staging table: {duplicate_in_stg}")
        if duplicate_in_stg > 0:
            logger.info(f"[STEP 5] Source duplicate breakdown: {total_eligible} total eligible, {unique_customers} unique customers")
        return duplicate_in_stg
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to count staging duplicates: {e}")
        raise

async def step_6_count_records_to_insert(account_id: str, location_id: int, engine):
    """
    Step 6: Count DISTINCT staging records ready for insertion (using DISTINCT to match actual insert)
    """
    logger.info(f"[STEP 6] Counting DISTINCT staging records ready for insertion")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT customer_id) as count
                FROM mt_customers_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location_id = :location_id
                  AND stg.first_name IS NOT NULL
                  AND stg.last_name IS NOT NULL
                  AND stg.customer_id NOT IN (
                    SELECT customer_id FROM customers 
                    WHERE account_id = :account_id AND location_id = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            insert_ready_count = result.fetchone()[0]
            
        logger.info(f"[STEP 6] SUCCESS: DISTINCT records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to count records ready for insertion: {e}")
        raise

async def step_7_insert_new_records(account_id: str, location_id: int, engine):
    """
    Step 7: Insert new records from staging to main customers table
    Uses DISTINCT to handle potential source duplicates from CRM
    """
    logger.info(f"[STEP 7] Inserting new records into main customers table")
    
    try:
        # Insert with DISTINCT to handle source duplicates automatically
        insert_sql = text("""
            INSERT INTO public.customers (
              customer_id, location_id, account_id, first_name, last_name, email, full_name, birth_date, birth_month, birth_day,
              phone_number, address_line1, address_line2, address_line3, city, country, state_province,
              customer_state, postal_code, gender, date_joined, is_opted_in_to_sms, completed_class_count,
              state_id, created_at, created_by, updated_at, updated_by, deleted_at, deleted_by
            )
            SELECT DISTINCT
              customer_id, location_id, account_id, first_name, last_name, email, full_name, birth_date, birth_month, birth_day,
              phone_number, address_line1, address_line2, address_line3, city, country, state_province,
              customer_state, postal_code, gender, date_joined, is_opted_in_to_sms, completed_class_count,
              state_id, NOW() AS created_at, 1 AS created_by, NOW() AS updated_at, updated_by, deleted_at, deleted_by
            FROM mt_customers_details_dlk
            WHERE account_id = :account_id
              AND location_id = :location_id
              AND first_name IS NOT NULL
              AND last_name IS NOT NULL
              AND customer_id NOT IN (
                SELECT customer_id FROM customers WHERE account_id = :account_id AND location_id = :location_id
              )
        """)
        
        with engine.begin() as conn:
            result = conn.execute(insert_sql, {"account_id": account_id, "location_id": location_id})
            inserted_count = result.rowcount
        
        logger.info(f"[STEP 7] SUCCESS: Inserted {inserted_count} unique customer records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 7] ERROR: Failed to insert new records: {e}")
        raise

async def process_customers(account_id: str, location_id: int, engine):
    """
    Process customers from staging to final table.
    Executes the transformation in 7 detailed steps with comprehensive logging.
    """
    logger.info(f"[process_customers] Starting customer processing for account_id={account_id}, location_id={location_id}")
    
    # Initialize variables for error handling
    duplicates_found = 0
    duplicates_removed = 0
    total_staging = 0
    null_mandatory_names = 0
    existing_in_main = 0
    already_existing = 0
    duplicate_in_stg = 0
    ready_to_insert = 0
    actual_inserted = 0

    try:
        # Step 0: Drop duplicates in staging table
        try:
            step_0_result = drop_staging_duplicates(engine, "mt_customers_details_dlk", "customer_id", account_id, location_id)
            duplicates_found = step_0_result["duplicates_found"]
            duplicates_removed = step_0_result["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_customers] ERROR: Step 0 failed: {e}")
            raise

        # Step 1: Count total staging records
        try:
            total_staging = await step_1_count_staging_total(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_customers] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 2: Count null mandatory name records
        try:
            null_mandatory_names = await step_2_count_null_mandatory_names(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_customers] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Count existing records in main table
        try:
            existing_in_main = await step_3_count_existing_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_customers] ERROR: Step 3 failed: {e}")
            raise
        
        # Step 4: Count duplicates (staging records already in main)
        try:
            already_existing = await step_4_count_records_already_exist(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_customers] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Count duplicates within staging table
        try:
            duplicate_in_stg = await step_5_count_duplicates_in_staging(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_customers] ERROR: Step 5 failed: {e}")
            raise
        
        # Step 6: Count DISTINCT records ready for insertion
        try:
            ready_to_insert = await step_6_count_records_to_insert(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_customers] ERROR: Step 6 failed: {e}")
            raise
        
        # Step 7: Validation before insertion - counts should match exactly
        expected_ready = total_staging - null_mandatory_names - already_existing - duplicate_in_stg
        
        logger.info(f"[process_customers] Pre-insertion validation:")
        logger.info(f"   Expected ready to insert: {expected_ready} (total - invalid - already_existing - duplicate_in_stg)")
        logger.info(f"   Actual ready to insert: {ready_to_insert}")
        
        # Exact count validation - should match perfectly now
        if ready_to_insert != expected_ready:
            error_msg = f"Count mismatch! Expected {expected_ready}, got {ready_to_insert}. Formula: {total_staging} - {null_mandatory_names} - {already_existing} - {duplicate_in_stg} = {expected_ready}"
            logger.error(f"[process_customers] ERROR: {error_msg}")
            raise Exception(error_msg)
        
        logger.info(f"[process_customers] Perfect count match - proceeding with insertion")
        
        # Step 7: Insert new records (optimize for zero records)
        if ready_to_insert == 0:
            logger.info(f"[process_customers] No records to insert - skipping insertion step")
            actual_inserted = 0
        else:
            try:
                actual_inserted = await step_7_insert_new_records(account_id, location_id, engine)
            except Exception as e:
                logger.error(f"[process_customers] ERROR: Step 7 failed: {e}")
                raise
        
        # Summary logging
        logger.info(f"[process_customers] SUMMARY for account_id={account_id}, location_id={location_id}:")
        logger.info(f"   Duplicates found/removed in staging: {duplicates_found}/{duplicates_removed}")
        logger.info(f"   Total staging records: {total_staging}")
        logger.info(f"   Records with null mandatory names: {null_mandatory_names}")
        logger.info(f"   Existing records in main table: {existing_in_main}")
        logger.info(f"   Records already existing (duplicates): {already_existing}")
        logger.info(f"   Duplicates within staging table: {duplicate_in_stg}")
        logger.info(f"   Records ready for insertion: {ready_to_insert}")
        logger.info(f"   Actually inserted: {actual_inserted}")
        
        logger.info(f"[process_customers] SUCCESS: Completed customer processing for account_id={account_id}, location_id={location_id}")
        
        return {
            "duplicates_found": duplicates_found,
            "duplicates_removed": duplicates_removed,
            "total_records": total_staging,
            "null_mandatory_names": null_mandatory_names,
            "existing_records": existing_in_main,
            "already_existing_records": already_existing,
            "duplicate_in_stg_records": duplicate_in_stg,
            "ready_to_insert": ready_to_insert,
            "inserted_records": actual_inserted
        }
        
    except Exception as e:
        logger.error(f"[process_customers] ERROR: Failed processing for account_id={account_id}, location_id={location_id}: {e}")
        logger.error(f"[process_customers] ERROR: Error occurred during customer processing. Partial results:")
        logger.error(f"[process_customers]   Total staging records: {total_staging}")
        logger.error(f"[process_customers]   Records with null mandatory names: {null_mandatory_names}")
        logger.error(f"[process_customers]   Existing records in main table: {existing_in_main}")
        logger.error(f"[process_customers]   Records already existing (duplicates): {already_existing}")
        logger.error(f"[process_customers]   Duplicates within staging table: {duplicate_in_stg}")
        logger.error(f"[process_customers]   Records ready for insertion: {ready_to_insert}")
        logger.error(f"[process_customers]   Actually inserted: {actual_inserted}")
        raise
