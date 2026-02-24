"""
Credit Transactions Orders Service - Step 4a of ETL Pipeline
Processes credit_transactions data from staging to credit_transactions_orders table with customer dependency validation
Dependencies: customers table only
"""

import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def step_0_drop_duplicates_in_staging(account_id: str, location_id: int, engine):
    """
    Step 0: Drop duplicate records in staging table (mt_credit_transactions_details_dlk) to clean data
    """
    logger.info(f"[STEP 0] Dropping duplicate records in mt_credit_transactions_details_dlk staging table")
    
    try:
        with engine.begin() as conn:
            # First count duplicates before removal
            count_result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT credit_transactions_id) as unique_transactions
                FROM mt_credit_transactions_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
            """), {"account_id": account_id, "location_id": location_id})
            stats = count_result.fetchone()
            total_before = stats[0]
            unique_transactions = stats[1]
            duplicates_found = total_before - unique_transactions
            
            if duplicates_found > 0:
                logger.info(f"[STEP 0] Found {duplicates_found} duplicate records to remove ({total_before} total, {unique_transactions} unique)")
                
                # Delete duplicates, keeping only one record per credit_transactions_id
                result = conn.execute(text("""
                    DELETE FROM mt_credit_transactions_details_dlk 
                    WHERE ctid NOT IN (
                        SELECT DISTINCT ON (credit_transactions_id) ctid
                        FROM mt_credit_transactions_details_dlk
                        WHERE account_id = :account_id
                          AND location = :location_id
                        ORDER BY credit_transactions_id, ctid
                    )
                    AND account_id = :account_id
                    AND location = :location_id
                """), {"account_id": account_id, "location_id": location_id})
                deleted_count = result.rowcount
                
                logger.info(f"[STEP 0] SUCCESS: Removed {deleted_count} duplicate records from staging table")
            else:
                logger.info(f"[STEP 0] SUCCESS: No duplicates found in staging table")
                deleted_count = 0
                
        return {
            "duplicates_found": duplicates_found,
            "duplicates_removed": deleted_count
        }
    except Exception as e:
        logger.error(f"[STEP 0] ERROR: Failed to drop duplicates in staging table: {e}")
        raise

async def step_1_count_total_records(account_id: str, location_id: int, engine):
    """
    Step 1: Count total credit_transactions records in staging
    """
    logger.info(f"[STEP 1] Counting total credit_transactions records in staging")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_credit_transactions_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
            """), {"account_id": account_id, "location_id": location_id})
            total_count = result.fetchone()[0]
        
        logger.info(f"[STEP 1] SUCCESS: Total credit_transactions records: {total_count}")
        return total_count
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count total credit_transactions records: {e}")
        raise

async def step_1b_count_unnecessary_records(account_id: str, location_id: int, engine):
    """
    Step 1b: Count unnecessary records (isin_order_line = false) that won't be processed by orders service
    """
    logger.info(f"[STEP 1b] Counting unnecessary records (isin_order_line = false)")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_credit_transactions_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
                  AND isin_order_line = false
            """), {"account_id": account_id, "location_id": location_id})
            unnecessary_count = result.fetchone()[0]
        
        logger.info(f"[STEP 1b] SUCCESS: Unnecessary records (isin_order_line = false): {unnecessary_count}")
        return unnecessary_count
    except Exception as e:
        logger.error(f"[STEP 1b] ERROR: Failed to count unnecessary records: {e}")
        raise

async def step_2_count_existing_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 2: Count entries already in main credit_transactions_orders table
    """
    logger.info(f"[STEP 2] Counting entries already in main credit_transactions_orders table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM public.credit_transactions_orders 
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
    Step 3: Count staging records that already exist in main table (only order_line related)
    """
    logger.info(f"[STEP 3] Counting staging records that already exist in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_credit_transactions_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_order_line = true
                  AND stg.credit_transactions_id IN (
                    SELECT credit_transactions_id FROM credit_transactions_orders 
                    WHERE account_id = :account_id AND location = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            already_exist_count = result.fetchone()[0]
        
        logger.info(f"[STEP 3] SUCCESS: Records already exist in main table: {already_exist_count}")
        return already_exist_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count records already exist in main table: {e}")
        raise

async def step_4_count_credit_transactions_with_missing_customers(account_id: str, location_id: int, engine):
    """
    Step 4: Count credit_transactions records with missing customer dependencies (invalid for insertion)
    OPTIMIZED: Single query with LEFT JOIN instead of 4 queries with NOT EXISTS
    """
    logger.info(f"[STEP 4] Counting credit_transactions records with missing customer dependencies")
    
    try:
        with engine.begin() as conn:  # ✅ Single connection for all queries
            # ✅ OPTIMIZED: Single query with LEFT JOIN gets ALL metrics at once!
            result = conn.execute(text("""
                WITH missing_customers AS (
                    SELECT 
                        stg.customer_id,
                        COUNT(*) as transaction_count
                    FROM mt_credit_transactions_details_dlk stg
                    LEFT JOIN customers c ON c.customer_id = stg.customer_id 
                        AND c.account_id = stg.account_id
                        AND c.location_id = :location_id
                    WHERE stg.account_id = :account_id
                      AND stg.location = :location_id
                      AND stg.isin_order_line = true
                      AND c.customer_id IS NULL  -- Customer not found
                    GROUP BY stg.customer_id
                )
                SELECT 
                    COALESCE(SUM(transaction_count), 0) as total_missing_transactions,
                    COUNT(DISTINCT customer_id) as distinct_missing_customers,
                    COALESCE(SUM(CASE WHEN customer_id IS NULL THEN transaction_count ELSE 0 END), 0) as null_customer_transactions,
                    (SELECT json_agg(json_build_object('customer_id', customer_id, 'transaction_count', transaction_count))
                     FROM (
                        SELECT customer_id, transaction_count 
                        FROM missing_customers 
                        ORDER BY transaction_count DESC 
                        LIMIT 10
                    ) t) as sample_missing_customers
                FROM missing_customers
            """), {"account_id": account_id, "location_id": location_id})
            
            row = result.fetchone()
            missing_customer_transactions = int(row[0])
            distinct_missing_customers = int(row[1])
            null_customer_transactions = int(row[2])
            sample_missing_customers = row[3]  # JSON array
            
        # Log details about missing customers if any
        if missing_customer_transactions > 0:
            logger.warning(f"[STEP 4] Found {missing_customer_transactions} credit_transactions records with missing customers")
            logger.warning(f"[STEP 4] Found {distinct_missing_customers} distinct missing customers")
            logger.warning(f"[STEP 4] Found {null_customer_transactions} credit_transactions records with NULL customer_id")
            
            # Parse and log sample (already fetched from main query)
            if sample_missing_customers:
                import json
                missing_details = [(item['customer_id'], item['transaction_count']) 
                   for item in sample_missing_customers]
                logger.warning(f"[STEP 4] Sample missing customers with transaction counts: {missing_details}")
        
        logger.info(f"[STEP 4] SUCCESS: Credit_transactions records with missing customers: {missing_customer_transactions}")
        logger.info(f"[STEP 4] SUCCESS: Distinct missing customers: {distinct_missing_customers}")
        logger.info(f"[STEP 4] SUCCESS: Credit_transactions records with NULL customer_id: {null_customer_transactions}")
        return missing_customer_transactions
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count credit_transactions with missing customer dependencies: {e}")
        raise

async def step_4_calculate_expected_ready(account_id: str, location_id: int, total_records: int, duplicate_records: int, invalid_customer_records: int):
    """
    Step 4: Calculate expected records ready for insertion
    Formula: expected_ready = total - duplicates - invalid_customers
    """
    logger.info(f"[STEP 4] Calculating expected records ready for insertion")
    
    expected_ready = total_records - duplicate_records - invalid_customer_records
    
    logger.info(f"[STEP 4] SUCCESS: Expected ready for insertion: {expected_ready}")
    logger.info(f"[STEP 4] Breakdown: Total({total_records}) - Duplicates({duplicate_records}) - Invalid_Customers({invalid_customer_records}) = {expected_ready}")
    
    return expected_ready

async def step_5_count_records_to_insert(account_id: str, location_id: int, engine):
    """
    Step 5: Count records that are actually ready for insertion (validation before insert)
    This should match the expected_ready count - if not, there's a logic error
    """
    logger.info(f"[STEP 5] Counting records ready for insertion (validation)")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_credit_transactions_details_dlk mt
                INNER JOIN customers c 
                ON mt.customer_id = c.customer_id  
                AND mt.account_id = c.account_id
                WHERE mt.account_id = :account_id 
                AND mt.location = :location_id
                AND mt.isin_order_line = true
                AND mt.credit_transactions_id NOT IN (SELECT credit_transactions_id FROM public.credit_transactions_orders WHERE account_id = :account_id AND location = :location_id)
            """), {"account_id": account_id, "location_id": location_id})
            insert_ready_count = result.fetchone()[0]
            
        logger.info(f"[STEP 5] SUCCESS: Records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to count records ready for insertion: {e}")
        raise

async def step_6_insert_new_records(account_id: str, location_id: int, engine):
    """
    Step 6: Insert new credit_transactions records into credit_transactions_orders table
    """
    logger.info(f"[STEP 6] Inserting new credit_transactions records")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO public.credit_transactions_orders(
                  credit_transactions_id, transaction_date, credit_name, is_expired, is_intro_offer,
                  parent_credit_transaction_type, parent_credit_transaction_id, customer_id, location,
                  created_at, created_by, updated_at, updated_by, deleted_at, deleted_by,
                  account_id, customer_ref_id, remaining_credits_cache
                )
                SELECT  
                  mt.credit_transactions_id, mt.transaction_date, mt.credit_name, mt.is_expired, mt.is_intro_offer,
                  mt.parent_credit_transaction_type, mt.parent_credit_transaction_id, mt.customer_id, mt.location,
                  now() as created_at, 1 as  created_by, now() as updated_at, mt.updated_by, mt.deleted_at, mt.deleted_by,
                  mt.account_id, c.id as customer_ref_id, mt.remaining_credits_cache
                FROM mt_credit_transactions_details_dlk mt
                INNER JOIN customers c 
                ON mt.customer_id = c.customer_id  
                AND mt.account_id = c.account_id
                WHERE mt.account_id = :account_id 
                AND mt.location = :location_id
                AND mt.isin_order_line = true
                AND mt.credit_transactions_id NOT IN (SELECT credit_transactions_id FROM public.credit_transactions_orders WHERE account_id = :account_id AND location = :location_id)
            """), {"account_id": account_id, "location_id": location_id})
            inserted_count = result.rowcount
            
        logger.info(f"[STEP 6] SUCCESS: Inserted {inserted_count} credit_transactions records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to insert credit_transactions records: {e}")
        raise

async def process_credit_transactions_orders(account_id: str, location_id: int, engine):
    """
    Process credit_transactions from staging to credit_transactions_orders table.
    Executes the transformation in 7 detailed steps with comprehensive logging.
    Step 0: Drop duplicates in staging table to clean data
    Handles customer table dependencies and validates data integrity.
    """
    logger.info(f"[process_credit_transactions_orders] Starting processing for account_id={account_id}, location_id={location_id}")
    
    # Initialize variables for error handling
    duplicates_removed = 0
    total_staging = 0
    unnecessary_records = 0
    existing_in_main = 0
    already_exist_in_main = 0
    missing_customer_transactions = 0
    ready_to_insert = 0
    actual_inserted = 0
    
    try:
        # Step 0: Drop duplicates in staging table
        try:
            dup_stats = await step_0_drop_duplicates_in_staging(account_id, location_id, engine)
            duplicates_removed = dup_stats["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_credit_transactions_orders] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Count total staging records (after duplicate removal)
        try:
            total_staging = await step_1_count_total_records(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_credit_transactions_orders] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 1b: Count unnecessary records (isin_order_line = false)
        try:
            unnecessary_records = await step_1b_count_unnecessary_records(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_credit_transactions_orders] ERROR: Step 1b failed: {e}")
            raise
        
        # Step 2: Count existing records in main table
        try:
            existing_in_main = await step_2_count_existing_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_credit_transactions_orders] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Count records already exist in main table
        try:
            already_exist_in_main = await step_3_count_records_already_exist_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_credit_transactions_orders] ERROR: Step 3 failed: {e}")
            raise
        
        # Step 4: Count records with missing customer dependencies
        try:
            missing_customer_transactions = await step_4_count_credit_transactions_with_missing_customers(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_credit_transactions_orders] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Count records ready for insertion
        try:
            ready_to_insert = await step_5_count_records_to_insert(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_credit_transactions_orders] ERROR: Step 5 failed: {e}")
            raise
        
        # Step 6: Simple validation before insertion (with unnecessary records filtered out)
        expected_ready = total_staging - unnecessary_records - missing_customer_transactions - already_exist_in_main
        
        logger.info(f"[process_credit_transactions_orders] Pre-insertion validation:")
        logger.info(f"   Expected ready to insert: {expected_ready} (total - unnecessary - missing_customers - already_exist_in_main)")
        logger.info(f"   Actual ready to insert: {ready_to_insert}")
        
        if ready_to_insert != expected_ready:
            error_msg = f"Count mismatch! Expected {expected_ready}, got {ready_to_insert}. Manual review required."
            logger.error(f"[process_credit_transactions_orders] ERROR: {error_msg}")
            raise Exception(error_msg)
        
        logger.info(f"[process_credit_transactions_orders] Validation passed - proceeding with insertion")
        
        # Step 7: Insert new records (optimize for zero records)
        if ready_to_insert == 0:
            logger.info(f"[process_credit_transactions_orders] No records to insert - skipping insertion step")
            actual_inserted = 0
        else:
            try:
                actual_inserted = await step_6_insert_new_records(account_id, location_id, engine)
            except Exception as e:
                logger.error(f"[process_credit_transactions_orders] ERROR: Step 7 failed: {e}")
                raise
           
        # Summary logging
        logger.info(f"[process_credit_transactions_orders] SUMMARY for account_id={account_id}, location_id={location_id}:")
        logger.info(f"   Duplicates removed from staging: {duplicates_removed}")
        logger.info(f"   Total staging records (after cleanup): {total_staging}")
        logger.info(f"   Unnecessary records (isin_order_line = false): {unnecessary_records} ({round((unnecessary_records / total_staging * 100), 2) if total_staging > 0 else 0.0}%)")
        logger.info(f"   Existing records in main table: {existing_in_main}")
        logger.info(f"   Records already exist in main: {already_exist_in_main}")
        logger.info(f"   Records with missing customers: {missing_customer_transactions} ({round((missing_customer_transactions / (total_staging - unnecessary_records) * 100), 2) if (total_staging - unnecessary_records) > 0 else 0.0}%)")
        logger.info(f"   Records ready for insertion: {ready_to_insert}")
        logger.info(f"   Actually inserted: {actual_inserted}")
        
        # Additional insights
        if duplicates_removed > 0:
            logger.warning(f"[process_credit_transactions_orders] WARNING: {duplicates_removed} duplicate records removed from staging table")
        if missing_customer_transactions > 0:
            logger.warning(f"[process_credit_transactions_orders] WARNING: {missing_customer_transactions} records skipped due to missing customer dependencies")
        
        logger.info(f"[process_credit_transactions_orders] SUCCESS: Completed credit_transactions_orders processing for account_id={account_id}, location_id={location_id}")
        
        return {
            "duplicates_removed": duplicates_removed,
            "total_records": total_staging,
            "unnecessary_records": unnecessary_records,
            "existing_records": existing_in_main,
            "already_exist_records": already_exist_in_main,
            "missing_customer_records": missing_customer_transactions,
            "missing_dependency_percentage": round(((total_staging - existing_in_main - actual_inserted) / total_staging * 100), 2) if total_staging > 0 else 0.0,
            "ready_to_insert": ready_to_insert,
            "inserted_records": actual_inserted
        }
        
    except Exception as e:
        logger.error(f"[process_credit_transactions_orders] ERROR: Failed processing for account_id={account_id}, location_id={location_id}: {e}")
        logger.error(f"[process_credit_transactions_orders] ERROR: Error occurred during processing. Partial results:")
        logger.error(f"[process_credit_transactions_orders]   Duplicates removed from staging: {duplicates_removed}")
        logger.error(f"[process_credit_transactions_orders]   Total staging records (after cleanup): {total_staging}")
        logger.error(f"[process_credit_transactions_orders]   Unnecessary records (isin_order_line = false): {unnecessary_records}")
        logger.error(f"[process_credit_transactions_orders]   Existing records in main table: {existing_in_main}")
        logger.error(f"[process_credit_transactions_orders]   Records already exist in main: {already_exist_in_main}")
        logger.error(f"[process_credit_transactions_orders]   Records with missing customers: {missing_customer_transactions}")
        logger.error(f"[process_credit_transactions_orders]   Records ready for insertion: {ready_to_insert}")
        logger.error(f"[process_credit_transactions_orders]   Actually inserted: {actual_inserted}")
        raise
