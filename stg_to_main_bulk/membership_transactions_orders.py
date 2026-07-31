"""
Membership Transactions Orders Service - Step 5a of ETL Pipeline
Processes membership_transactions data from staging to membership_transactions_orders table with customer dependency validation
Dependencies: customers table only
"""

import logging
from sqlalchemy import text
from stg_to_main_bulk._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)

async def step_1_count_total_records(account_id: str, engine):
    """
    Step 1: Count total membership_transactions records in staging after Step 0 cleanup
    """
    logger.info(f"[STEP 1] Counting total membership_transactions records in staging (after cleanup)")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_membership_transactions_orders_bulk
                WHERE account_id = :account_id
            """), {"account_id": account_id})
            total_count = result.fetchone()[0]
        
        logger.info(f"[STEP 1] SUCCESS: Total membership_transactions records after cleanup: {total_count}")
        return {"total_staging_after_cleanup": total_count}
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count total membership_transactions records: {e}")
        raise

async def step_1b_count_unnecessary_records(account_id: str, engine):
    """
    Step 1b: always 0 in the bulk pipeline — kept because process_membership_transactions_orders's
    count formula subtracts it.

    The onboarding pipeline stages one shared credit/membership staging table and splits
    the rows with isin_reservation / isin_order_line, counting the other side's rows here
    as "unnecessary". Silver ships the two sides as separate tables
    (silver.membership_transactions / silver.membership_transactions_orders), each loaded into its own bulk staging
    table, so every staged row belongs to this side and none is unnecessary.
    """
    logger.info("[STEP 1b] Unnecessary records: 0 (bulk staging is pre-split by Silver)")
    return 0

async def step_2_count_existing_in_main_table(account_id: str, engine):
    """
    Step 2: Count membership_transactions records already existing in main table
    """
    logger.info(f"[STEP 2] Counting membership_transactions records already in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_membership_transactions_orders_bulk stg
                WHERE stg.account_id = :account_id
                  AND stg.membership_transactions_id IN (
                    SELECT membership_transactions_id FROM membership_transactions_orders 
                    WHERE account_id = :account_id
                  )
            """), {"account_id": account_id})
            existing_count = result.fetchone()[0]
        
        logger.info(f"[STEP 2] SUCCESS: Records already exist in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing membership_transactions: {e}")
        raise

async def step_3_count_records_already_exist_in_main_table(account_id: str, engine):
    """
    Step 3: Count membership_transactions records that already exist in main table (alternative method)
    """
    logger.info(f"[STEP 3] Verifying count of records already existing in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_membership_transactions_orders_bulk stg
                INNER JOIN membership_transactions_orders mto
                  ON mto.membership_transactions_id = stg.membership_transactions_id
                  AND mto.account_id = stg.account_id
                  AND mto.location = stg.location
                WHERE stg.account_id = :account_id
            """), {"account_id": account_id})
            already_exist_count = result.fetchone()[0]
        
        logger.info(f"[STEP 3] SUCCESS: Records already exist in main table (verification): {already_exist_count}")
        return already_exist_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to verify existing membership_transactions: {e}")
        raise

async def step_4_count_transactions_with_missing_dependencies(account_id: str, engine):
    """
    Step 4: Count membership_transactions records with missing dependencies (customers or membership_instances)
    OPTIMIZED: Single query with LEFT JOIN instead of 3 separate queries
    """
    logger.info(f"[STEP 4] Counting membership_transactions records with missing dependencies")
    
    try:
        with engine.begin() as conn:  # ✅ Single connection
            # ✅ OPTIMIZED: Single query gets ALL metrics at once!
            result = conn.execute(text("""
                SELECT 
                    COUNT(CASE WHEN c.customer_id IS NULL OR mi.membership_instances_id IS NULL 
                               THEN 1 END) as total_missing_dependencies,
                    COUNT(CASE WHEN c.customer_id IS NULL THEN 1 END) as missing_customers,
                    COUNT(CASE WHEN mi.membership_instances_id IS NULL THEN 1 END) as missing_membership_instances
                FROM stg_membership_transactions_orders_bulk mt
                LEFT JOIN customers c ON c.customer_id = mt.customer_id 
                    AND c.account_id = :account_id
                LEFT JOIN membership_instances mi ON mi.membership_instances_id = mt.membership_instances_id 
                    AND mi.location = mt.location 
                    AND mi.account_id = mt.account_id
                    AND mi.account_id = :account_id
                WHERE mt.account_id = :account_id
            """), {"account_id": account_id})
            
            row = result.fetchone()
            missing_dependencies_count = int(row[0])
            missing_customers_count = int(row[1])
            missing_membership_instances_count = int(row[2])
        
        # Log details about missing dependencies
        if missing_dependencies_count > 0:
            logger.warning(f"[STEP 4] Found {missing_dependencies_count} membership_transactions records with missing dependencies")
            logger.warning(f"[STEP 4] Missing customers: {missing_customers_count}")
            logger.warning(f"[STEP 4] Missing membership_instances: {missing_membership_instances_count}")
        
        logger.info(f"[STEP 4] SUCCESS: Records with missing dependencies: {missing_dependencies_count}")
        logger.info(f"[STEP 4] SUCCESS: Records with missing customers: {missing_customers_count}")
        logger.info(f"[STEP 4] SUCCESS: Records with missing membership_instances: {missing_membership_instances_count}")
        return missing_dependencies_count
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count membership_transactions with missing dependencies: {e}")
        raise

async def step_5_calculate_expected_ready(account_id: str, total_staging_after_cleanup: int, unnecessary_records: int, already_exist_in_main: int, invalid_dependency_records: int):
    """
    Step 5: Calculate expected records ready for insertion
    Formula: total_staging_after_cleanup - unnecessary_records - already_exist_in_main - invalid_dependency_records
    """
    logger.info(f"[STEP 5] Calculating expected records ready for insertion")
    
    expected_ready = total_staging_after_cleanup - unnecessary_records - already_exist_in_main - invalid_dependency_records
    
    logger.info(f"[STEP 5] SUCCESS: Expected ready for insertion: {expected_ready}")
    logger.info(f"[STEP 5] Breakdown: Total_after_cleanup({total_staging_after_cleanup}) - Unnecessary({unnecessary_records}) - Already_exist({already_exist_in_main}) - Invalid_Dependencies({invalid_dependency_records}) = {expected_ready}")
    
    return expected_ready

async def step_6_count_records_to_insert(account_id: str, engine):
    """
    Step 6: Count records that are actually ready for insertion (validation before insert)
    OPTIMIZED: Uses LEFT JOIN + IS NULL instead of NOT IN for better performance
    """
    logger.info(f"[STEP 6] Counting records ready for insertion (validation)")
    
    try:
        with engine.begin() as conn:
            # ✅ OPTIMIZED: LEFT JOIN instead of NOT IN
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_membership_transactions_orders_bulk mt
                INNER JOIN customers c 
                    ON c.customer_id = mt.customer_id  
                    AND c.account_id = mt.account_id
                INNER JOIN membership_instances mi 
                    ON mi.membership_instances_id = mt.membership_instances_id 
                    AND mi.location = mt.location 
                    AND mi.account_id = mt.account_id
                LEFT JOIN public.membership_transactions_orders mto
                    ON mto.membership_transactions_id = mt.membership_transactions_id
                    AND mto.account_id = mt.account_id
                    AND mto.location = mt.location
                WHERE mt.account_id = :account_id 
                  AND mto.membership_transactions_id IS NULL
            """), {"account_id": account_id})
            insert_ready_count = result.fetchone()[0]
            
        logger.info(f"[STEP 6] SUCCESS: Records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to count records ready for insertion: {e}")
        raise

async def step_7_insert_new_records(account_id: str, engine):
    """
    Step 7: Insert new membership_transactions records into membership_transactions_orders table
    """
    logger.info(f"[STEP 7] Inserting new membership_transactions records")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO public.membership_transactions_orders(
                     membership_transactions_id, transaction_date, membership_name, parent_membership_transaction_id,
                   membership_instances_id, customer_id, location, created_at, created_by, updated_at, updated_by, 
                   deleted_at, deleted_by, payment_interval_end_date, account_id,  membership_instances_ref_id
                )
                SELECT  
                   mt.membership_transactions_id, mt.transaction_date, mt.membership_name, mt.parent_membership_transaction_id,
                   mt.membership_instances_id, mt.customer_id, mt.location, now() created_at, 1 as created_by, now() updated_at, mt.updated_by, 
                   mt.deleted_at, mt.deleted_by, mt.payment_interval_end_date, mt.account_id, mi.id as membership_instances_ref_id
                FROM stg_membership_transactions_orders_bulk mt
                INNER JOIN customers c 
                ON mt.customer_id = c.customer_id  
                AND mt.account_id = c.account_id
                INNER JOIN membership_instances mi 
                ON mt.membership_instances_id = mi.membership_instances_id 
                AND mt.location = mi.location 
                AND mt.account_id = mi.account_id
                WHERE mt.account_id = :account_id
                AND mt.membership_transactions_id NOT IN (SELECT membership_transactions_id FROM public.membership_transactions_orders WHERE account_id = :account_id)
            """), {"account_id": account_id})
            inserted_count = result.rowcount
            
        logger.info(f"[STEP 7] SUCCESS: Inserted {inserted_count} membership_transactions records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 7] ERROR: Failed to insert membership_transactions records: {e}")
        raise

async def process_membership_transactions_orders(account_id: str, engine):
    """
    Main function to process membership_transactions data with customer dependency validation
    7-step process (0-6): Drop duplicates, count records, validate dependencies, insert data
    """
    logger.info(f"[process_membership_transactions_orders] Starting processing for account_id={account_id}")
    
    try:
        # Step 0: Drop duplicates in staging table
        try:
            step_0_result = drop_staging_duplicates(engine, "stg_membership_transactions_orders_bulk", "membership_transactions_id", account_id)
            duplicates_found = step_0_result["duplicates_found"]
            duplicates_removed = step_0_result["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_membership_transactions_orders] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Count total records after cleanup
        try:
            total_records_result = await step_1_count_total_records(account_id, engine)
            total_staging_after_cleanup = total_records_result["total_staging_after_cleanup"]
        except Exception as e:
            logger.error(f"[process_membership_transactions_orders] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 1b: Count unnecessary records (not used in order lines)
        try:
            unnecessary_records = await step_1b_count_unnecessary_records(account_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions_orders] ERROR: Step 1b failed: {e}")
            raise
        
        # Step 2: Count records already existing in main table  
        try:
            already_exist_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions_orders] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Verify count of existing records (alternative method)
        try:
            already_exist_verification = await step_3_count_records_already_exist_in_main_table(account_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions_orders] ERROR: Step 3 failed: {e}")
            raise
        
        # Step 4: Count records with missing dependencies (customers or membership_instances)
        try:
            invalid_dependency_records = await step_4_count_transactions_with_missing_dependencies(account_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions_orders] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Calculate expected records for insertion
        try:
            expected_ready = await step_5_calculate_expected_ready(account_id, total_staging_after_cleanup, unnecessary_records, already_exist_in_main, invalid_dependency_records)
        except Exception as e:
            logger.error(f"[process_membership_transactions_orders] ERROR: Step 5 failed: {e}")
            raise
        
        # Step 6: Count records ready for insertion (validation before insert)
        if expected_ready > 0:
            try:
                insert_ready_count = await step_6_count_records_to_insert(account_id, engine)
            except Exception as e:
                logger.error(f"[process_membership_transactions_orders] ERROR: Step 6 failed: {e}")
                raise
            
            # Validate that insert_ready_count matches expected_ready
            if insert_ready_count != expected_ready:
                logger.error(f"[process_membership_transactions_orders] ERROR: Validation failed - expected {expected_ready} but found {insert_ready_count} ready for insertion")
                raise Exception(f"Insert ready count mismatch: expected {expected_ready}, actual {insert_ready_count}")
            
            logger.info(f"[process_membership_transactions_orders] SUCCESS: Validation passed - {insert_ready_count} records ready for insertion as expected")

            # Step 7: Insert new records
            try:
                inserted_records = await step_7_insert_new_records(account_id, engine)
            except Exception as e:
                logger.error(f"[process_membership_transactions_orders] ERROR: Step 7 failed: {e}")
                raise

            # Final validation that insertion count matches expected
            if inserted_records == expected_ready:
                logger.info(f"[process_membership_transactions_orders] SUCCESS: Final validation passed - inserted {inserted_records} records as expected")
            else:
                logger.error(f"[process_membership_transactions_orders] ERROR: Final validation failed - expected {expected_ready} but inserted {inserted_records}")
                raise Exception(f"Final insertion count mismatch: expected {expected_ready}, actual {inserted_records}")
        else:
            logger.warning(f"[process_membership_transactions_orders] WARNING: No records to insert (expected_ready: {expected_ready})")
            insert_ready_count = 0
            inserted_records = 0
        
        # Calculate missing percentage: (total - unnecessary - already_existing - inserted) / total * 100
        total_not_inserted = total_staging_after_cleanup - unnecessary_records - already_exist_in_main - inserted_records
        missing_percentage = round((total_not_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        
        # Summary
        logger.info(f"[process_membership_transactions_orders] PROCESS COMPLETE:")
        logger.info(f"  - Duplicates found in staging: {duplicates_found}")
        logger.info(f"  - Duplicates removed: {duplicates_removed}")
        logger.info(f"  - Total staging records after cleanup: {total_staging_after_cleanup}")
        logger.info(f"  - Unnecessary records (not in order lines): {unnecessary_records}")
        logger.info(f"  - Records already exist in main table: {already_exist_in_main}")
        logger.info(f"  - Records with missing dependencies: {invalid_dependency_records}")
        logger.info(f"  - Records not inserted (unnecessary + missing deps + validation failures): {total_not_inserted} ({missing_percentage}%)")
        logger.info(f"  - Records ready for insertion: {insert_ready_count}")
        logger.info(f"  - Actually inserted: {inserted_records}")
        
        return {
            "duplicates_found": duplicates_found,
            "duplicates_removed": duplicates_removed,
            "total_staging_after_cleanup": total_staging_after_cleanup,
            "unnecessary_records": unnecessary_records,
            "already_exist_in_main_table": already_exist_in_main,
            "invalid_dependency_records": invalid_dependency_records,
            "missing_dependency_percentage": missing_percentage,
            "insert_ready_count": insert_ready_count,
            "inserted_records": inserted_records
        }
        
    except Exception as e:
        logger.error(f"[process_membership_transactions_orders] FATAL ERROR: {e}")
        raise
