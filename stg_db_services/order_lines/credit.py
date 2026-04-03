"""
Order Lines Credit Service - Step 7a of ETL Pipeline
Processes CreditTransaction order_lines from staging to main table with dual dependency validation
Dependencies: orders table AND credit_transactions_orders table
"""

import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def step_0_drop_duplicates_in_staging(account_id: str, location_id: int, engine):
    """
    Step 0: Drop duplicate CreditTransaction records in staging table (mt_order_lines_details_dlk) to clean data
    """
    logger.info(f"[STEP 0] Dropping duplicate CreditTransaction records in mt_order_lines_details_dlk staging table")
    
    try:
        with engine.begin() as conn:
            # First count duplicates before removal
            count_result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT order_line_id) as unique_order_lines
                FROM mt_order_lines_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
                  AND transaction_type = 'CreditTransaction'
                  AND is_valid = TRUE
            """), {"account_id": account_id, "location_id": str(location_id)})
            stats = count_result.fetchone()
            total_before = stats[0]
            unique_order_lines = stats[1]
            duplicates_found = total_before - unique_order_lines
            
            if duplicates_found > 0:
                logger.info(f"[STEP 0] Found {duplicates_found} duplicate CreditTransaction records to remove ({total_before} total, {unique_order_lines} unique)")
                
                # Delete duplicates, keeping only one record per order_line_id
                result = conn.execute(text("""
                    DELETE FROM mt_order_lines_details_dlk 
                    WHERE ctid NOT IN (
                        SELECT DISTINCT ON (order_line_id) ctid
                        FROM mt_order_lines_details_dlk
                        WHERE account_id = :account_id
                          AND location = :location_id
                          AND transaction_type = 'CreditTransaction'
                        ORDER BY order_line_id, ctid
                    )
                    AND account_id = :account_id
                    AND location = :location_id
                    AND transaction_type = 'CreditTransaction'
                """), {"account_id": account_id, "location_id": str(location_id)})
                deleted_count = result.rowcount
                
                logger.info(f"[STEP 0] SUCCESS: Removed {deleted_count} duplicate CreditTransaction records from staging table")
            else:
                logger.info(f"[STEP 0] SUCCESS: No duplicates found in CreditTransaction staging records")
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
    Step 1: Count total unique CreditTransaction order_lines records after DISTINCT ON logic
    This ensures consistency with steps 2, 4, 6, 7 that all use DISTINCT ON
    """
    logger.info(f"[STEP 1] Counting total unique CreditTransaction order_lines records (using DISTINCT ON logic)")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM (
                  SELECT DISTINCT ON (cto.id)
                    stg.order_line_id
                  FROM mt_order_lines_details_dlk stg
                  INNER JOIN credit_transactions_orders cto
                    ON stg.credit_transactions_id = cto.credit_transactions_id
                    AND cto.account_id = :account_id
                    AND cto.location = :location_id
                  WHERE stg.account_id = :account_id
                    AND stg.location = :location_id
                    AND stg.transaction_type = 'CreditTransaction'
                    AND stg.is_valid = TRUE
                  ORDER BY cto.id, stg.updated_at DESC
                ) unique_records
            """), {"account_id": account_id, "location_id": str(location_id)})
            total_count = result.fetchone()[0]
        
        logger.info(f"[STEP 1] SUCCESS: Total unique CreditTransaction order_lines records: {total_count}")
        return {"total_staging_after_cleanup": total_count}
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count total unique CreditTransaction order_lines records: {e}")
        raise

async def step_2_count_existing_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 2: Count CreditTransaction order_lines records already existing in main table
    OPTIMIZED: Uses INNER JOIN instead of EXISTS for better performance
    """
    logger.info(f"[STEP 2] Counting CreditTransaction order_lines records already in main table")
    
    try:
        with engine.begin() as conn:
            # ✅ OPTIMIZED: INNER JOIN instead of EXISTS
            result = conn.execute(text("""
                WITH candidate_rows AS (
                    SELECT DISTINCT ON (cto.id)
                        cto.id AS credit_transactions_ref_id
                    FROM mt_order_lines_details_dlk stg
                    INNER JOIN credit_transactions_orders cto
                        ON cto.credit_transactions_id = stg.credit_transactions_id
                        AND cto.account_id = :account_id
                        AND cto.location = :location_id
                    INNER JOIN orders o
                        ON o.order_id = stg.order_id
                        AND o.account_id = :account_id
                    WHERE stg.account_id = :account_id
                        AND stg.location = :location_id
                        AND stg.transaction_type = 'CreditTransaction'
                        AND stg.is_valid = TRUE
                    ORDER BY cto.id, stg.updated_at DESC
                )
                SELECT COUNT(*)
                FROM candidate_rows cr
                INNER JOIN order_lines ol 
                    ON ol.credit_transactions_ref_id = cr.credit_transactions_ref_id
            """), {"account_id": account_id, "location_id": str(location_id)})
            existing_count = result.fetchone()[0]
        
        logger.info(f"[STEP 2] SUCCESS: CreditTransaction records already exist in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing CreditTransaction order_lines: {e}")
        raise

async def step_3_count_records_already_exist_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 3: Verify count using the same logic as Step 2 for consistency
    """
    logger.info(f"[STEP 3] Verifying count of CreditTransaction records already existing in main table")
    
    # Use the same logic as Step 2 for consistency
    return await step_2_count_existing_in_main_table(account_id, location_id, engine)

async def step_4_count_order_lines_with_missing_dependencies(account_id: str, location_id: int, engine):
    """
    Step 4: Count CreditTransaction order_lines records with missing dependencies
    OPTIMIZED: Single query with LEFT JOIN instead of 2 separate queries
    """
    logger.info(f"[STEP 4] Counting CreditTransaction order_lines records with missing dependencies")
    
    try:
        with engine.begin() as conn:
            # ✅ OPTIMIZED: Single query gets all metrics at once!
            result = conn.execute(text("""
                WITH unique_records AS (
                    SELECT DISTINCT ON (cto.id)
                        stg.order_line_id,
                        cto.id AS credit_transactions_ref_id,
                        stg.order_id
                    FROM mt_order_lines_details_dlk stg
                    INNER JOIN credit_transactions_orders cto
                        ON cto.credit_transactions_id = stg.credit_transactions_id
                        AND cto.account_id = :account_id
                        AND cto.location = :location_id
                    WHERE stg.account_id = :account_id
                        AND stg.location = :location_id
                        AND stg.transaction_type = 'CreditTransaction'
                        AND stg.is_valid = TRUE
                    ORDER BY cto.id, stg.updated_at DESC
                )
                SELECT 
                    COUNT(*) as total_unique_count,
                    COUNT(o.order_id) as valid_dependencies_count,
                    COUNT(*) - COUNT(o.order_id) as invalid_dependencies_count
                FROM unique_records ur
                LEFT JOIN orders o 
                    ON o.order_id = ur.order_id
                    AND o.account_id = :account_id
            """), {"account_id": account_id, "location_id": str(location_id)})
            
            row = result.fetchone()
            total_unique_count = int(row[0])
            valid_dependencies_count = int(row[1])
            invalid_dependencies_count = int(row[2])
            
        logger.info(f"[STEP 4] SUCCESS: Total unique records: {total_unique_count}")
        logger.info(f"[STEP 4] SUCCESS: Valid dependencies: {valid_dependencies_count}")
        logger.info(f"[STEP 4] SUCCESS: Invalid dependencies: {invalid_dependencies_count}")
        return invalid_dependencies_count
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count CreditTransaction order_lines with missing dependencies: {e}")
        raise

async def step_5_calculate_expected_ready(account_id: str, location_id: int, total_staging_after_cleanup: int, already_exist_in_main: int, invalid_dependency_records: int):
    """
    Step 5: Calculate expected CreditTransaction order_lines records ready for insertion
    Formula: total_staging_after_cleanup - already_exist_in_main - invalid_dependency_records
    Now all values use the same DISTINCT ON logic so the formula is consistent
    """
    logger.info(f"[STEP 5] Calculating expected CreditTransaction order_lines records ready for insertion")
    
    expected_ready = total_staging_after_cleanup - already_exist_in_main - invalid_dependency_records
    
    logger.info(f"[STEP 5] SUCCESS: Expected ready for insertion: {expected_ready}")
    logger.info(f"[STEP 5] Breakdown: Total_unique_records({total_staging_after_cleanup}) - Already_exist({already_exist_in_main}) - Invalid_Dependencies({invalid_dependency_records}) = {expected_ready}")
    
    return expected_ready

async def step_6_count_records_to_insert(account_id: str, location_id: int, engine):
    """
    Step 6: Count CreditTransaction order_lines records ready for insertion
    OPTIMIZED: Uses LEFT JOIN + IS NULL instead of NOT EXISTS for better performance
    """
    logger.info(f"[STEP 6] Counting CreditTransaction order_lines records ready for insertion")
    
    try:
        with engine.begin() as conn:
            # ✅ OPTIMIZED: LEFT JOIN instead of NOT EXISTS
            result = conn.execute(text("""
                WITH candidate_rows AS (
                    SELECT DISTINCT ON (cto.id)
                        stg.order_line_id,
                        cto.id AS credit_transactions_ref_id
                    FROM mt_order_lines_details_dlk stg
                    INNER JOIN credit_transactions_orders cto
                        ON cto.credit_transactions_id = stg.credit_transactions_id
                        AND cto.account_id = :account_id
                        AND cto.location = :location_id
                    INNER JOIN orders o
                        ON o.order_id = stg.order_id
                        AND o.account_id = :account_id
                    WHERE stg.account_id = :account_id
                        AND stg.location = :location_id
                        AND stg.transaction_type = 'CreditTransaction'
                        AND stg.is_valid = TRUE
                    ORDER BY cto.id, stg.updated_at DESC
                )
                SELECT COUNT(*)
                FROM candidate_rows cr
                LEFT JOIN order_lines ol 
                    ON ol.credit_transactions_ref_id = cr.credit_transactions_ref_id
                WHERE ol.credit_transactions_ref_id IS NULL
            """), {"account_id": account_id, "location_id": str(location_id)})
            ready_count = result.fetchone()[0]
        
        logger.info(f"[STEP 6] SUCCESS: {ready_count} CreditTransaction order_lines records ready for insertion (matching INSERT logic exactly)")
        return ready_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to count ready CreditTransaction order_lines: {e}")
        raise

async def step_7_insert_new_records(account_id: str, location_id: int, engine):
    """
    Step 7: Insert CreditTransaction order_lines records using robust raw SQL
    Uses DISTINCT ON and WHERE NOT EXISTS for consistent behavior
    """
    logger.info(f"[STEP 7] Inserting CreditTransaction order_lines records")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                WITH candidate_rows AS (
                  SELECT DISTINCT ON (cto.id)
                    stg.order_line_id,
                    stg.order_id,
                    stg.transaction_type,
                    stg.location,
                    stg.credit_transactions_id,
                    stg.membership_transactions_id,
                    stg.title,
                    stg.line_total,
                    stg.processed_by,
                    now() AS created_at,
                    1 AS created_by,
                    now() AS updated_at,
                    stg.updated_by,
                    stg.deleted_at,
                    stg.deleted_by,
                    stg.account_id,
                    cto.id AS credit_transactions_ref_id,
                    o.id AS order_ref_id
                  FROM mt_order_lines_details_dlk stg
                  INNER JOIN credit_transactions_orders cto
                    ON stg.credit_transactions_id = cto.credit_transactions_id
                    AND cto.account_id = :account_id
                    AND cto.location = :location_id
                  INNER JOIN orders o
                    ON stg.order_id = o.order_id
                    AND o.account_id = :account_id
                  WHERE stg.account_id = :account_id
                    AND stg.location = :location_id
                    AND stg.transaction_type = 'CreditTransaction'
                    AND stg.is_valid = TRUE
                  ORDER BY cto.id, stg.updated_at DESC
                )
                INSERT INTO public.order_lines (
                  order_line_id, order_id, transaction_type, location, credit_transactions_id, membership_transactions_id,
                  title, line_total, processed_by, created_at, created_by, updated_at, updated_by, deleted_at, deleted_by,
                  account_id, credit_transactions_ref_id, membership_transactions_ref_id, order_ref_id
                )
                SELECT
                  order_line_id, order_id, transaction_type, location, credit_transactions_id, membership_transactions_id,
                  title, line_total, processed_by, created_at, created_by, updated_at, updated_by, deleted_at, deleted_by,
                  account_id, credit_transactions_ref_id, NULL AS membership_transactions_ref_id, order_ref_id
                FROM candidate_rows cr
                WHERE NOT EXISTS (
                  SELECT 1 FROM public.order_lines ol
                  WHERE ol.credit_transactions_ref_id = cr.credit_transactions_ref_id
                )
            """), {"account_id": account_id, "location_id": str(location_id)})
            inserted_count = result.rowcount
            
        logger.info(f"[STEP 7] SUCCESS: Inserted {inserted_count} records using robust raw SQL approach")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 7] ERROR: Failed to insert CreditTransaction order_lines records: {e}")
        raise

async def process_order_lines_credit(account_id: str, location_id: int, engine):
    """
    Main function to process CreditTransaction order_lines data with dual dependency validation
    8-step process (0-7): Drop duplicates, count records, validate dependencies, insert data
    Dependencies: orders table AND credit_transactions_orders table
    All steps now use consistent DISTINCT ON (cto.id) logic for accurate count tracking
    """
    logger.info(f"[process_order_lines_credit] Starting processing for account_id={account_id}, location_id={location_id}")
    
    try:
        # Step 0: Drop duplicates in staging table
        try:
            step_0_result = await step_0_drop_duplicates_in_staging(account_id, location_id, engine)
            duplicates_found = step_0_result["duplicates_found"]
            duplicates_removed = step_0_result["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_order_lines_credit] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Count total unique records (using DISTINCT ON logic)
        try:
            total_records_result = await step_1_count_total_records(account_id, location_id, engine)
            total_staging_after_cleanup = total_records_result["total_staging_after_cleanup"]
        except Exception as e:
            logger.error(f"[process_order_lines_credit] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 2: Count records already existing in main table  
        try:
            already_exist_in_main = await step_2_count_existing_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_order_lines_credit] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Verify count of existing records (should match Step 2)
        try:
            already_exist_verification = await step_3_count_records_already_exist_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_order_lines_credit] ERROR: Step 3 failed: {e}")
            raise
        
        # Validate Step 2 and Step 3 consistency
        if already_exist_in_main != already_exist_verification:
            logger.warning(f"[process_order_lines_credit] WARNING: Step 2 ({already_exist_in_main}) and Step 3 ({already_exist_verification}) counts don't match")
        
        # Step 4: Count records with missing dependencies
        try:
            invalid_dependency_records = await step_4_count_order_lines_with_missing_dependencies(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_order_lines_credit] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Calculate expected records for insertion
        try:
            expected_ready = await step_5_calculate_expected_ready(account_id, location_id, total_staging_after_cleanup, already_exist_in_main, invalid_dependency_records)
        except Exception as e:
            logger.error(f"[process_order_lines_credit] ERROR: Step 5 failed: {e}")
            raise
        
        # Step 6: Count records ready for insertion (validation before insert)
        if expected_ready > 0:
            try:
                insert_ready_count = await step_6_count_records_to_insert(account_id, location_id, engine)
            except Exception as e:
                logger.error(f"[process_order_lines_credit] ERROR: Step 6 failed: {e}")
                raise
            
            # Validate that insert_ready_count matches expected_ready
            if insert_ready_count != expected_ready:
                logger.error(f"[process_order_lines_credit] ERROR: Validation failed - expected {expected_ready} but found {insert_ready_count} ready for insertion")
                raise Exception(f"Insert ready count mismatch: expected {expected_ready}, actual {insert_ready_count}")
            
            logger.info(f"[process_order_lines_credit] SUCCESS: Validation passed - {insert_ready_count} records ready for insertion as expected")
            
            # Step 7: Insert new records
            try:
                inserted_records = await step_7_insert_new_records(account_id, location_id, engine)
            except Exception as e:
                logger.error(f"[process_order_lines_credit] ERROR: Step 7 failed: {e}")
                raise
            
            # Final validation that insertion count matches expected
            if inserted_records == expected_ready:
                logger.info(f"[process_order_lines_credit] SUCCESS: Final validation passed - inserted {inserted_records} records as expected")
            else:
                logger.error(f"[process_order_lines_credit] ERROR: Final validation failed - expected {expected_ready} but inserted {inserted_records}")
                raise Exception(f"Final insertion count mismatch: expected {expected_ready}, actual {inserted_records}")
        else:
            logger.warning(f"[process_order_lines_credit] WARNING: No records to insert (expected_ready: {expected_ready})")
            insert_ready_count = 0
            inserted_records = 0
        
        # Calculate missing percentage based on consistent unique count logic
        total_not_inserted = total_staging_after_cleanup - already_exist_in_main - inserted_records
        missing_percentage = round((total_not_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        
        # Summary
        logger.info(f"[process_order_lines_credit] PROCESS COMPLETE:")
        logger.info(f"  - Duplicates found in staging: {duplicates_found}")
        logger.info(f"  - Duplicates removed: {duplicates_removed}")
        logger.info(f"  - Total unique records (DISTINCT ON logic): {total_staging_after_cleanup}")
        logger.info(f"  - Records already exist in main table: {already_exist_in_main}")
        logger.info(f"  - Records with missing dependencies: {invalid_dependency_records}")
        logger.info(f"  - Records not inserted (missing deps + validation failures): {total_not_inserted} ({missing_percentage}%)")
        logger.info(f"  - Records ready for insertion: {insert_ready_count}")
        logger.info(f"  - Actually inserted: {inserted_records}")
        
        return {
            "transaction_type": "CreditTransaction",
            "duplicates_found": duplicates_found,
            "duplicates_removed": duplicates_removed,
            "total_staging_after_cleanup": total_staging_after_cleanup,
            "already_exist_in_main_table": already_exist_in_main,
            "invalid_dependency_records": invalid_dependency_records,
            "missing_dependency_percentage": missing_percentage,
            "insert_ready_count": insert_ready_count,
            "inserted_records": inserted_records
        }
        
    except Exception as e:
        logger.error(f"[process_order_lines_credit] FATAL ERROR: {e}")
        raise
