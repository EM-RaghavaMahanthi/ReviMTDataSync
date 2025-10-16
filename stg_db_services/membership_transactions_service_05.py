"""
Membership Transactions Service - Step 5 of ETL Pipeline
Processes membership_transactions data from staging to main table with dual dependency validation
Dependencies: customers table and membership_instances table
"""

import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def step_0_drop_duplicates_in_staging(account_id: str, location_id: int, engine):
    """
    Step 0: Drop duplicate records in staging table (mt_membership_transactions_details_dlk) to clean data
    """
    logger.info(f"[STEP 0] Dropping duplicate records in mt_membership_transactions_details_dlk staging table")
    
    try:
        with engine.begin() as conn:
            # First count duplicates before removal
            count_result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT membership_transactions_id) as unique_transactions
                FROM mt_membership_transactions_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
            """), {"account_id": account_id, "location_id": location_id})
            stats = count_result.fetchone()
            total_before = stats[0]
            unique_transactions = stats[1]
            duplicates_found = total_before - unique_transactions
            
            if duplicates_found > 0:
                logger.info(f"[STEP 0] Found {duplicates_found} duplicate records to remove ({total_before} total, {unique_transactions} unique)")
                
                # Delete duplicates, keeping only one record per membership_transactions_id
                result = conn.execute(text("""
                    DELETE FROM mt_membership_transactions_details_dlk 
                    WHERE ctid NOT IN (
                        SELECT DISTINCT ON (membership_transactions_id) ctid
                        FROM mt_membership_transactions_details_dlk
                        WHERE account_id = :account_id
                          AND location = :location_id
                        ORDER BY membership_transactions_id, ctid
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
    Step 1: Count total records in staging table after Step 0 cleanup
    """
    logger.info(f"[STEP 1] Counting total records in mt_membership_transactions_details_dlk (after cleanup)")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM mt_membership_transactions_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
            """), {"account_id": account_id, "location_id": location_id})
            total_count = result.scalar()
            logger.info(f"[STEP 1] SUCCESS: Total staging records after cleanup: {total_count}")
            
        return {"total_staging_after_cleanup": total_count}
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count total records: {e}")
        raise

async def step_1b_count_unnecessary_records(account_id: str, location_id: int, engine):
    """
    Step 1b: Count unnecessary records (isin_reservation = false) that won't be processed by reservations service
    """
    logger.info(f"[STEP 1b] Counting unnecessary records (isin_reservation = false)")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_membership_transactions_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
                  AND isin_reservation = false
            """), {"account_id": account_id, "location_id": location_id})
            unnecessary_count = result.fetchone()[0]
        
        logger.info(f"[STEP 1b] SUCCESS: Unnecessary records (isin_reservation = false): {unnecessary_count}")
        return unnecessary_count
    except Exception as e:
        logger.error(f"[STEP 1b] ERROR: Failed to count unnecessary records: {e}")
        raise

async def step_2_count_existing_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 2: Count membership_transactions records already existing in main table (only reservation related)
    """
    logger.info(f"[STEP 2] Counting membership_transactions records already in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_membership_transactions_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
                  AND EXISTS (
                    SELECT 1 FROM membership_transactions mt
                    WHERE mt.membership_transactions_id = stg.membership_transactions_id
                      AND mt.account_id = :account_id
                      AND mt.location = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            existing_count = result.fetchone()[0]
        
        logger.info(f"[STEP 2] SUCCESS: Records already exist in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing membership_transactions: {e}")
        raise

async def step_3_count_records_already_exist_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 3: Count membership_transactions records that already exist in main table (alternative count method, only reservation related)
    """
    logger.info(f"[STEP 3] Verifying count of records already existing in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_membership_transactions_details_dlk stg
                INNER JOIN membership_transactions mt
                  ON mt.membership_transactions_id = stg.membership_transactions_id
                  AND mt.account_id = stg.account_id
                  AND mt.location = stg.location
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
            """), {"account_id": account_id, "location_id": location_id})
            already_exist_count = result.fetchone()[0]
        
        logger.info(f"[STEP 3] SUCCESS: Records already exist in main table (verification): {already_exist_count}")
        return already_exist_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to verify existing membership_transactions: {e}")
        raise

async def step_4_count_transactions_with_missing_dependencies(account_id: str, location_id: int, engine):
    """
    Step 3: Count membership_transactions records with missing customer or membership_instances dependencies
    Uses DISTINCT with LEFT JOINs to find records that would fail the INNER JOIN in insertion
    This approach scales cleanly to any number of dependencies
    """
    logger.info(f"[STEP 3] Counting membership_transactions records with missing dependencies")
    
    try:
        with engine.begin() as conn:
            # Count records that would fail the INNER JOINs in the insertion query
            # This mirrors the exact logic used in step_5_insert_valid_records
            total_invalid_result = conn.execute(text("""
                SELECT COUNT(DISTINCT stg.membership_transactions_id) as count
                FROM mt_membership_transactions_details_dlk stg
                LEFT JOIN customers c ON stg.customer_id = c.customer_id  
                                    AND c.account_id = stg.account_id
                                    AND c.location_id = :location_id
                LEFT JOIN membership_instances mi ON stg.membership_instances_id = mi.membership_instances_id 
                                               AND stg.location = mi.location 
                                               AND stg.account_id = mi.account_id
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
                  AND (c.id IS NULL OR mi.id IS NULL)
            """), {"account_id": account_id, "location_id": location_id})
            total_invalid_transactions = total_invalid_result.fetchone()[0]
            
            # Count records with missing customer dependencies for detailed logging
            missing_customers_result = conn.execute(text("""
                SELECT COUNT(DISTINCT stg.membership_transactions_id) as count
                FROM mt_membership_transactions_details_dlk stg
                LEFT JOIN customers c ON stg.customer_id = c.customer_id  
                                    AND c.account_id = stg.account_id
                                    AND c.location_id = :location_id
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
                  AND c.id IS NULL
            """), {"account_id": account_id, "location_id": location_id})
            missing_customer_transactions = missing_customers_result.fetchone()[0]
            
            # Count records with missing membership_instances dependencies for detailed logging
            missing_instances_result = conn.execute(text("""
                SELECT COUNT(DISTINCT stg.membership_transactions_id) as count
                FROM mt_membership_transactions_details_dlk stg
                LEFT JOIN membership_instances mi ON stg.membership_instances_id = mi.membership_instances_id 
                                               AND stg.location = mi.location 
                                               AND stg.account_id = mi.account_id
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
                  AND mi.id IS NULL
            """), {"account_id": account_id, "location_id": location_id})
            missing_instance_transactions = missing_instances_result.fetchone()[0]
            
            # Count NULL customer_ids specifically
            null_customers_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_membership_transactions_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
                  AND stg.customer_id IS NULL
            """), {"account_id": account_id, "location_id": location_id})
            null_customer_transactions = null_customers_result.fetchone()[0]
            
            # Count NULL membership_instances_ids specifically
            null_instances_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_membership_transactions_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
                  AND stg.membership_instances_id IS NULL
            """), {"account_id": account_id, "location_id": location_id})
            null_instance_transactions = null_instances_result.fetchone()[0]
            
            # Count distinct missing customers (excluding NULLs)
            distinct_missing_customers_result = conn.execute(text("""
                SELECT COUNT(DISTINCT stg.customer_id) as count
                FROM mt_membership_transactions_details_dlk stg
                LEFT JOIN customers c ON stg.customer_id = c.customer_id  
                                    AND c.account_id = stg.account_id
                                    AND c.location_id = :location_id
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
                  AND stg.customer_id IS NOT NULL
                  AND c.id IS NULL
            """), {"account_id": account_id, "location_id": location_id})
            distinct_missing_customers = distinct_missing_customers_result.fetchone()[0]
            
            # Count distinct missing membership_instances (excluding NULLs)
            distinct_missing_instances_result = conn.execute(text("""
                SELECT COUNT(DISTINCT stg.membership_instances_id) as count
                FROM mt_membership_transactions_details_dlk stg
                LEFT JOIN membership_instances mi ON stg.membership_instances_id = mi.membership_instances_id 
                                               AND stg.location = mi.location 
                                               AND stg.account_id = mi.account_id
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.isin_reservation = true
                  AND stg.membership_instances_id IS NOT NULL
                  AND mi.id IS NULL
            """), {"account_id": account_id, "location_id": location_id})
            distinct_missing_instances = distinct_missing_instances_result.fetchone()[0]
        
        # Log detailed dependency information
        if total_invalid_transactions > 0:
            logger.warning(f"[STEP 3] Found {total_invalid_transactions} membership_transactions records with missing dependencies")
            logger.warning(f"[STEP 3] Customer dependency issues: {missing_customer_transactions} records")
            logger.warning(f"[STEP 3] Membership_instances dependency issues: {missing_instance_transactions} records")
            logger.warning(f"[STEP 3] NULL customer_id: {null_customer_transactions} records")
            logger.warning(f"[STEP 3] NULL membership_instances_id: {null_instance_transactions} records")
            logger.warning(f"[STEP 3] Distinct missing customers: {distinct_missing_customers}")
            logger.warning(f"[STEP 3] Distinct missing membership_instances: {distinct_missing_instances}")
            
            # Get sample of problematic records for debugging
            with engine.begin() as conn:
                sample_result = conn.execute(text("""
                    SELECT 
                        stg.membership_transactions_id,
                        stg.customer_id,
                        stg.membership_instances_id,
                        CASE 
                            WHEN stg.customer_id IS NULL THEN 'NULL_CUSTOMER'
                            WHEN c.id IS NULL THEN 'MISSING_CUSTOMER'
                            ELSE 'VALID_CUSTOMER'
                        END as customer_status,
                        CASE 
                            WHEN stg.membership_instances_id IS NULL THEN 'NULL_INSTANCE'
                            WHEN mi.id IS NULL THEN 'MISSING_INSTANCE'
                            ELSE 'VALID_INSTANCE'
                        END as instance_status
                    FROM mt_membership_transactions_details_dlk stg
                    LEFT JOIN customers c ON stg.customer_id = c.customer_id  
                                        AND c.account_id = stg.account_id
                                        AND c.location_id = :location_id
                    LEFT JOIN membership_instances mi ON stg.membership_instances_id = mi.membership_instances_id 
                                                   AND stg.location = mi.location 
                                                   AND stg.account_id = mi.account_id
                    WHERE stg.account_id = :account_id
                      AND stg.location = :location_id
                      AND stg.isin_reservation = true
                      AND (c.id IS NULL OR mi.id IS NULL)
                    ORDER BY stg.membership_transactions_id
                    LIMIT 10
                """), {"account_id": account_id, "location_id": location_id})
                sample_details = [(row[0], row[1], row[2], row[3], row[4]) for row in sample_result.fetchall()]
                logger.warning(f"[STEP 3] Sample problematic records (tx_id, customer_id, instance_id, customer_status, instance_status): {sample_details}")
        
        logger.info(f"[STEP 3] SUCCESS: Total invalid membership_transactions records: {total_invalid_transactions}")
        logger.info(f"[STEP 3] SUCCESS: Customer dependency issues: {missing_customer_transactions}")
        logger.info(f"[STEP 3] SUCCESS: Membership_instances dependency issues: {missing_instance_transactions}")
        logger.info(f"[STEP 3] SUCCESS: NULL customer_id records: {null_customer_transactions}")
        logger.info(f"[STEP 3] SUCCESS: NULL membership_instances_id records: {null_instance_transactions}")
        logger.info(f"[STEP 3] SUCCESS: Distinct missing customers: {distinct_missing_customers}")
        logger.info(f"[STEP 3] SUCCESS: Distinct missing membership_instances: {distinct_missing_instances}")
        
        return total_invalid_transactions
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count membership_transactions with missing dependencies: {e}")
        raise

async def step_5_calculate_expected_ready(account_id: str, location_id: int, total_staging_after_cleanup: int, already_exist_in_main: int, invalid_dependency_records: int):
    """
    Step 5: Calculate expected records ready for insertion
    Formula: total_staging_after_cleanup - already_exist_in_main - invalid_dependency_records
    """
    logger.info(f"[STEP 5] Calculating expected records ready for insertion")
    
    expected_ready = total_staging_after_cleanup - already_exist_in_main - invalid_dependency_records
    logger.info(f"[STEP 5] SUCCESS: Expected ready for insertion: {expected_ready} " +
                f"(total_after_cleanup: {total_staging_after_cleanup}, already_exist: {already_exist_in_main}, invalid_dependencies: {invalid_dependency_records})")
    
    return expected_ready

async def step_6_count_records_to_insert(account_id: str, location_id: int, engine):
    """
    Step 6: Count records that are actually ready for insertion (validation before insert)
    This should match the expected_ready count - if not, there's a logic error
    """
    logger.info(f"[STEP 6] Counting records ready for insertion (validation)")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_membership_transactions_details_dlk mt
                INNER JOIN customers c ON mt.customer_id = c.customer_id  
                                    and c.account_id = :account_id
                                    and c.location_id = :location_id
                INNER JOIN membership_instances mi ON mt.membership_instances_id = mi.membership_instances_id 
                        and mt.location = mi.location 
                        and mt.account_id = mi.account_id
                        and mi.account_id = :account_id
                        and mi.location = :location_id
                WHERE mt.account_id = :account_id
                  AND mt.location = :location_id
                  AND mt.isin_reservation = true
                  AND mt.membership_transactions_id NOT IN (
                    SELECT membership_transactions_id FROM public.membership_transactions WHERE account_id = :account_id AND location = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            insert_ready_count = result.fetchone()[0]
            
        logger.info(f"[STEP 6] SUCCESS: Records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to count records ready for insertion: {e}")
        raise

async def step_7_insert_valid_records(account_id: str, location_id: int, engine):
    """
    Step 6: Insert valid membership_transactions records (non-duplicates with valid dependencies)
    """
    logger.info(f"[STEP 6] Inserting valid membership_transactions records")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO public.membership_transactions (
                  membership_transactions_id, transaction_date, membership_name, parent_membership_transaction_id,
                  membership_instances_id, customer_id, location, created_at, created_by, updated_at,
                  updated_by, deleted_at, deleted_by, account_id, customer_ref_id, membership_instances_ref_id
                )
                SELECT  
                  mt.membership_transactions_id, 
                  mt.transaction_date, mt.membership_name, mt.parent_membership_transaction_id,
                  mt.membership_instances_id AS membership_instances_id, 
                  mt.customer_id, mt.location, now() created_at, 1 as created_by,
                  now() updated_at, mt.updated_by, mt.deleted_at, mt.deleted_by, mt.account_id, c.id as customer_ref_id,
                  mi.id as membership_instances_ref_id 
                FROM mt_membership_transactions_details_dlk mt
                INNER JOIN customers c ON mt.customer_id = c.customer_id  
                                    and c.account_id = :account_id
                                    and c.location_id = :location_id
                INNER JOIN membership_instances mi ON mt.membership_instances_id = mi.membership_instances_id 
                        and mt.location = mi.location 
                        and mt.account_id = mi.account_id
                        and mi.account_id = :account_id
                        and mi.location = :location_id
                WHERE mt.account_id = :account_id
                  AND mt.location = :location_id
                  AND mt.isin_reservation = true
                  AND mt.membership_transactions_id NOT IN (
                    SELECT membership_transactions_id FROM public.membership_transactions WHERE account_id = :account_id AND location = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            inserted_count = result.rowcount
        
        logger.info(f"[STEP 6] SUCCESS: Inserted {inserted_count} membership_transactions records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to insert membership_transactions records: {e}")
        raise

async def process_membership_transactions(account_id: str, location_id: int, engine):
    """
    Main process to handle membership_transactions ETL pipeline with dual dependency validation
    7-step process (0-6): Drop duplicates, count records, validate dependencies, insert data
    """
    logger.info(f"[process_membership_transactions] Starting ETL process for account {account_id}, location {location_id}")
    
    try:
        # Step 0: Drop duplicates in staging table
        try:
            step_0_result = await step_0_drop_duplicates_in_staging(account_id, location_id, engine)
            duplicates_found = step_0_result["duplicates_found"]
            duplicates_removed = step_0_result["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Count total records after cleanup
        try:
            total_records_result = await step_1_count_total_records(account_id, location_id, engine)
            total_staging_after_cleanup = total_records_result["total_staging_after_cleanup"]
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 1b: Count unnecessary records (isin_reservation = false)
        try:
            unnecessary_records = await step_1b_count_unnecessary_records(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 1b failed: {e}")
            raise
        
        # Step 2: Count records already existing in main table  
        try:
            already_exist_in_main = await step_2_count_existing_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Verify count of existing records (alternative method)
        try:
            already_exist_verification = await step_3_count_records_already_exist_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 3 failed: {e}")
            raise
        
        # Step 4: Count records with missing dependencies
        try:
            invalid_dependency_records = await step_4_count_transactions_with_missing_dependencies(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Calculate expected records for insertion (with unnecessary records filtered out)
        try:
            expected_ready = await step_5_calculate_expected_ready(account_id, location_id, total_staging_after_cleanup - unnecessary_records, already_exist_in_main, invalid_dependency_records)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 5 failed: {e}")
            raise
        
        # Step 6: Count records ready for insertion (validation before insert)
        if expected_ready > 0:
            try:
                insert_ready_count = await step_6_count_records_to_insert(account_id, location_id, engine)
            except Exception as e:
                logger.error(f"[process_membership_transactions] ERROR: Step 6 failed: {e}")
                raise
            
            # Validate that insert_ready_count matches expected_ready
            if insert_ready_count != expected_ready:
                logger.error(f"[process_membership_transactions] ERROR: Validation failed - expected {expected_ready} but found {insert_ready_count} ready for insertion")
                raise Exception(f"Insert ready count mismatch: expected {expected_ready}, actual {insert_ready_count}")
            
            logger.info(f"[process_membership_transactions] SUCCESS: Validation passed - {insert_ready_count} records ready for insertion as expected")
            
            # Step 7: Insert valid records (commented out for now)
            # try:
            #     inserted_records = await step_7_insert_valid_records(account_id, location_id, engine)
            # except Exception as e:
            #     logger.error(f"[process_membership_transactions] ERROR: Step 7 failed: {e}")
            #     raise
        else:
            logger.warning(f"[process_membership_transactions] WARNING: No records to insert (expected_ready: {expected_ready})")
            insert_ready_count = 0
        
        # Calculate missing percentage: (total - already_existing - inserted) / total * 100
        # Note: No records actually inserted since step 7 is commented out
        inserted_records = 0
        total_not_inserted = total_staging_after_cleanup - already_exist_in_main - inserted_records
        missing_percentage = round((total_not_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        
        # Summary
        logger.info(f"[process_membership_transactions] PROCESS COMPLETE:")
        logger.info(f"  - Duplicates found in staging: {duplicates_found}")
        logger.info(f"  - Duplicates removed: {duplicates_removed}")
        logger.info(f"  - Total staging records after cleanup: {total_staging_after_cleanup}")        
        logger.info(f"  - Unnecessary records (isin_reservation = false): {unnecessary_records} ({round((unnecessary_records / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0}%)")
        logger.info(f"  - Records already exist in main table: {already_exist_in_main}")
        logger.info(f"  - Records with missing dependencies: {invalid_dependency_records}")
        logger.info(f"  - Records not inserted (missing deps + validation failures): {total_not_inserted} ({missing_percentage}%)")
        logger.info(f"  - Records ready for insertion: {insert_ready_count}")
        
        return {
            "duplicates_found": duplicates_found,
            "duplicates_removed": duplicates_removed,
            "total_staging_after_cleanup": total_staging_after_cleanup,
            "unnecessary_records": unnecessary_records,
            "already_exist_in_main_table": already_exist_in_main,
            "invalid_dependency_records": invalid_dependency_records,
            "missing_dependency_percentage": missing_percentage,
            "insert_ready_count": insert_ready_count
        }
        
    except Exception as e:
        logger.error(f"[process_membership_transactions] FATAL ERROR: {e}")
        raise
