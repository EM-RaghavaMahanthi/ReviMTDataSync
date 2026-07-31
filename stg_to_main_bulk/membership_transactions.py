"""
Membership Transactions Service - Step 5 of ETL Pipeline
Processes membership_transactions data from staging to main table with dual dependency validation
Dependencies: customers table and membership_instances table
"""

import logging
from sqlalchemy import text
from stg_to_main_bulk._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)


async def step_1_count_total_records(account_id: str, engine):
    """
    Step 1: Count total records in staging table after Step 0 cleanup
    """
    logger.info(f"[STEP 1] Counting total records in stg_membership_transactions_bulk (after cleanup)")

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM stg_membership_transactions_bulk
                WHERE account_id = :account_id
            """), {"account_id": account_id})
            total_count = result.scalar()
            logger.info(f"[STEP 1] SUCCESS: Total staging records after cleanup: {total_count}")
            
        return {"total_staging_after_cleanup": total_count}
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count total records: {e}")
        raise

async def step_1b_count_unnecessary_records(account_id: str, engine):
    """
    Step 1b: always 0 in the bulk pipeline — kept because process_membership_transactions's
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
    Step 2: Count membership_transactions records already existing in main table (only reservation related)
    """
    logger.info(f"[STEP 2] Counting membership_transactions records already in main table")

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_membership_transactions_bulk stg
                WHERE stg.account_id = :account_id
                  AND EXISTS (
                    SELECT 1 FROM membership_transactions mt
                    WHERE mt.membership_transactions_id = stg.membership_transactions_id
                      AND mt.account_id = :account_id
                  )
            """), {"account_id": account_id})
            existing_count = result.fetchone()[0]
        
        logger.info(f"[STEP 2] SUCCESS: Records already exist in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing membership_transactions: {e}")
        raise

async def step_4_count_transactions_with_missing_dependencies(account_id: str, engine):
    """
    Step 4: Count membership_transactions records with missing customer or membership_instances dependencies
    OPTIMIZED: Single query with LEFT JOINs instead of 7 separate queries
    """
    logger.info(f"[STEP 4] Counting membership_transactions records with missing dependencies")
    
    try:
        with engine.connect() as conn:
            # Single query gets ALL metrics at once
            result = conn.execute(text("""
                WITH dependency_analysis AS (
                    SELECT 
                        stg.membership_transactions_id,
                        stg.customer_id,
                        stg.membership_instances_id,
                        c.id as customer_ref_id,
                        mi.id as membership_instance_ref_id,
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
                    FROM stg_membership_transactions_bulk stg
                    LEFT JOIN customers c ON c.customer_id = stg.customer_id  
                        AND c.account_id = stg.account_id
                    LEFT JOIN membership_instances mi ON mi.membership_instances_id = stg.membership_instances_id 
                        AND mi.location = stg.location 
                        AND mi.account_id = stg.account_id
                    WHERE stg.account_id = :account_id
                )
                SELECT 
                    -- Total invalid (missing either customer or instance)
                    COUNT(DISTINCT CASE WHEN customer_ref_id IS NULL OR membership_instance_ref_id IS NULL 
                                   THEN membership_transactions_id END) as total_invalid_transactions,
                    
                    -- Missing customer dependencies
                    COUNT(DISTINCT CASE WHEN customer_ref_id IS NULL 
                                   THEN membership_transactions_id END) as missing_customer_transactions,
                    
                    -- Missing instance dependencies
                    COUNT(DISTINCT CASE WHEN membership_instance_ref_id IS NULL 
                                   THEN membership_transactions_id END) as missing_instance_transactions,
                    
                    -- NULL customer_ids
                    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_customer_transactions,
                    
                    -- NULL membership_instances_ids
                    COUNT(CASE WHEN membership_instances_id IS NULL THEN 1 END) as null_instance_transactions,
                    
                    -- Distinct missing customers (excluding NULLs)
                    COUNT(DISTINCT CASE WHEN customer_id IS NOT NULL AND customer_ref_id IS NULL 
                                   THEN customer_id END) as distinct_missing_customers,
                    
                    -- Distinct missing instances (excluding NULLs)
                    COUNT(DISTINCT CASE WHEN membership_instances_id IS NOT NULL AND membership_instance_ref_id IS NULL 
                                   THEN membership_instances_id END) as distinct_missing_instances,
                    
                    -- Sample problematic records (JSON)
                    (SELECT json_agg(json_build_object(
                        'membership_transactions_id', membership_transactions_id,
                        'customer_id', customer_id,
                        'membership_instances_id', membership_instances_id,
                        'customer_status', customer_status,
                        'instance_status', instance_status
                    ))
                     FROM (
                        SELECT membership_transactions_id, customer_id, membership_instances_id, 
                               customer_status, instance_status
                        FROM dependency_analysis
                        WHERE customer_ref_id IS NULL OR membership_instance_ref_id IS NULL
                        ORDER BY membership_transactions_id
                        LIMIT 10
                     ) sample) as sample_problematic_records
                FROM dependency_analysis
            """), {"account_id": account_id})
            
            row = result.fetchone()
            total_invalid_transactions = int(row[0])
            missing_customer_transactions = int(row[1])
            missing_instance_transactions = int(row[2])
            null_customer_transactions = int(row[3])
            null_instance_transactions = int(row[4])
            distinct_missing_customers = int(row[5])
            distinct_missing_instances = int(row[6])
            sample_problematic_records = row[7]  # JSON array
        
        # Log detailed dependency information
        if total_invalid_transactions > 0:
            logger.warning(f"[STEP 4] Found {total_invalid_transactions} membership_transactions records with missing dependencies")
            logger.warning(f"[STEP 4] Customer dependency issues: {missing_customer_transactions} records")
            logger.warning(f"[STEP 4] Membership_instances dependency issues: {missing_instance_transactions} records")
            logger.warning(f"[STEP 4] NULL customer_id: {null_customer_transactions} records")
            logger.warning(f"[STEP 4] NULL membership_instances_id: {null_instance_transactions} records")
            logger.warning(f"[STEP 4] Distinct missing customers: {distinct_missing_customers}")
            logger.warning(f"[STEP 4] Distinct missing membership_instances: {distinct_missing_instances}")
            
            # Parse and log sample (already fetched from main query)
            if sample_problematic_records:
                import json
                # Handle both cases: JSON string or already parsed list
                if isinstance(sample_problematic_records, str):
                    sample_data = json.loads(sample_problematic_records)
                elif isinstance(sample_problematic_records, list):
                    sample_data = sample_problematic_records
                else:
                    sample_data = []
                
                if sample_data:
                    sample_details = [
                        (item['membership_transactions_id'], item['customer_id'], 
                         item['membership_instances_id'], item['customer_status'], 
                         item['instance_status'])
                        for item in sample_data
                    ]
                    logger.warning(f"[STEP 4] Sample problematic records (tx_id, customer_id, instance_id, customer_status, instance_status): {sample_details}")
        
        logger.info(f"[STEP 4] SUCCESS: Total invalid membership_transactions records: {total_invalid_transactions}")
        logger.info(f"[STEP 4] SUCCESS: Customer dependency issues: {missing_customer_transactions}")
        logger.info(f"[STEP 4] SUCCESS: Membership_instances dependency issues: {missing_instance_transactions}")
        logger.info(f"[STEP 4] SUCCESS: NULL customer_id records: {null_customer_transactions}")
        logger.info(f"[STEP 4] SUCCESS: NULL membership_instances_id records: {null_instance_transactions}")
        logger.info(f"[STEP 4] SUCCESS: Distinct missing customers: {distinct_missing_customers}")
        logger.info(f"[STEP 4] SUCCESS: Distinct missing membership_instances: {distinct_missing_instances}")
        
        return total_invalid_transactions
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count membership_transactions with missing dependencies: {e}")
        raise

async def step_5_calculate_expected_ready(account_id: str, total_staging_after_cleanup: int, already_exist_in_main: int, invalid_dependency_records: int):
    """
    Step 5: Calculate expected records ready for insertion
    Formula: total_staging_after_cleanup - already_exist_in_main - invalid_dependency_records
    """
    logger.info(f"[STEP 5] Calculating expected records ready for insertion")
    
    expected_ready = total_staging_after_cleanup - already_exist_in_main - invalid_dependency_records
    logger.info(f"[STEP 5] SUCCESS: Expected ready for insertion: {expected_ready} " +
                f"(total_after_cleanup: {total_staging_after_cleanup}, already_exist: {already_exist_in_main}, invalid_dependencies: {invalid_dependency_records})")
    
    return expected_ready

async def step_6_count_records_to_insert(account_id: str, engine):
    """
    Step 6: Count records that are actually ready for insertion (validation before insert)
    """
    logger.info(f"[STEP 6] Counting records ready for insertion (validation)")

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_membership_transactions_bulk mt
                WHERE mt.account_id = :account_id
                  AND EXISTS (
                      SELECT 1 
                      FROM customers c 
                      WHERE c.customer_id = mt.customer_id  
                        AND c.account_id = :account_id
                  )
                  AND EXISTS (
                      SELECT 1 
                      FROM membership_instances mi 
                      WHERE mi.membership_instances_id = mt.membership_instances_id 
                        AND mi.location = mt.location 
                        AND mi.account_id = :account_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM public.membership_transactions mtr
                      WHERE mtr.membership_transactions_id = mt.membership_transactions_id
                        AND mtr.account_id = :account_id
                  )
            """), {"account_id": account_id})
            insert_ready_count = result.fetchone()[0]
            
        logger.info(f"[STEP 6] SUCCESS: Records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to count records ready for insertion: {e}")
        raise

async def step_7_insert_valid_records(account_id: str, engine):
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
                FROM stg_membership_transactions_bulk mt
                INNER JOIN customers c ON mt.customer_id = c.customer_id  
                                    and c.account_id = :account_id
                INNER JOIN membership_instances mi ON mt.membership_instances_id = mi.membership_instances_id 
                        and mt.location = mi.location 
                        and mt.account_id = mi.account_id
                        and mi.account_id = :account_id
                WHERE mt.account_id = :account_id
                  AND mt.membership_transactions_id NOT IN (
                    SELECT membership_transactions_id FROM public.membership_transactions WHERE account_id = :account_id
                  )
            """), {"account_id": account_id})
            inserted_count = result.rowcount
        
        logger.info(f"[STEP 6] SUCCESS: Inserted {inserted_count} membership_transactions records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to insert membership_transactions records: {e}")
        raise

async def process_membership_transactions(account_id: str, engine):
    """
    Main process to handle membership_transactions ETL pipeline with dual dependency validation
    7-step process (0-6): Drop duplicates, count records, validate dependencies, insert data
    """
    logger.info(f"[process_membership_transactions] Starting ETL process for account {account_id}")
    
    try:
        # Step 0: Drop duplicates in staging table
        try:
            step_0_result = drop_staging_duplicates(engine, "stg_membership_transactions_bulk", "membership_transactions_id", account_id)
            duplicates_found = step_0_result["duplicates_found"]
            duplicates_removed = step_0_result["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Count total records after cleanup
        try:
            total_records_result = await step_1_count_total_records(account_id, engine)
            total_staging_after_cleanup = total_records_result["total_staging_after_cleanup"]
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 1b: Count unnecessary records (always 0 in bulk)
        try:
            unnecessary_records = await step_1b_count_unnecessary_records(account_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 1b failed: {e}")
            raise
        
        # Step 2: Count records already existing in main table  
        try:
            already_exist_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 4: Count records with missing dependencies
        try:
            invalid_dependency_records = await step_4_count_transactions_with_missing_dependencies(account_id, engine)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Calculate expected records for insertion (with unnecessary records filtered out)
        try:
            expected_ready = await step_5_calculate_expected_ready(account_id, total_staging_after_cleanup - unnecessary_records, already_exist_in_main, invalid_dependency_records)
        except Exception as e:
            logger.error(f"[process_membership_transactions] ERROR: Step 5 failed: {e}")
            raise
        
        # Step 6: Count records ready for insertion (validation before insert)
        if expected_ready > 0:
            try:
                insert_ready_count = await step_6_count_records_to_insert(account_id, engine)
            except Exception as e:
                logger.error(f"[process_membership_transactions] ERROR: Step 6 failed: {e}")
                raise
            
            # Validate that insert_ready_count matches expected_ready
            if insert_ready_count != expected_ready:
                logger.error(f"[process_membership_transactions] ERROR: Validation failed - expected {expected_ready} but found {insert_ready_count} ready for insertion")
                raise Exception(f"Insert ready count mismatch: expected {expected_ready}, actual {insert_ready_count}")
            
            logger.info(f"[process_membership_transactions] SUCCESS: Validation passed - {insert_ready_count} records ready for insertion as expected")
            
            # Step 7: Insert valid records
            try:
                inserted_records = await step_7_insert_valid_records(account_id, engine)
            except Exception as e:
                logger.error(f"[process_membership_transactions] ERROR: Step 7 failed: {e}")
                raise
        else:
            logger.warning(f"[process_membership_transactions] WARNING: No records to insert (expected_ready: {expected_ready})")
            insert_ready_count = 0
            inserted_records = 0

        # Calculate missing percentage
        total_not_inserted = total_staging_after_cleanup - already_exist_in_main - inserted_records
        missing_percentage = round((total_not_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        
        # Summary
        logger.info(f"[process_membership_transactions] PROCESS COMPLETE:")
        logger.info(f"  - Duplicates found in staging: {duplicates_found}")
        logger.info(f"  - Duplicates removed: {duplicates_removed}")
        logger.info(f"  - Total staging records after cleanup: {total_staging_after_cleanup}")        
        logger.info(f"  - Unnecessary records (always 0 in bulk): {unnecessary_records} ({round((unnecessary_records / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0}%)")
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
            "insert_ready_count": insert_ready_count,
            "inserted_records": inserted_records
        }
        
    except Exception as e:
        logger.error(f"[process_membership_transactions] FATAL ERROR: {e}")
        raise
