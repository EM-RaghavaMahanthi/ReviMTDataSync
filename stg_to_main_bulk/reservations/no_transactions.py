"""
Reservations No Transactions Service - Step 8c of ETL Pipeline
Processes reservations that are NOT linked to any transactions (both credit_transactions_id and membership_transactions_id are NULL)
Dependencies: customers, class_sessions tables
"""

import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def step_1_count_total_no_transactions_reservations(account_id: str, engine):
    """
    Step 1: Count total no-transactions reservation records in staging (no duplicates needed - already done in service 08)
    """
    logger.info(f"[STEP 1] Counting total no-transactions reservation records in staging")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_reservations_bulk
                WHERE account_id = :account_id
                  AND credit_transactions_id IS NULL
                  AND membership_transactions_id IS NULL
            """), {"account_id": account_id})
            total_count = result.fetchone()[0]
        
        logger.info(f"[STEP 1] SUCCESS: Total no-transactions reservation records: {total_count}")
        return {"total_no_transactions_reservations": total_count}
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count total no-transactions reservation records: {e}")
        raise

async def step_2_count_existing_in_main_table(account_id: str, engine):
    """
    Step 2: Count no-transactions reservation records already existing in main table
    Uses same JOIN conditions as insert logic to ensure accurate counting
    """
    logger.info(f"[STEP 2] Counting no-transactions reservation records already in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT stg.reservations_id) as count
                FROM stg_reservations_bulk stg
                INNER JOIN customers c
                  ON stg.customer_id = c.customer_id
                  AND c.account_id = :account_id
                INNER JOIN class_sessions cs
                  ON stg.class_session_id = cs.class_session_id
                  AND cs.account_id = :account_id
                WHERE stg.account_id = :account_id
                  AND stg.credit_transactions_id IS NULL
                  AND stg.membership_transactions_id IS NULL
                  AND stg.reservations_id IN (
                    SELECT reservations_id FROM reservations 
                    WHERE account_id = :account_id 
                  )
            """), {"account_id": account_id})
            existing_count = result.fetchone()[0]
        
        logger.info(f"[STEP 2] SUCCESS: No-transactions reservation records already exist in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing no-transactions reservation records: {e}")
        raise

async def step_3_count_records_with_missing_dependencies(account_id: str, engine):
    """
    Step 3: Count no-transactions reservation records with missing dependencies (invalid for insertion)
    Dependencies: customers, class_sessions tables
    """
    logger.info(f"[STEP 3] Counting no-transactions reservation records with missing dependencies")
    
    try:
        with engine.begin() as conn:
            # Count records with missing customers
            missing_customers_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_reservations_bulk stg
                WHERE stg.account_id = :account_id
                  AND stg.credit_transactions_id IS NULL
                  AND stg.membership_transactions_id IS NULL
                  AND (
                    stg.customer_id IS NULL
                    OR NOT EXISTS (
                      SELECT 1 FROM customers c 
                      WHERE c.customer_id = stg.customer_id 
                        AND c.account_id = :account_id
                    )
                  )
            """), {"account_id": account_id})
            missing_customers_count = missing_customers_result.fetchone()[0]
            
            # Count records with missing class_sessions
            missing_class_sessions_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_reservations_bulk stg
                WHERE stg.account_id = :account_id
                  AND stg.credit_transactions_id IS NULL
                  AND stg.membership_transactions_id IS NULL
                  AND (
                    stg.class_session_id IS NULL
                    OR NOT EXISTS (
                      SELECT 1 FROM class_sessions cs 
                      WHERE cs.class_session_id = stg.class_session_id 
                        AND cs.account_id = :account_id
                    )
                  )
            """), {"account_id": account_id})
            missing_class_sessions_count = missing_class_sessions_result.fetchone()[0]
            
            # Use UNION to count unique records with ANY missing dependency (avoid double-counting)
            total_missing_deps_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM (
                    SELECT DISTINCT stg.reservations_id
                    FROM stg_reservations_bulk stg
                    WHERE stg.account_id = :account_id
                      AND stg.credit_transactions_id IS NULL
                      AND stg.membership_transactions_id IS NULL
                      AND (
                        stg.customer_id IS NULL
                        OR NOT EXISTS (
                          SELECT 1 FROM customers c 
                          WHERE c.customer_id = stg.customer_id 
                            AND c.account_id = :account_id
                        )
                        OR stg.class_session_id IS NULL
                        OR NOT EXISTS (
                          SELECT 1 FROM class_sessions cs 
                          WHERE cs.class_session_id = stg.class_session_id 
                            AND cs.account_id = :account_id
                        )
                      )
                ) unique_missing
            """), {"account_id": account_id})
            total_missing_deps_count = total_missing_deps_result.fetchone()[0]
        
        logger.info(f"[STEP 3] SUCCESS: Missing dependencies breakdown:")
        logger.info(f"  - Records with missing customers: {missing_customers_count}")
        logger.info(f"  - Records with missing class_sessions: {missing_class_sessions_count}")
        logger.info(f"  - Total unique records with any missing dependency: {total_missing_deps_count}")
        
        return total_missing_deps_count
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count missing dependencies: {e}")
        raise

async def step_4_count_records_ready_for_insertion(account_id: str, engine):
    """
    Step 4: Count no-transactions reservation records ready for insertion (have all dependencies and not already in main table)
    OPTIMIZED: Uses EXISTS/NOT EXISTS for better performance
    """
    logger.info(f"[STEP 4] Counting no-transactions reservation records ready for insertion")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT stg.reservations_id) as count
                FROM stg_reservations_bulk stg
                WHERE stg.account_id = :account_id
                  AND stg.credit_transactions_id IS NULL
                  AND stg.membership_transactions_id IS NULL
                  AND EXISTS (
                    SELECT 1 FROM customers c
                    WHERE c.customer_id = stg.customer_id
                      AND c.account_id = :account_id
                  )
                  AND EXISTS (
                    SELECT 1 FROM class_sessions cs
                    WHERE cs.class_session_id = stg.class_session_id
                      AND cs.account_id = :account_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM public.reservations r
                    WHERE r.reservations_id = stg.reservations_id
                      AND r.account_id = :account_id
                  )
            """), {"account_id": account_id})
            ready_count = result.fetchone()[0]
        
        logger.info(f"[STEP 4] SUCCESS: No-transactions reservation records ready for insertion: {ready_count}")
        return ready_count
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count records ready for insertion: {e}")
        raise


async def step_5_insert_new_records(account_id: str, engine):
    """
    Step 5: Insert new no-transactions reservation records into main table
    OPTIMIZED: Uses NOT EXISTS for better performance
    """
    logger.info(f"[STEP 5] Inserting new no-transactions reservation records")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO public.reservations (
                  reservations_id, cancel_date, check_in_date, creation_date, status, credit_transactions_type,
                  credit_transactions_id, membership_transactions_type, membership_transactions_id, guest, customer_id,
                  location, created_at, created_by, updated_at, updated_by, deleted_at, deleted_by, first_timer,
                  class_session_id, reservation_type, account_id, class_session_ref_id, credit_transactions_ref_id,
                  customer_ref_id, membership_transactions_ref_id, prev_reservation_type, transaction_type
                )
                SELECT 
                  stg.reservations_id, stg.cancel_date, stg.check_in_date, stg.creation_date, stg.status, stg.credit_transactions_type,
                  stg.credit_transactions_id, stg.membership_transactions_type, stg.membership_transactions_id, stg.guest, stg.customer_id,
                  stg.location, now() AS created_at, 1 AS created_by, now() AS updated_at, stg.updated_by, stg.deleted_at, stg.deleted_by,
                  stg.first_timer, stg.class_session_id, stg.reservation_type, stg.account_id, cs.id AS class_session_ref_id,
                  NULL AS credit_transactions_ref_id, c.id AS customer_ref_id, NULL AS membership_transactions_ref_id, 
                  stg.reservation_type as prev_reservation_type, NULL AS transaction_type
                FROM stg_reservations_bulk stg
                INNER JOIN customers c
                  ON c.customer_id = stg.customer_id
                  AND c.account_id = :account_id
                INNER JOIN class_sessions cs
                  ON cs.class_session_id = stg.class_session_id
                  AND cs.account_id = :account_id
                WHERE stg.account_id = :account_id
                  AND stg.credit_transactions_id IS NULL
                  AND stg.membership_transactions_id IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM public.reservations r
                    WHERE r.reservations_id = stg.reservations_id
                      AND r.account_id = :account_id
                  )
            """), {"account_id": account_id})
            
            inserted_count = result.rowcount
        
        logger.info(f"[STEP 5] SUCCESS: Inserted {inserted_count} no-transactions reservation records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to insert new records: {e}")
        raise

async def process_reservations_no_transactions(account_id: str, engine):
    """
    Main function to process no-transactions reservations data
    5-step process: count records, validate dependencies, insert data
    Dependencies: customers, class_sessions tables
    """
    logger.info(f"[process_reservations_no_transactions] Starting processing for account_id={account_id}")
    
    try:
        # Step 1: Count total no-transactions reservations
        try:
            total_records_result = await step_1_count_total_no_transactions_reservations(account_id, engine)
            total_no_transactions_reservations = total_records_result["total_no_transactions_reservations"]
        except Exception as e:
            logger.error(f"[process_reservations_no_transactions] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 2: Count records already existing in main table  
        try:
            already_exist_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        except Exception as e:
            logger.error(f"[process_reservations_no_transactions] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Count records with missing dependencies
        try:
            invalid_dependency_records = await step_3_count_records_with_missing_dependencies(account_id, engine)
        except Exception as e:
            logger.error(f"[process_reservations_no_transactions] ERROR: Step 3 failed: {e}")
            raise
        
        # Calculate expected records for insertion
        expected_ready = total_no_transactions_reservations - already_exist_in_main - invalid_dependency_records
        
        # Step 4: Count records ready for insertion (validation before insert)
        if expected_ready > 0:
            try:
                insert_ready_count = await step_4_count_records_ready_for_insertion(account_id, engine)
            except Exception as e:
                logger.error(f"[process_reservations_no_transactions] ERROR: Step 4 failed: {e}")
                raise
            
            # Validate that insert_ready_count matches expected_ready
            if insert_ready_count != expected_ready:
                logger.error(f"[process_reservations_no_transactions] ERROR: Validation failed - expected {expected_ready} but found {insert_ready_count} ready for insertion")
                raise Exception(f"Insert ready count mismatch: expected {expected_ready}, actual {insert_ready_count}")
            
            logger.info(f"[process_reservations_no_transactions] SUCCESS: Validation passed - {insert_ready_count} records ready for insertion as expected")
            
            # Step 5: Insert new records
            try:
                inserted_records = await step_5_insert_new_records(account_id, engine)
            except Exception as e:
                logger.error(f"[process_reservations_no_transactions] ERROR: Step 5 failed: {e}")
                raise
            
            # Final validation that insertion count matches expected
            if inserted_records == expected_ready:
                logger.info(f"[process_reservations_no_transactions] SUCCESS: Final validation passed - inserted {inserted_records} records as expected")
            else:
                logger.error(f"[process_reservations_no_transactions] ERROR: Final validation failed - expected {expected_ready} but inserted {inserted_records}")
                raise Exception(f"Final insertion count mismatch: expected {expected_ready}, actual {inserted_records}")
        else:
            logger.warning(f"[process_reservations_no_transactions] WARNING: No records to insert (expected_ready: {expected_ready})")
            insert_ready_count = 0
            inserted_records = 0
        
        # Calculate missing percentage: (total - already_existing - inserted) / total * 100
        total_not_inserted = total_no_transactions_reservations - already_exist_in_main - inserted_records
        missing_percentage = round((total_not_inserted / total_no_transactions_reservations * 100), 2) if total_no_transactions_reservations > 0 else 0.0
        
        # Summary
        logger.info(f"[process_reservations_no_transactions] PROCESS COMPLETE:")
        logger.info(f"  - Total no-transactions reservations: {total_no_transactions_reservations}")
        logger.info(f"  - Records already exist in main table: {already_exist_in_main}")
        logger.info(f"  - Records with missing dependencies: {invalid_dependency_records}")
        logger.info(f"  - Records not inserted (missing deps + validation failures): {total_not_inserted} ({missing_percentage}%)")
        logger.info(f"  - Records ready for insertion: {insert_ready_count}")
        logger.info(f"  - Actually inserted: {inserted_records}")
        
        return {
            "transaction_type": "NoTransaction",
            "total_no_transactions_reservations": total_no_transactions_reservations,
            "already_exist_in_main_table": already_exist_in_main,
            "invalid_dependency_records": invalid_dependency_records,
            "missing_dependency_percentage": missing_percentage,
            "insert_ready_count": insert_ready_count,
            "inserted_records": inserted_records
        }
        
    except Exception as e:
        logger.error(f"[process_reservations_no_transactions] FATAL ERROR: {e}")
        raise
