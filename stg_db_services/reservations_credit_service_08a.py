"""
Reservations Credit Service - Step 8a of ETL Pipeline
Processes credit_transactions reservations from staging to main table
Dependencies: customers, class_sessions, credit_transactions tables
"""

import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def step_1_count_total_credit_reservations(account_id: str, location_id: int, engine):
    """
    Step 1: Count total credit_transactions reservation records in staging (no duplicates needed - already done in service 08)
    """
    logger.info(f"[STEP 1] Counting total credit_transactions reservation records in staging")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_reservations_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
                  AND credit_transactions_id IS NOT NULL
            """), {"account_id": account_id, "location_id": str(location_id)})
            total_count = result.fetchone()[0]
        
        logger.info(f"[STEP 1] SUCCESS: Total credit_transactions reservation records: {total_count}")
        return {"total_credit_reservations": total_count}
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count total credit_transactions reservation records: {e}")
        raise

async def step_2_count_existing_in_main_table(account_id: str, location_id: int, engine):
    """
    Step 2: Count credit_transactions reservation records already existing in main table
    Uses same JOIN conditions as insert logic to ensure accurate counting
    """
    logger.info(f"[STEP 2] Counting credit_transactions reservation records already in main table")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_reservations_details_dlk stg
                INNER JOIN customers c
                  ON stg.customer_id = c.customer_id
                  AND c.account_id = :account_id
                  AND c.location_id = :location_id
                INNER JOIN class_sessions cs
                  ON stg.class_session_id = cs.class_session_id
                  AND cs.account_id = :account_id
                  AND cs.location = :location_id
                INNER JOIN credit_transactions ct
                  ON stg.credit_transactions_id = ct.credit_transactions_id
                  AND ct.account_id = :account_id
                  AND ct.location = :location_id
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.credit_transactions_id IS NOT NULL
                  AND stg.reservations_id IN (
                    SELECT reservations_id FROM reservations 
                    WHERE account_id = :account_id 
                      AND location = :location_id
                  )
            """), {"account_id": account_id, "location_id": str(location_id)})
            existing_count = result.fetchone()[0]
        
        logger.info(f"[STEP 2] SUCCESS: Credit_transactions reservation records already exist in main table: {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing credit_transactions reservation records: {e}")
        raise

async def step_3_count_records_with_missing_dependencies(account_id: str, location_id: int, engine):
    """
    Step 3: Count credit_transactions reservation records with missing dependencies (invalid for insertion)
    Dependencies: customers, class_sessions, credit_transactions tables
    """
    logger.info(f"[STEP 3] Counting credit_transactions reservation records with missing dependencies")
    
    try:
        with engine.begin() as conn:
            # Count records with missing customers
            missing_customers_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_reservations_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.credit_transactions_id IS NOT NULL
                  AND (
                    stg.customer_id IS NULL
                    OR NOT EXISTS (
                      SELECT 1 FROM customers c 
                      WHERE c.customer_id = stg.customer_id 
                        AND c.account_id = :account_id
                        AND c.location_id = :location_id
                    )
                  )
            """), {"account_id": account_id, "location_id": str(location_id)})
            missing_customers_count = missing_customers_result.fetchone()[0]
            
            # Count records with missing class_sessions
            missing_class_sessions_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_reservations_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.credit_transactions_id IS NOT NULL
                  AND (
                    stg.class_session_id IS NULL
                    OR NOT EXISTS (
                      SELECT 1 FROM class_sessions cs 
                      WHERE cs.class_session_id = stg.class_session_id 
                        AND cs.account_id = :account_id
                        AND cs.location = :location_id
                    )
                  )
            """), {"account_id": account_id, "location_id": str(location_id)})
            missing_class_sessions_count = missing_class_sessions_result.fetchone()[0]
            
            # Count records with missing credit_transactions
            missing_credit_transactions_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_reservations_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.credit_transactions_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM credit_transactions ct 
                    WHERE ct.credit_transactions_id = stg.credit_transactions_id 
                      AND ct.account_id = :account_id
                      AND ct.location = :location_id
                  )
            """), {"account_id": account_id, "location_id": str(location_id)})
            missing_credit_transactions_count = missing_credit_transactions_result.fetchone()[0]
            
            # Count total records with ANY missing dependency
            total_invalid_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_reservations_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.credit_transactions_id IS NOT NULL
                  AND (
                    stg.customer_id IS NULL
                    OR NOT EXISTS (
                      SELECT 1 FROM customers c 
                      WHERE c.customer_id = stg.customer_id 
                        AND c.account_id = :account_id
                        AND c.location_id = :location_id
                    )
                    OR stg.class_session_id IS NULL
                    OR NOT EXISTS (
                      SELECT 1 FROM class_sessions cs 
                      WHERE cs.class_session_id = stg.class_session_id 
                        AND cs.account_id = :account_id
                        AND cs.location = :location_id
                    )
                    OR NOT EXISTS (
                      SELECT 1 FROM credit_transactions ct 
                      WHERE ct.credit_transactions_id = stg.credit_transactions_id 
                        AND ct.account_id = :account_id
                        AND ct.location = :location_id
                    )
                  )
            """), {"account_id": account_id, "location_id": str(location_id)})
            total_invalid_dependencies = total_invalid_result.fetchone()[0]
            
        # Log details about missing dependencies if any
        if total_invalid_dependencies > 0:
            logger.warning(f"[STEP 3] Found {total_invalid_dependencies} credit_transactions reservation records with missing dependencies")
            logger.warning(f"[STEP 3] Missing customers: {missing_customers_count}")
            logger.warning(f"[STEP 3] Missing class_sessions: {missing_class_sessions_count}")
            logger.warning(f"[STEP 3] Missing credit_transactions: {missing_credit_transactions_count}")
        
        logger.info(f"[STEP 3] SUCCESS: Credit_transactions reservation records with missing dependencies: {total_invalid_dependencies}")
        return total_invalid_dependencies
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count credit_transactions reservation records with missing dependencies: {e}")
        raise

async def step_4_count_records_ready_for_insertion(account_id: str, location_id: str, engine):
    """
    Step 4: Count credit_transactions reservation records that are actually ready for insertion (validation before insert)
    OPTIMIZED: Uses EXISTS/NOT EXISTS for better performance
    """
    logger.info(f"[STEP 4] Counting credit_transactions reservation records ready for insertion (validation)")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM mt_reservations_details_dlk stg
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.credit_transactions_id IS NOT NULL
                  AND EXISTS (
                    SELECT 1 FROM customers c
                    WHERE c.customer_id = stg.customer_id
                      AND c.account_id = :account_id
                      AND c.location_id = :location_id
                  )
                  AND EXISTS (
                    SELECT 1 FROM class_sessions cs
                    WHERE cs.class_session_id = stg.class_session_id
                      AND cs.account_id = :account_id
                      AND cs.location = :location_id
                  )
                  AND EXISTS (
                    SELECT 1 FROM credit_transactions ct
                    WHERE ct.credit_transactions_id = stg.credit_transactions_id
                      AND ct.account_id = :account_id
                      AND ct.location = :location_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM reservations r
                    WHERE r.reservations_id = stg.reservations_id
                      AND r.account_id = :account_id
                      AND r.location = :location_id
                  )
            """), {"account_id": account_id, "location_id": location_id})
            insert_ready_count = result.fetchone()[0]
            
        logger.info(f"[STEP 4] SUCCESS: Credit_transactions reservation records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 4] ERROR: Failed to count credit_transactions reservation records ready for insertion: {e}")
        raise


async def step_5_insert_new_records(account_id: str, location_id: int, engine):
    """
    Step 5: Insert new credit_transactions reservation records into main table
    """
    logger.info(f"[STEP 5] Inserting new credit_transactions reservation records")
    
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
                  ct.id AS credit_transactions_ref_id, c.id AS customer_ref_id, NULL AS membership_transactions_ref_id, stg.reservation_type as prev_reservation_type, stg.transactions_type
                FROM mt_reservations_details_dlk stg
                INNER JOIN customers c
                  ON stg.customer_id = c.customer_id
                  AND c.account_id = :account_id
                  AND c.location_id = :location_id
                INNER JOIN class_sessions cs
                  ON stg.class_session_id = cs.class_session_id
                  AND cs.account_id = :account_id
                  AND cs.location = :location_id
                INNER JOIN credit_transactions ct
                  ON stg.credit_transactions_id = ct.credit_transactions_id
                  AND ct.account_id = :account_id
                  AND ct.location = :location_id
                WHERE stg.account_id = :account_id
                  AND stg.location = :location_id
                  AND stg.credit_transactions_id IS NOT NULL
                  AND stg.reservations_id NOT IN (
                    SELECT reservations_id FROM reservations 
                    WHERE account_id = :account_id 
                      AND location = :location_id
                  )
            """), {"account_id": account_id, "location_id": str(location_id)})
            inserted_count = result.rowcount
            
        logger.info(f"[STEP 5] SUCCESS: Inserted {inserted_count} credit_transactions reservation records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 5] ERROR: Failed to insert credit_transactions reservation records: {e}")
        raise

async def process_reservations_credit(account_id: str, location_id: int, engine):
    """
    Main function to process credit_transactions reservations data
    5-step process: count records, validate dependencies, insert data
    Dependencies: customers, class_sessions, credit_transactions tables
    """
    logger.info(f"[process_reservations_credit] Starting processing for account_id={account_id}, location_id={location_id}")
    
    try:
        # Step 1: Count total credit reservations
        try:
            total_records_result = await step_1_count_total_credit_reservations(account_id, location_id, engine)
            total_credit_reservations = total_records_result["total_credit_reservations"]
        except Exception as e:
            logger.error(f"[process_reservations_credit] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 2: Count records already existing in main table  
        try:
            already_exist_in_main = await step_2_count_existing_in_main_table(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_reservations_credit] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 3: Count records with missing dependencies
        try:
            invalid_dependency_records = await step_3_count_records_with_missing_dependencies(account_id, location_id, engine)
        except Exception as e:
            logger.error(f"[process_reservations_credit] ERROR: Step 3 failed: {e}")
            raise
        
        # Calculate expected records for insertion
        expected_ready = total_credit_reservations - already_exist_in_main - invalid_dependency_records
        
        # Step 4: Count records ready for insertion (validation before insert)
        if expected_ready > 0:
            try:
                insert_ready_count = await step_4_count_records_ready_for_insertion(account_id, location_id, engine)
            except Exception as e:
                logger.error(f"[process_reservations_credit] ERROR: Step 4 failed: {e}")
                raise
            
            # Validate that insert_ready_count matches expected_ready
            if insert_ready_count != expected_ready:
                logger.error(f"[process_reservations_credit] ERROR: Validation failed - expected {expected_ready} but found {insert_ready_count} ready for insertion")
                raise Exception(f"Insert ready count mismatch: expected {expected_ready}, actual {insert_ready_count}")
            
            logger.info(f"[process_reservations_credit] SUCCESS: Validation passed - {insert_ready_count} records ready for insertion as expected")
            
            # Step 5: Insert new records
            try:
                inserted_records = await step_5_insert_new_records(account_id, location_id, engine)
            except Exception as e:
                logger.error(f"[process_reservations_credit] ERROR: Step 5 failed: {e}")
                raise
            
            # Final validation that insertion count matches expected
            if inserted_records == expected_ready:
                logger.info(f"[process_reservations_credit] SUCCESS: Final validation passed - inserted {inserted_records} records as expected")
            else:
                logger.error(f"[process_reservations_credit] ERROR: Final validation failed - expected {expected_ready} but inserted {inserted_records}")
                raise Exception(f"Final insertion count mismatch: expected {expected_ready}, actual {inserted_records}")
        else:
            logger.warning(f"[process_reservations_credit] WARNING: No records to insert (expected_ready: {expected_ready})")
            insert_ready_count = 0
            inserted_records = 0
        
        # Calculate missing percentage: (total - already_existing - inserted) / total * 100
        total_not_inserted = total_credit_reservations - already_exist_in_main - inserted_records
        missing_percentage = round((total_not_inserted / total_credit_reservations * 100), 2) if total_credit_reservations > 0 else 0.0
        
        # Summary
        logger.info(f"[process_reservations_credit] PROCESS COMPLETE:")
        logger.info(f"  - Total credit_transactions reservations: {total_credit_reservations}")
        logger.info(f"  - Records already exist in main table: {already_exist_in_main}")
        logger.info(f"  - Records with missing dependencies: {invalid_dependency_records}")
        logger.info(f"  - Records not inserted (missing deps + validation failures): {total_not_inserted} ({missing_percentage}%)")
        logger.info(f"  - Records ready for insertion: {insert_ready_count}")
        logger.info(f"  - Actually inserted: {inserted_records}")
        
        return {
            "transaction_type": "CreditTransaction",
            "total_credit_reservations": total_credit_reservations,
            "already_exist_in_main_table": already_exist_in_main,
            "invalid_dependency_records": invalid_dependency_records,
            "missing_dependency_percentage": missing_percentage,
            "insert_ready_count": insert_ready_count,
            "inserted_records": inserted_records
        }
        
    except Exception as e:
        logger.error(f"[process_reservations_credit] FATAL ERROR: {e}")
        raise
