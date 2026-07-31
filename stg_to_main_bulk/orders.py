"""
Orders Service - Step 6 of ETL Pipeline
Processes orders data from staging to main table with customer dependency validation
Dependencies: customers table only
"""

import logging
from sqlalchemy import text
from stg_to_main_bulk._base.dedup import drop_staging_duplicates

logger = logging.getLogger(__name__)

async def step_1_count_total_records(account_id: str, engine):
    """
    Step 1: Count total orders records in staging after Step 0 cleanup
    """
    logger.info(f"[STEP 1] Counting total orders records in staging (after cleanup)")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_orders_bulk
                WHERE account_id = :account_id
            """), {"account_id": account_id})
            total_count = result.fetchone()[0]
        
        logger.info(f"[STEP 1] SUCCESS: Total orders records after cleanup: {total_count}")
        return {"total_staging_after_cleanup": total_count}
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to count total orders records: {e}")
        raise

async def step_2_count_existing_in_main_table(account_id: str, engine):
    """
    Step 2: Count orders records already existing in main table with customer dependency validation
    Now uses same logic as steps 6-7 to ensure accurate counting
    """
    logger.info(f"[STEP 2] Counting orders records already in main table (with customer validation)")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_orders_bulk stg
                INNER JOIN customers c
                  ON stg.customer_id = c.customer_id  
                  AND stg.account_id = c.account_id
                WHERE stg.account_id = :account_id
                  AND stg.order_id IN (
                    SELECT order_id FROM orders 
                    WHERE account_id = :account_id
                  )
            """), {"account_id": account_id})
            existing_count = result.fetchone()[0]
        
        logger.info(f"[STEP 2] SUCCESS: Records already exist in main table (with valid customers): {existing_count}")
        return existing_count
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Failed to count existing orders: {e}")
        raise

async def step_4_count_orders_with_missing_customers(account_id: str, engine):
    """
    Step 3: Count orders records with missing customer dependencies (invalid for insertion)
    Also counts the number of distinct missing customers for insights
    """
    logger.info(f"[STEP 3] Counting orders records with missing customer dependencies")
    
    try:
        with engine.connect() as conn:
            # Count orders records where customer_id is NULL or doesn't exist in customers table
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_orders_bulk stg
                WHERE stg.account_id = :account_id
                  AND (
                    stg.customer_id IS NULL
                    OR NOT EXISTS (
                      SELECT 1 FROM customers c 
                      WHERE c.customer_id = stg.customer_id 
                        AND c.account_id = :account_id
                    )
                  )
            """), {"account_id": account_id})
            missing_customer_orders = result.fetchone()[0]
            
            # Count distinct missing customers
            distinct_result = conn.execute(text("""
                SELECT COUNT(DISTINCT stg.customer_id) as count
                FROM stg_orders_bulk stg
                WHERE stg.account_id = :account_id
                  AND (
                    stg.customer_id IS NULL
                    OR NOT EXISTS (
                      SELECT 1 FROM customers c 
                      WHERE c.customer_id = stg.customer_id 
                        AND c.account_id = :account_id
                    )
                  )
            """), {"account_id": account_id})
            distinct_missing_customers = distinct_result.fetchone()[0]
            
            # Also count NULL customer_ids separately for insights
            null_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_orders_bulk stg
                WHERE stg.account_id = :account_id
                  AND stg.customer_id IS NULL
            """), {"account_id": account_id})
            null_customer_orders = null_result.fetchone()[0]
        
        # Log details about missing customers if any
        if missing_customer_orders > 0:
            logger.warning(f"[STEP 3] Found {missing_customer_orders} orders records with missing customers")
            logger.warning(f"[STEP 3] Found {distinct_missing_customers} distinct missing customers")
            logger.warning(f"[STEP 3] Found {null_customer_orders} orders records with NULL customer_id")
            
            # Get sample of missing customer_ids and their order counts for debugging
            with engine.connect() as conn:
                sample_result = conn.execute(text("""
                    SELECT stg.customer_id, COUNT(*) as order_count
                    FROM stg_orders_bulk stg
                    WHERE stg.account_id = :account_id
                      AND (
                        stg.customer_id IS NULL
                        OR NOT EXISTS (
                          SELECT 1 FROM customers c 
                          WHERE c.customer_id = stg.customer_id 
                            AND c.account_id = :account_id
                        )
                      )
                    GROUP BY stg.customer_id
                    ORDER BY order_count DESC
                    LIMIT 10
                """), {"account_id": account_id})
                missing_details = [(row[0], row[1]) for row in sample_result.fetchall()]
                logger.warning(f"[STEP 3] Sample missing customers with order counts: {missing_details}")
        
        logger.info(f"[STEP 3] SUCCESS: Orders records with missing customers: {missing_customer_orders}")
        logger.info(f"[STEP 3] SUCCESS: Distinct missing customers: {distinct_missing_customers}")
        logger.info(f"[STEP 3] SUCCESS: Orders records with NULL customer_id: {null_customer_orders}")
        return missing_customer_orders
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to count orders with missing customer dependencies: {e}")
        raise

async def step_5_calculate_expected_ready(account_id: str, total_staging_after_cleanup: int, already_exist_in_main: int, invalid_customer_records: int):
    """
    Step 5: Calculate expected records ready for insertion
    Formula: total_staging_after_cleanup - already_exist_in_main - invalid_customer_records
    """
    logger.info(f"[STEP 5] Calculating expected records ready for insertion")
    
    expected_ready = total_staging_after_cleanup - already_exist_in_main - invalid_customer_records
    
    logger.info(f"[STEP 5] SUCCESS: Expected ready for insertion: {expected_ready}")
    logger.info(f"[STEP 5] Breakdown: Total_after_cleanup({total_staging_after_cleanup}) - Already_exist({already_exist_in_main}) - Invalid_Customers({invalid_customer_records}) = {expected_ready}")
    
    return expected_ready

async def step_6_count_records_to_insert(account_id: str, engine):
    """
    Step 6: Count records that are actually ready for insertion (validation before insert)
    This should match the expected_ready count - if not, there's a logic error
    """
    logger.info(f"[STEP 6] Counting records ready for insertion (validation)")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM stg_orders_bulk o
                INNER JOIN customers c 
                ON o.customer_id = c.customer_id  
                AND o.account_id = c.account_id
                WHERE o.account_id = :account_id 
                AND o.order_id NOT IN (SELECT order_id FROM public.orders WHERE account_id = :account_id)
            """), {"account_id": account_id})
            insert_ready_count = result.fetchone()[0]
            
        logger.info(f"[STEP 6] SUCCESS: Records ready for insertion: {insert_ready_count}")
        return insert_ready_count
    except Exception as e:
        logger.error(f"[STEP 6] ERROR: Failed to count records ready for insertion: {e}")
        raise

async def step_7_insert_new_records(account_id: str, engine):
    """
    Step 7: Insert new orders records into main table
    """
    logger.info(f"[STEP 7] Inserting new orders records")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO public.orders (
                  order_id, date_placed, location, payment_sources_labels, status, order_lines_id,
                  customer_id, created_at, created_by, updated_at, updated_by, deleted_at, deleted_by,
                  account_id, customer_ref_id
                )
                SELECT stg.order_id, stg.date_placed, stg.location, stg.payment_sources_labels, stg.status, stg.order_lines_id,
                       stg.customer_id, now() AS created_at, 1 AS created_by, now() AS updated_at, stg.updated_by, 
                       stg.deleted_at, stg.deleted_by, stg.account_id, c.id AS customer_ref_id
                FROM stg_orders_bulk stg
                INNER JOIN customers c ON stg.customer_id = c.customer_id  
                                    AND c.account_id = :account_id
                WHERE stg.account_id = :account_id
                  AND stg.order_id NOT IN (
                    SELECT order_id FROM orders 
                    WHERE account_id = :account_id
                  )
            """), {"account_id": account_id})
            inserted_count = result.rowcount
            
        logger.info(f"[STEP 7] SUCCESS: Inserted {inserted_count} orders records")
        return inserted_count
    except Exception as e:
        logger.error(f"[STEP 7] ERROR: Failed to insert orders records: {e}")
        raise

async def process_orders(account_id: str, engine):
    """
    Main function to process orders data with customer dependency validation
    7-step process (0-6): Drop duplicates, count records, validate dependencies, insert data
    """
    logger.info(f"[process_orders] Starting processing for account_id={account_id}")
    
    try:
        # Step 0: Drop duplicates in staging table
        try:
            step_0_result = drop_staging_duplicates(engine, "stg_orders_bulk", "order_id", account_id)
            duplicates_found = step_0_result["duplicates_found"]
            duplicates_removed = step_0_result["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_orders] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Count total records after cleanup
        try:
            total_records_result = await step_1_count_total_records(account_id, engine)
            total_staging_after_cleanup = total_records_result["total_staging_after_cleanup"]
        except Exception as e:
            logger.error(f"[process_orders] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 2: Count records already existing in main table  
        try:
            already_exist_in_main = await step_2_count_existing_in_main_table(account_id, engine)
        except Exception as e:
            logger.error(f"[process_orders] ERROR: Step 2 failed: {e}")
            raise
        
        # Step 4: Count records with missing customer dependencies
        try:
            invalid_customer_records = await step_4_count_orders_with_missing_customers(account_id, engine)
        except Exception as e:
            logger.error(f"[process_orders] ERROR: Step 4 failed: {e}")
            raise
        
        # Step 5: Calculate expected records for insertion
        try:
            expected_ready = await step_5_calculate_expected_ready(account_id, total_staging_after_cleanup, already_exist_in_main, invalid_customer_records)
        except Exception as e:
            logger.error(f"[process_orders] ERROR: Step 5 failed: {e}")
            raise
        
        # Step 6: Count records ready for insertion (validation before insert)
        if expected_ready > 0:
            try:
                insert_ready_count = await step_6_count_records_to_insert(account_id, engine)
            except Exception as e:
                logger.error(f"[process_orders] ERROR: Step 6 failed: {e}")
                raise
            
            # Validate that insert_ready_count matches expected_ready
            if insert_ready_count != expected_ready:
                logger.error(f"[process_orders] ERROR: Validation failed - expected {expected_ready} but found {insert_ready_count} ready for insertion")
                raise Exception(f"Insert ready count mismatch: expected {expected_ready}, actual {insert_ready_count}")
            
            logger.info(f"[process_orders] SUCCESS: Validation passed - {insert_ready_count} records ready for insertion as expected")
            
            # Step 7: Insert new records
            try:
                inserted_records = await step_7_insert_new_records(account_id, engine)
            except Exception as e:
                logger.error(f"[process_orders] ERROR: Step 7 failed: {e}")
                raise
            
            # Final validation that insertion count matches expected
            if inserted_records == expected_ready:
                logger.info(f"[process_orders] SUCCESS: Final validation passed - inserted {inserted_records} records as expected")
            else:
                logger.error(f"[process_orders] ERROR: Final validation failed - expected {expected_ready} but inserted {inserted_records}")
                raise Exception(f"Final insertion count mismatch: expected {expected_ready}, actual {inserted_records}")
        else:
            logger.warning(f"[process_orders] WARNING: No records to insert (expected_ready: {expected_ready})")
            insert_ready_count = 0
            inserted_records = 0
        
        # Calculate missing percentage: (total - already_existing - inserted) / total * 100
        total_not_inserted = total_staging_after_cleanup - already_exist_in_main - inserted_records
        missing_percentage = round((total_not_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        
        # Summary
        logger.info(f"[process_orders] PROCESS COMPLETE:")
        logger.info(f"  - Duplicates found in staging: {duplicates_found}")
        logger.info(f"  - Duplicates removed: {duplicates_removed}")
        logger.info(f"  - Total staging records after cleanup: {total_staging_after_cleanup}")
        logger.info(f"  - Records already exist in main table: {already_exist_in_main}")
        logger.info(f"  - Records with missing customers: {invalid_customer_records}")
        logger.info(f"  - Records not inserted (missing deps + validation failures): {total_not_inserted} ({missing_percentage}%)")
        logger.info(f"  - Records ready for insertion: {insert_ready_count}")
        logger.info(f"  - Actually inserted: {inserted_records}")
        
        return {
            "duplicates_found": duplicates_found,
            "duplicates_removed": duplicates_removed,
            "total_staging_after_cleanup": total_staging_after_cleanup,
            "already_exist_in_main_table": already_exist_in_main,
            "invalid_customer_records": invalid_customer_records,
            "missing_dependency_percentage": missing_percentage,
            "insert_ready_count": insert_ready_count,
            "inserted_records": inserted_records
        }
        
    except Exception as e:
        logger.error(f"[process_orders] FATAL ERROR: {e}")
        raise
