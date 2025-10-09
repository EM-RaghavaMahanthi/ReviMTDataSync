"""
Reservations Service - Step 8 of ETL Pipeline
Processes reservations from staging to main table with first_timer calculation
Step 0: Remove duplicates, Step 1: Calculate first_timer field
"""

import logging
from sqlalchemy import text
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def step_1_calculate_first_timer_field_with_pandas(account_id: str, location_id: int, engine):
    """
    Step 1: Calculate and update first_timer field in staging table using pandas (CORRECTED LOGIC)
    """
    logger.info(f"[STEP 1] Calculating first_timer field using pandas approach")
    
    try:
        with engine.begin() as conn:
            # Load staging reservations data
            reservations_df = pd.read_sql_query(text("""
                SELECT reservations_id, customer_id, class_session_id, status, account_id, location
                FROM mt_reservations_details_dlk 
                WHERE account_id = :account_id
                  AND location = :location_id
            """), conn, params={"account_id": account_id, "location_id": str(location_id)})
            
            logger.info(f"[STEP 1] Loaded {len(reservations_df)} reservations from staging")
            
            # Get valid customer IDs
            valid_customers_df = pd.read_sql_query(text("""
                SELECT DISTINCT customer_id
                FROM customers 
                WHERE account_id = :account_id
                  AND location_id = :location_id
            """), conn, params={"account_id": account_id, "location_id": location_id})
            
            # Get class session start times
            class_sessions_df = pd.read_sql_query(text("""
                SELECT class_session_id, start_datetime
                FROM class_sessions 
                WHERE account_id = :account_id
                  AND location = :location_id
            """), conn, params={"account_id": account_id, "location_id": str(location_id)})
            
            # Filter reservations to valid customers only
            filtered_reservations = reservations_df[
                reservations_df['customer_id'].isin(valid_customers_df['customer_id'])
            ].copy()
            
            # Add class session start times
            filtered_reservations = filtered_reservations.merge(
                class_sessions_df, 
                on='class_session_id', 
                how='inner'
            )
            
            if len(filtered_reservations) == 0:
                logger.info(f"[STEP 1] No valid reservations found")
                return {"first_timer_updated_count": 0}
            
            # Calculate reservation counts per customer (like reservation_counts CTE)
            reservation_counts = filtered_reservations.groupby('customer_id').size().reset_index(name='total_reservations')
            
            # Find first eligible reservations - ONLY for ('check in', 'pending') status
            eligible_reservations = filtered_reservations[
                filtered_reservations['status'].isin(['check in', 'pending'])
            ].copy()
            
            if len(eligible_reservations) > 0:
                # Sort by customer and start_datetime to find first reservation
                eligible_reservations = eligible_reservations.sort_values(['customer_id', 'start_datetime'])
                eligible_reservations['rn'] = eligible_reservations.groupby('customer_id').cumcount() + 1
                
                # Get first eligible reservation per customer (rn = 1)
                first_eligible = eligible_reservations[eligible_reservations['rn'] == 1][['reservations_id', 'customer_id']]
            else:
                first_eligible = pd.DataFrame(columns=['reservations_id', 'customer_id'])
            
            # Apply exact SQL logic
            result_df = filtered_reservations.merge(reservation_counts, on='customer_id', how='left')
            result_df = result_df.merge(first_eligible, on=['reservations_id', 'customer_id'], how='left', indicator='is_first_eligible')
            
            # CORRECTED: Match exact SQL logic
            first_timer_conditions = (
                # Single reservation customers (no status filter - matches SQL)
                (result_df['total_reservations'] == 1) |
                # Multi-reservation customers where this is their first eligible reservation
                ((result_df['total_reservations'] > 1) & 
                 (result_df['is_first_eligible'] == 'both'))
            )
            
            first_timer_ids = result_df[first_timer_conditions]['reservations_id'].tolist()
            
            logger.info(f"[STEP 1] Identified {len(first_timer_ids)} first-timer reservations")
            
            # Update database with first timers
            first_timer_count = 0
            if first_timer_ids:
                batch_size = 1000
                for i in range(0, len(first_timer_ids), batch_size):
                    batch_ids = first_timer_ids[i:i + batch_size]
                    placeholders = ','.join([':id' + str(j) for j in range(len(batch_ids))])
                    params = {f'id{j}': batch_ids[j] for j in range(len(batch_ids))}
                    params.update({"account_id": account_id, "location_id": str(location_id)})
                    
                    batch_result = conn.execute(text(f"""
                        UPDATE mt_reservations_details_dlk 
                        SET first_timer = TRUE 
                        WHERE reservations_id IN ({placeholders})
                          AND account_id = :account_id 
                          AND location = :location_id
                    """), params)
                    
                    first_timer_count += batch_result.rowcount
            
        logger.info(f"[STEP 1] SUCCESS: Updated first_timer field for {first_timer_count} reservations")
        return {"first_timer_updated_count": first_timer_count}
        
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to calculate first_timer field: {e}")
        raise


async def step_0_drop_duplicates_in_staging(account_id: str, location_id: int, engine):
    """
    Step 0: Drop duplicate reservation records in staging table (mt_reservations_details_dlk) to clean data
    """
    logger.info(f"[STEP 0] Dropping duplicate reservation records in mt_reservations_details_dlk staging table")
    
    try:
        with engine.begin() as conn:
            # First count duplicates before removal
            count_result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT reservations_id) as unique_reservations
                FROM mt_reservations_details_dlk
                WHERE account_id = :account_id
                  AND location = :location_id
            """), {"account_id": account_id, "location_id": str(location_id)})
            stats = count_result.fetchone()
            total_before = stats[0]
            unique_reservations = stats[1]
            duplicates_found = total_before - unique_reservations
            
            if duplicates_found > 0:
                logger.info(f"[STEP 0] Found {duplicates_found} duplicate reservation records to remove ({total_before} total, {unique_reservations} unique)")
                
                # Delete duplicates, keeping only one record per reservations_id
                result = conn.execute(text("""
                    DELETE FROM mt_reservations_details_dlk 
                    WHERE ctid NOT IN (
                        SELECT DISTINCT ON (reservations_id) ctid
                        FROM mt_reservations_details_dlk
                        WHERE account_id = :account_id
                          AND location = :location_id
                        ORDER BY reservations_id, ctid
                    )
                    AND account_id = :account_id
                    AND location = :location_id
                """), {"account_id": account_id, "location_id": str(location_id)})
                deleted_count = result.rowcount
                
                logger.info(f"[STEP 0] SUCCESS: Removed {deleted_count} duplicate reservation records from staging table")
            else:
                logger.info(f"[STEP 0] SUCCESS: No duplicates found in reservation staging records")
                deleted_count = 0
                
        return {
            "duplicates_found": duplicates_found,
            "duplicates_removed": deleted_count
        }
    except Exception as e:
        logger.error(f"[STEP 0] ERROR: Failed to drop duplicates in staging table: {e}")
        raise

async def step_1_calculate_first_timer_field(account_id: str, location_id: int, engine):
    """
    Step 1: Calculate and update first_timer field in staging table based on customer reservation history
    """
    logger.info(f"[STEP 1] Calculating first_timer field for reservations in staging table")
    
    try:
        with engine.begin() as conn:
            # First, reset all first_timer fields to FALSE
            reset_result = conn.execute(text("""
                UPDATE mt_reservations_details_dlk 
                SET first_timer = FALSE
                WHERE account_id = :account_id
                  AND location = :location_id
            """), {"account_id": account_id, "location_id": str(location_id)})
            
            logger.info(f"[STEP 1] Reset first_timer field for all records")
            
            # Apply first_timer calculation logic
            update_result = conn.execute(text("""
                WITH filtered_reservations_dlk AS (
                  SELECT mt.*
                  FROM mt_reservations_details_dlk mt
                  INNER JOIN customers c ON mt.customer_id = c.customer_id
                    AND c.account_id = :account_id
                    AND c.location_id = :location_id
                  INNER JOIN class_sessions cs ON mt.class_session_id = cs.class_session_id
                    AND cs.account_id = :account_id
                    AND cs.location = :location_id
                  WHERE mt.account_id = :account_id
                    AND mt.location = :location_id
                ),
                reservation_counts AS (
                  SELECT 
                      customer_id,
                      COUNT(*) AS total_reservations
                  FROM filtered_reservations_dlk
                  GROUP BY customer_id
                ),
                first_timer_candidates AS (
                  SELECT 
                      r.reservations_id AS reservation_id,
                      r.customer_id,
                      ROW_NUMBER() OVER (
                          PARTITION BY r.customer_id 
                          ORDER BY cs.start_datetime ASC
                      ) AS rn
                  FROM filtered_reservations_dlk r
                  INNER JOIN class_sessions cs 
                      ON r.class_session_id = cs.class_session_id
                     AND cs.account_id = :account_id
                     AND cs.location = :location_id
                  WHERE r.status IN ('check in', 'pending')
                ),
                expected_first_timer_table AS (
                  SELECT 
                      r.reservations_id,
                      r.customer_id,
                      rc.total_reservations,
                      r.status,
                      CASE 
                          WHEN rc.total_reservations = 1 THEN TRUE
                          WHEN rc.total_reservations > 1 AND ft.rn = 1 THEN TRUE
                          ELSE FALSE
                      END AS expected_first_timer
                  FROM filtered_reservations_dlk r
                  LEFT JOIN reservation_counts rc ON r.customer_id = rc.customer_id
                  LEFT JOIN first_timer_candidates ft ON r.reservations_id = ft.reservation_id
                ) 
                UPDATE mt_reservations_details_dlk mt
                SET first_timer = TRUE
                FROM expected_first_timer_table eft
                WHERE mt.reservations_id = eft.reservations_id
                  AND eft.expected_first_timer = TRUE
                  AND mt.account_id = :account_id
                  AND mt.location = :location_id
            """), {"account_id": account_id, "location_id": str(location_id)})
            
            first_timer_count = update_result.rowcount
            
        logger.info(f"[STEP 1] SUCCESS: Updated first_timer field for {first_timer_count} reservations")
        return {"first_timer_updated_count": first_timer_count}
        
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to calculate first_timer field: {e}")
        raise

async def step_3_sync_first_timer_with_main_table(account_id: str, location_id: int, engine):
    """
    Step 3: Synchronize first_timer field between staging (mt_reservations_details_dlk) and main (reservations) tables
    """
    logger.info(f"[STEP 3] Starting first_timer synchronization between staging and main tables")
    
    try:
        with engine.begin() as conn:
            # Query 1: Find records where main table has TRUE but staging has FALSE
            query_1_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM reservations r
                JOIN mt_reservations_details_dlk dlk
                  ON r.reservations_id = dlk.reservations_id
                WHERE r.first_timer = TRUE
                  AND dlk.first_timer = FALSE
                  AND r.account_id = :account_id
                  AND r.location = :location_id
            """), {"account_id": account_id, "location_id": str(location_id)})
            
            main_true_staging_false_count = query_1_result.fetchone()[0]
            logger.info(f"[STEP 3] Query 1: Found {main_true_staging_false_count} records where main=TRUE, staging=FALSE")
            
            # Query 2: Update main table to FALSE where staging is FALSE
            if main_true_staging_false_count > 0:
                update_1_result = conn.execute(text("""
                    UPDATE reservations r
                    SET first_timer = FALSE
                    FROM mt_reservations_details_dlk dlk
                    WHERE r.reservations_id = dlk.reservations_id
                      AND r.first_timer = TRUE
                      AND dlk.first_timer = FALSE
                      AND r.account_id = :account_id
                      AND r.location = :location_id
                """), {"account_id": account_id, "location_id": str(location_id)})
                
                actual_updated_1 = update_1_result.rowcount
                logger.info(f"[STEP 3] Query 2: Expected {main_true_staging_false_count}, Actually updated {actual_updated_1} records (main TRUE→FALSE)")
                
                # Validation: Should match
                if actual_updated_1 == main_true_staging_false_count:
                    logger.info(f"[STEP 3] ✅ Query 1-2 MATCH: Expected and actual updates are equal")
                else:
                    logger.warning(f"[STEP 3] ❌ Query 1-2 MISMATCH: Expected {main_true_staging_false_count}, got {actual_updated_1}")
            else:
                actual_updated_1 = 0
                logger.info(f"[STEP 3] Query 2: No updates needed (main TRUE→FALSE)")
            
            # Query 3: Find records where main table has FALSE but staging has TRUE
            query_3_result = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM reservations r
                JOIN mt_reservations_details_dlk dlk
                  ON r.reservations_id = dlk.reservations_id
                WHERE r.first_timer = FALSE
                  AND dlk.first_timer = TRUE
                  AND r.account_id = :account_id
                  AND r.location = :location_id
            """), {"account_id": account_id, "location_id": str(location_id)})
            
            main_false_staging_true_count = query_3_result.fetchone()[0]
            logger.info(f"[STEP 3] Query 3: Found {main_false_staging_true_count} records where main=FALSE, staging=TRUE")
            
            # Query 4: Update main table to TRUE where staging is TRUE
            if main_false_staging_true_count > 0:
                update_2_result = conn.execute(text("""
                    UPDATE reservations r
                    SET first_timer = TRUE
                    FROM mt_reservations_details_dlk dlk
                    WHERE r.reservations_id = dlk.reservations_id
                      AND r.first_timer = FALSE
                      AND dlk.first_timer = TRUE
                      AND r.account_id = :account_id
                      AND r.location = :location_id
                """), {"account_id": account_id, "location_id": str(location_id)})
                
                actual_updated_2 = update_2_result.rowcount
                logger.info(f"[STEP 3] Query 4: Expected {main_false_staging_true_count}, Actually updated {actual_updated_2} records (main FALSE→TRUE)")
                
                # Validation: Should match
                if actual_updated_2 == main_false_staging_true_count:
                    logger.info(f"[STEP 3] ✅ Query 3-4 MATCH: Expected and actual updates are equal")
                else:
                    logger.warning(f"[STEP 3] ❌ Query 3-4 MISMATCH: Expected {main_false_staging_true_count}, got {actual_updated_2}")
            else:
                actual_updated_2 = 0
                logger.info(f"[STEP 3] Query 4: No updates needed (main FALSE→TRUE)")
            
            # Query 5: Find customers with multiple first_timer=TRUE reservations (should be corrected)
            query_5_result = conn.execute(text("""
                SELECT COUNT(DISTINCT r.reservations_id) as count
                FROM reservations r
                JOIN class_sessions cs
                  ON r.class_session_ref_id = cs.id
                WHERE r.account_id = :account_id
                  AND r.location = :location_id
                  AND r.first_timer IS TRUE
                  AND r.customer_id IN (
                    SELECT customer_id
                    FROM reservations
                    WHERE account_id = :account_id
                      AND location = :location_id
                      AND first_timer IS TRUE
                    GROUP BY customer_id
                    HAVING COUNT(*) > 1
                  )
            """), {"account_id": account_id, "location_id": str(location_id)})
            
            multiple_first_timers_count = query_5_result.fetchone()[0]
            logger.info(f"[STEP 3] Query 5: Found {multiple_first_timers_count} reservations from customers with multiple first_timer=TRUE")
            
            # Query 6: Fix customers with multiple first_timer=TRUE (keep only the earliest)
            if multiple_first_timers_count > 0:
                update_3_result = conn.execute(text("""
                    UPDATE reservations r
                    SET first_timer = FALSE
                    WHERE r.reservations_id IN (
                      SELECT reservations_id
                      FROM (
                        SELECT 
                          r2.reservations_id,
                          COUNT(*) OVER (PARTITION BY r2.customer_id) as customer_count,
                          ROW_NUMBER() OVER (PARTITION BY r2.customer_id ORDER BY cs.start_datetime ASC) AS rn
                        FROM reservations r2
                        JOIN class_sessions cs ON r2.class_session_ref_id = cs.id
                        WHERE r2.account_id = :account_id
                          AND r2.location = :location_id
                          AND r2.first_timer IS TRUE
                      ) ranked
                      WHERE ranked.customer_count > 1 AND ranked.rn > 1
                    )
                """), {"account_id": account_id, "location_id": str(location_id)})
                
                actual_corrected = update_3_result.rowcount
                expected_corrected = multiple_first_timers_count // 2
                logger.info(f"[STEP 3] Query 6: Expected {expected_corrected}, Actually corrected {actual_corrected} duplicate first_timer records")
                
                # Validation: Should be exactly half (since we keep first, remove rest)
                if actual_corrected == expected_corrected:
                    logger.info(f"[STEP 3] ✅ Query 5-6 MATCH: Expected and actual corrections are equal")
                else:
                    logger.error(f"[STEP 3] ❌ Query 5-6 MISMATCH: Expected {expected_corrected}, got {actual_corrected}")
            else:
                actual_corrected = 0
                logger.info(f"[STEP 3] Query 6: No duplicate first_timer corrections needed")
            
        total_changes = actual_updated_1 + actual_updated_2 + actual_corrected
        logger.info(f"[STEP 3] SUCCESS: Synchronization completed with {total_changes} total changes")
        logger.info(f"[STEP 3] Summary: {actual_updated_1} TRUE→FALSE, {actual_updated_2} FALSE→TRUE, {actual_corrected} duplicates corrected")
        
        return {
            "main_true_staging_false_found": main_true_staging_false_count,
            "main_true_staging_false_updated": actual_updated_1,
            "main_false_staging_true_found": main_false_staging_true_count,
            "main_false_staging_true_updated": actual_updated_2,
            "multiple_first_timers_found": multiple_first_timers_count,
            "duplicate_first_timers_corrected": actual_corrected,
            "total_changes": total_changes
        }
        
    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to synchronize first_timer fields: {e}")
        raise

async def step_2_orchestrate_reservations_processing(account_id: str, location_id: int, engine):
    """
    Step 2: Orchestrate reservations processing by calling services 8a and 8b
    """
    logger.info(f"[STEP 2] Starting orchestration of reservations processing")
    
    try:
        # Import reservation processing services
        try:
            from .reservations_credit_service_08a import process_reservations_credit
            from .reservations_membership_service_08b import process_reservations_membership
            from .reservations_no_transactions_service_08c import process_reservations_no_transactions
        except ImportError:
            # Fallback for different import paths
            try:
                from reservations_credit_service_08a import process_reservations_credit
                from reservations_membership_service_08b import process_reservations_membership
                from reservations_no_transactions_service_08c import process_reservations_no_transactions
            except ImportError as e:
                logger.error(f"[STEP 2] ERROR: Failed to import reservation services: {e}")
                raise
        
        # Process credit transactions reservations (Service 8a)
        logger.info(f"[STEP 2] Processing credit transactions reservations (Service 8a)")
        try:
            credit_result = await process_reservations_credit(account_id, location_id, engine)
            logger.info(f"[STEP 2] Service 8a completed: {credit_result}")
        except Exception as e:
            logger.error(f"[STEP 2] ERROR: Service 8a failed: {e}")
            raise
        
        # Process membership transactions reservations (Service 8b)
        logger.info(f"[STEP 2] Processing membership transactions reservations (Service 8b)")
        try:
            membership_result = await process_reservations_membership(account_id, location_id, engine)
            logger.info(f"[STEP 2] Service 8b completed: {membership_result}")
        except Exception as e:
            logger.error(f"[STEP 2] ERROR: Service 8b failed: {e}")
            raise

        # Process reservations with no transactions (Service 8c)
        logger.info(f"[STEP 2] Processing reservations with no transactions (Service 8c)")
        try:
            no_transactions_result = await process_reservations_no_transactions(account_id, location_id, engine)
            logger.info(f"[STEP 2] Service 8c completed: {no_transactions_result}")
        except Exception as e:
            logger.error(f"[STEP 2] ERROR: Service 8c failed: {e}")
            raise
        
        # Combine results
        total_credit_inserted = credit_result.get("inserted_records", 0)
        total_membership_inserted = membership_result.get("inserted_records", 0)
        total_no_transactions_inserted = no_transactions_result.get("inserted_records", 0)
        total_inserted = total_credit_inserted + total_membership_inserted + total_no_transactions_inserted
        
        logger.info(f"[STEP 2] SUCCESS: Orchestration completed")
        logger.info(f"[STEP 2] Credit reservations inserted: {total_credit_inserted}")
        logger.info(f"[STEP 2] Membership reservations inserted: {total_membership_inserted}")
        logger.info(f"[STEP 2] No-transactions reservations inserted: {total_no_transactions_inserted}")
        logger.info(f"[STEP 2] Total reservations inserted: {total_inserted}")
        
        return {
            "credit_result": credit_result,
            "membership_result": membership_result,
            "no_transactions_result": no_transactions_result,
            "total_inserted": total_inserted
        }
        
    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Orchestration failed: {e}")
        raise

async def process_reservations(account_id: str, location_id: int, engine):
    """
    Main function to process reservations - orchestrator pattern
    Steps: 0 (drop duplicates), 1 (calculate first_timer), 2 (orchestrate 8a,8b,8c), 3 (sync first_timer with main table)
    """
    logger.info(f"[process_reservations] Starting reservations processing for account_id={account_id}, location_id={location_id}")
    
    try:
        # Step 0: Drop duplicates in staging table
        try:
            step_0_result = await step_0_drop_duplicates_in_staging(account_id, location_id, engine)
            duplicates_found = step_0_result["duplicates_found"]
            duplicates_removed = step_0_result["duplicates_removed"]
        except Exception as e:
            logger.error(f"[process_reservations] ERROR: Step 0 failed: {e}")
            raise
        
        # Step 1: Calculate first_timer field
        try:
            step_1_result = await step_1_calculate_first_timer_field_with_pandas(account_id, location_id, engine)
            first_timer_updated = step_1_result["first_timer_updated_count"]
        except Exception as e:
            logger.error(f"[process_reservations] ERROR: Step 1 failed: {e}")
            raise
        
        # Step 1.5: Count total staging records after Step 0 cleanup
        try:
            with engine.begin() as conn:
                total_staging_result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM mt_reservations_details_dlk
                    WHERE account_id = :account_id
                      AND location = :location_id
                """), {"account_id": account_id, "location_id": str(location_id)})
                total_staging_after_cleanup = total_staging_result.fetchone()[0]
            logger.info(f"[process_reservations] Total staging records after cleanup: {total_staging_after_cleanup}")
        except Exception as e:
            logger.error(f"[process_reservations] ERROR: Failed to count total staging records: {e}")
            raise
        
        # Step 2: Orchestrate processing of services 8a and 8b
        try:
            orchestration_result = await step_2_orchestrate_reservations_processing(account_id, location_id, engine)
            total_inserted = orchestration_result["total_inserted"]
            credit_result = orchestration_result["credit_result"]
            membership_result = orchestration_result["membership_result"]
            no_transactions_result = orchestration_result["no_transactions_result"]
            total_no_transactions_inserted = no_transactions_result.get("inserted_records", 0)
        except Exception as e:
            logger.error(f"[process_reservations] ERROR: Step 2 orchestration failed: {e}")
            raise
        
        # Step 3: Synchronize first_timer field between staging and main tables
        try:
            step_3_result = await step_3_sync_first_timer_with_main_table(account_id, location_id, engine)
            sync_total_changes = step_3_result["total_changes"]
        except Exception as e:
            logger.error(f"[process_reservations] ERROR: Step 3 synchronization failed: {e}")
            raise
        
        # Calculate comprehensive percentages
        total_credit_reservations = credit_result.get("total_credit_reservations", 0)
        total_membership_reservations = membership_result.get("total_membership_reservations", 0)
        
        # Credit transaction percentages
        credit_missing_deps = credit_result.get("invalid_dependency_records", 0)
        credit_missing_percentage = credit_result.get("missing_dependency_percentage", 0.0)
        credit_inserted = credit_result.get("inserted_records", 0)
        credit_inserted_percentage = round((credit_inserted / total_credit_reservations * 100), 2) if total_credit_reservations > 0 else 0.0
        
        # Membership transaction percentages  
        membership_missing_deps = membership_result.get("invalid_dependency_records", 0)
        membership_missing_percentage = membership_result.get("missing_dependency_percentage", 0.0)
        membership_inserted = membership_result.get("inserted_records", 0)
        membership_inserted_percentage = round((membership_inserted / total_membership_reservations * 100), 2) if total_membership_reservations > 0 else 0.0
        
        # Overall percentages based on total staging records (after Step 0 cleanup)
        total_missing_deps = credit_missing_deps + membership_missing_deps
        total_not_inserted = total_staging_after_cleanup - total_inserted  # Records that were not successfully inserted
        overall_missing_percentage = round((total_not_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        overall_inserted_percentage = round((total_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        
        # Summary with percentages
        logger.info(f"[process_reservations] PROCESS COMPLETE:")
        logger.info(f"  - Duplicates found in staging: {duplicates_found}")
        logger.info(f"  - Duplicates removed: {duplicates_removed}")
        logger.info(f"  - First_timer records updated: {first_timer_updated}")
        logger.info(f"  - Total staging records after cleanup: {total_staging_after_cleanup}")
        logger.info(f"  - Credit reservations: {total_credit_reservations} | Missing deps: {credit_missing_deps} ({credit_missing_percentage}%) | Inserted: {credit_inserted} ({credit_inserted_percentage}%)")
        logger.info(f"  - Membership reservations: {total_membership_reservations} | Missing deps: {membership_missing_deps} ({membership_missing_percentage}%) | Inserted: {membership_inserted} ({membership_inserted_percentage}%)")
        logger.info(f"  - SYNC: first_timer synchronization changes: {sync_total_changes}")
        logger.info(f"  - OVERALL: Total not inserted: {total_not_inserted} ({overall_missing_percentage}%) | Total inserted: {total_inserted} ({overall_inserted_percentage}%)")
        
        return {
            "duplicates_found": duplicates_found,
            "duplicates_removed": duplicates_removed,
            "first_timer_updated_count": first_timer_updated,
            "total_staging_after_cleanup": total_staging_after_cleanup,
            "total_credit_reservations": total_credit_reservations,
            "total_membership_reservations": total_membership_reservations,
            "total_missing_dependencies": total_missing_deps,
            "total_not_inserted": total_not_inserted,
            "overall_missing_percentage": overall_missing_percentage,
            "total_inserted": total_inserted,
            "overall_inserted_percentage": overall_inserted_percentage,
            "credit_result": credit_result,
            "credit_inserted_percentage": credit_inserted_percentage,
            "membership_result": membership_result,
            "membership_inserted_percentage": membership_inserted_percentage,
            "no_transactions_result": no_transactions_result,
            "no_transactions_inserted": total_no_transactions_inserted,
            "step_3_sync_result": step_3_result,
            "sync_total_changes": sync_total_changes
        }
        
    except Exception as e:
        logger.error(f"[process_reservations] FATAL ERROR: {e}")
        raise