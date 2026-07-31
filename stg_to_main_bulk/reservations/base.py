"""
Reservations Service - Step 8 of ETL Pipeline
Processes reservations from staging to main table with first_timer calculation

Step 0: Remove duplicates from staging
Step 1: Calculate first_timer on staging via pandas (99% correct at insert time)
Step 2: Insert staging → main (8a credit, 8b membership, 8c no-transactions)
Step 3: Recalculate first_timer on main table via single SQL (fixes remaining 1% from webhook records)
"""

import logging
import pandas as pd
from sqlalchemy import text
from stg_to_main_bulk._base.dedup import drop_staging_duplicates
from .credit import process_reservations_credit
from .membership import process_reservations_membership
from .no_transactions import process_reservations_no_transactions

logger = logging.getLogger(__name__)


async def step_1_calculate_first_timer_field_with_pandas(account_id: str, engine):
    """
    Step 1: Calculate and update first_timer on staging table using pandas.
    Gives 99% correct values so inserts in Step 2 carry the right flag.

    Logic:
      - Single reservation per customer  → first_timer = TRUE regardless of status
      - Multiple reservations            → earliest 'pending' or 'check in' by class start_datetime
                                           fallback to earliest overall if none match
    """
    logger.info(f"[STEP 1] Calculating first_timer on staging via pandas")

    try:
        with engine.begin() as conn:
            reservations_df = pd.read_sql_query(text("""
                SELECT reservations_id, customer_id, class_session_id, status, account_id, location
                FROM stg_reservations_bulk
                WHERE account_id = :account_id
            """), conn, params={"account_id": account_id})

            logger.info(f"[STEP 1] Loaded {len(reservations_df)} staging reservations")

            valid_customers_df = pd.read_sql_query(text("""
                SELECT DISTINCT customer_id
                FROM customers
                WHERE account_id  = :account_id
            """), conn, params={"account_id": account_id})

            class_sessions_df = pd.read_sql_query(text("""
                SELECT class_session_id, start_datetime
                FROM class_sessions
                WHERE account_id = :account_id
            """), conn, params={"account_id": account_id})

            # Filter to valid customers and join class session times
            filtered = reservations_df[
                reservations_df["customer_id"].isin(valid_customers_df["customer_id"])
            ].copy()

            filtered = filtered.merge(class_sessions_df, on="class_session_id", how="inner")

            if len(filtered) == 0:
                logger.info(f"[STEP 1] No valid reservations found")
                return {"first_timer_updated_count": 0}

            # Mirror exact SQL ORDER BY (2 cases, not 3):
            #   1. pending/check in → priority 0, everything else → priority 1
            #   2. start_datetime ASC
            #   3. reservations_id ASC (tiebreaker)
            # DISTINCT ON equivalent: first row per customer = correct first_timer
            filtered["status_priority"] = (~filtered["status"].isin(["pending", "check in"])).astype(int)

            first_timer_ids = (
                filtered
                .sort_values(["customer_id", "status_priority", "start_datetime", "reservations_id"])
                .groupby("customer_id", sort=False)["reservations_id"]
                .first()
                .tolist()
            )
            logger.info(f"[STEP 1] Identified {len(first_timer_ids)} first-timer reservations in staging")

            # Batch update staging
            first_timer_count = 0
            if first_timer_ids:
                batch_size = 1000
                for i in range(0, len(first_timer_ids), batch_size):
                    batch_ids = first_timer_ids[i:i + batch_size]
                    placeholders = ",".join([f":id{j}" for j in range(len(batch_ids))])
                    params = {f"id{j}": batch_ids[j] for j in range(len(batch_ids))}
                    params.update({"account_id": account_id})

                    batch_result = conn.execute(text(f"""
                        UPDATE stg_reservations_bulk
                        SET first_timer = TRUE
                        WHERE reservations_id IN ({placeholders})
                          AND account_id = :account_id
                    """), params)

                    first_timer_count += batch_result.rowcount

        logger.info(f"[STEP 1] SUCCESS: Updated first_timer for {first_timer_count} staging rows")
        return {"first_timer_updated_count": first_timer_count}

    except Exception as e:
        logger.error(f"[STEP 1] ERROR: {e}")
        raise


async def step_2_orchestrate_reservations_processing(account_id: str, engine):
    """
    Step 2: Insert staging → main via services 8a (credit), 8b (membership), 8c (no-transactions).
    Rows carry first_timer values already set by Step 1 (~99% correct).
    Each sub-service is wrapped individually so failures are attributed correctly.
    """
    logger.info(f"[STEP 2] Starting insert orchestration")

    try:
        # Service 8a: credit transactions reservations
        logger.info(f"[STEP 2] Processing credit transactions reservations (Service 8a)")
        try:
            credit_result = await process_reservations_credit(account_id, engine)
            logger.info(f"[STEP 2] Service 8a completed: {credit_result}")
        except Exception as e:
            logger.error(f"[STEP 2] ERROR: Service 8a failed: {e}")
            raise

        # Service 8b: membership transactions reservations
        logger.info(f"[STEP 2] Processing membership transactions reservations (Service 8b)")
        try:
            membership_result = await process_reservations_membership(account_id, engine)
            logger.info(f"[STEP 2] Service 8b completed: {membership_result}")
        except Exception as e:
            logger.error(f"[STEP 2] ERROR: Service 8b failed: {e}")
            raise

        # Service 8c: no-transaction reservations
        logger.info(f"[STEP 2] Processing reservations with no transactions (Service 8c)")
        try:
            no_transactions_result = await process_reservations_no_transactions(account_id, engine)
            logger.info(f"[STEP 2] Service 8c completed: {no_transactions_result}")
        except Exception as e:
            logger.error(f"[STEP 2] ERROR: Service 8c failed: {e}")
            raise

        total_inserted = (
            credit_result.get("inserted_records", 0)
            + membership_result.get("inserted_records", 0)
            + no_transactions_result.get("inserted_records", 0)
        )

        logger.info(f"[STEP 2] SUCCESS: Total inserted={total_inserted}")
        return {
            "credit_result": credit_result,
            "membership_result": membership_result,
            "no_transactions_result": no_transactions_result,
            "total_inserted": total_inserted,
        }

    except Exception as e:
        logger.error(f"[STEP 2] ERROR: Orchestration failed: {e}")
        raise


async def step_3_recalculate_first_timer(account_id: str, engine):
    """
    Step 3: Recalculate first_timer on main reservations table.
    Considers ALL reservations (webhook-inserted + onboarding) to fix the remaining ~1%
    that pandas in Step 1 couldn't see (pre-onboarding webhook records not in staging).

    Pattern: read count → update → validate count matches.

    Logic:
      - Single reservation per customer  → first_timer = TRUE regardless of status
      - Multiple reservations            → earliest 'pending' or 'check in' by class start_datetime
                                           fallback to earliest overall if none match
    """
    logger.info(f"[STEP 3] Recalculating first_timer on main table for account_id={account_id}")

    PARAMS = {"account_id": account_id}

    CORRECT_CTE = """
        WITH correct AS (
            SELECT DISTINCT ON (r.account_id, r.customer_id)
                r.reservations_id AS correct_reservations_id,
                r.account_id,
                r.customer_id
            FROM reservations r
            JOIN class_sessions cs ON cs.id = r.class_session_ref_id
            WHERE r.account_id        = :account_id
              AND r.deleted_by       IS NULL
              AND r.class_session_id IS NOT NULL
            ORDER BY
                r.account_id,
                r.customer_id,
                CASE WHEN r.status IN ('pending', 'check in') THEN 0 ELSE 1 END ASC,
                cs.start_datetime ASC,
                r.reservations_id ASC
        )
    """

    try:
        with engine.begin() as conn:

            # ── Read: count records that need correction ──────────────────────
            count_result = conn.execute(text(f"""
                {CORRECT_CTE}
                SELECT COUNT(*) AS count
                FROM reservations r
                JOIN correct c ON c.account_id = r.account_id AND c.customer_id = r.customer_id
                WHERE r.account_id        = :account_id
                  AND r.deleted_by       IS NULL
                  AND r.class_session_id IS NOT NULL
                  AND r.first_timer IS DISTINCT FROM (r.reservations_id = c.correct_reservations_id)
            """), PARAMS)

            expected_count = count_result.fetchone()[0]
            logger.info(f"[STEP 3] Read: {expected_count} reservations need first_timer correction")

            if expected_count == 0:
                logger.info(f"[STEP 3] No corrections needed — skipping update")
                return {"first_timer_expected": 0, "first_timer_updated_count": 0}

            # ── Update ────────────────────────────────────────────────────────
            update_result = conn.execute(text(f"""
                {CORRECT_CTE}
                UPDATE reservations r
                SET
                    first_timer = (r.reservations_id = c.correct_reservations_id),
                    updated_at  = NOW(),
                    updated_by  = 1
                FROM correct c
                WHERE c.account_id   = r.account_id
                  AND c.customer_id  = r.customer_id
                  AND r.account_id   = :account_id
                  AND r.deleted_by  IS NULL
                  AND r.class_session_id IS NOT NULL
                  AND r.first_timer IS DISTINCT FROM (r.reservations_id = c.correct_reservations_id)
            """), PARAMS)

            actual_count = update_result.rowcount

            # ── Validate ──────────────────────────────────────────────────────
            if actual_count == expected_count:
                logger.info(f"[STEP 3] ✅ MATCH: expected={expected_count}, updated={actual_count}")
            else:
                logger.warning(f"[STEP 3] ❌ MISMATCH: expected={expected_count}, updated={actual_count}")

        logger.info(f"[STEP 3] SUCCESS: first_timer corrected for {actual_count} reservations")
        return {
            "first_timer_expected":      expected_count,
            "first_timer_updated_count": actual_count,
        }

    except Exception as e:
        logger.error(f"[STEP 3] ERROR: Failed to recalculate first_timer: {e}")
        raise


async def process_reservations(account_id: str, engine):
    """
    Main orchestrator for reservations processing.

    Flow:
      Step 0 — dedup staging
      Step 1 — pandas first_timer calc on staging (~99% correct at insert time)
      Step 2 — insert staging → main (8a, 8b, 8c)
      Step 3 — single SQL recalculation on main (fixes remaining ~1% from webhook records)
    """
    logger.info(f"[process_reservations] Starting for account_id={account_id}")

    try:
        # Step 0: dedup staging
        step_0_result      = drop_staging_duplicates(engine, "stg_reservations_bulk", "reservations_id", account_id)
        duplicates_found   = step_0_result["duplicates_found"]
        duplicates_removed = step_0_result["duplicates_removed"]
        logger.info(f"[process_reservations] Step 0: {duplicates_found} found, {duplicates_removed} removed")

        # Step 1: pandas first_timer on staging
        step_1_result        = await step_1_calculate_first_timer_field_with_pandas(account_id, engine)
        staging_first_timers = step_1_result["first_timer_updated_count"]

        # Step 1.5: count staging records after dedup
        with engine.connect() as conn:
            total_staging = conn.execute(text("""
                SELECT COUNT(*) FROM stg_reservations_bulk
                WHERE account_id = :account_id
            """), {"account_id": account_id}).scalar()
        logger.info(f"[process_reservations] Staging records after dedup: {total_staging}")

        # Step 2: insert staging → main
        step_2_result          = await step_2_orchestrate_reservations_processing(account_id, engine)
        total_inserted         = step_2_result["total_inserted"]
        credit_result          = step_2_result["credit_result"]
        membership_result      = step_2_result["membership_result"]
        no_transactions_result = step_2_result["no_transactions_result"]

        # Step 3: recalculate first_timer on main table
        step_3_result       = await step_3_recalculate_first_timer(account_id, engine)
        main_first_timer_corrected = step_3_result["first_timer_updated_count"]

        # Percentages
        total_credit_reservations     = credit_result.get("total_credit_reservations", 0)
        total_membership_reservations = membership_result.get("total_membership_reservations", 0)
        credit_missing_deps           = credit_result.get("invalid_dependency_records", 0)
        membership_missing_deps       = membership_result.get("invalid_dependency_records", 0)
        credit_missing_percentage     = credit_result.get("missing_dependency_percentage", 0.0)
        membership_missing_percentage = membership_result.get("missing_dependency_percentage", 0.0)
        credit_inserted               = credit_result.get("inserted_records", 0)
        membership_inserted           = membership_result.get("inserted_records", 0)
        total_no_transactions_inserted = no_transactions_result.get("inserted_records", 0)

        credit_inserted_percentage     = round(credit_inserted / total_credit_reservations * 100, 2) if total_credit_reservations > 0 else 0.0
        membership_inserted_percentage = round(membership_inserted / total_membership_reservations * 100, 2) if total_membership_reservations > 0 else 0.0
        overall_inserted_percentage    = round(total_inserted / total_staging * 100, 2) if total_staging > 0 else 0.0
        overall_missing_percentage     = round((total_staging - total_inserted) / total_staging * 100, 2) if total_staging > 0 else 0.0

        logger.info(f"[process_reservations] PROCESS COMPLETE:")
        logger.info(f"  - Duplicates found/removed:  {duplicates_found}/{duplicates_removed}")
        logger.info(f"  - Staging first_timer set:   {staging_first_timers}")
        logger.info(f"  - Total staging records:     {total_staging}")
        logger.info(f"  - Credit:      {total_credit_reservations} | missing: {credit_missing_deps} ({credit_missing_percentage}%) | inserted: {credit_inserted} ({credit_inserted_percentage}%)")
        logger.info(f"  - Membership:  {total_membership_reservations} | missing: {membership_missing_deps} ({membership_missing_percentage}%) | inserted: {membership_inserted} ({membership_inserted_percentage}%)")
        logger.info(f"  - No-txn:      {total_no_transactions_inserted} inserted")
        logger.info(f"  - OVERALL:     {total_inserted}/{total_staging} inserted ({overall_inserted_percentage}%), missing {overall_missing_percentage}%")
        logger.info(f"  - first_timer corrected on main: {main_first_timer_corrected}")

        return {
            "duplicates_found":               duplicates_found,
            "duplicates_removed":             duplicates_removed,
            "staging_first_timers_set":       staging_first_timers,
            "total_staging_after_cleanup":    total_staging,
            "inserted_records":               total_inserted,
            "overall_inserted_percentage":    overall_inserted_percentage,
            "overall_missing_percentage":     overall_missing_percentage,
            "total_credit_reservations":      total_credit_reservations,
            "total_membership_reservations":  total_membership_reservations,
            "credit_missing_deps":            credit_missing_deps,
            "membership_missing_deps":        membership_missing_deps,
            "credit_inserted_percentage":     credit_inserted_percentage,
            "membership_inserted_percentage": membership_inserted_percentage,
            "no_transactions_inserted":       total_no_transactions_inserted,
            "credit_result":                  credit_result,
            "membership_result":              membership_result,
            "no_transactions_result":         no_transactions_result,
            "main_first_timer_corrected":     main_first_timer_corrected,
        }

    except Exception as e:
        logger.error(f"[process_reservations] FATAL ERROR: {e}")
        raise
