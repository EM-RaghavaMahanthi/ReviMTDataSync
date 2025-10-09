"""
Order Lines Orchestrator Service - Step 7 of ETL Pipeline
Orchestrates sequential processing of order_lines: 7a → 7b → 7c
Handles all transaction types with proper dependency validation
"""

import logging
from sqlalchemy import text

# Try different import patterns for Lambda environment compatibility
try:
    from order_lines_credit_service_07a import process_order_lines_credit
    from order_lines_membership_service_07b import process_order_lines_membership
    from order_lines_other_service_07c import process_order_lines_other
except ImportError:
    try:
        from .order_lines_credit_service_07a import process_order_lines_credit
        from .order_lines_membership_service_07b import process_order_lines_membership
        from .order_lines_other_service_07c import process_order_lines_other
    except ImportError:
        from stg_db_services.order_lines_credit_service_07a import process_order_lines_credit
        from stg_db_services.order_lines_membership_service_07b import process_order_lines_membership
        from stg_db_services.order_lines_other_service_07c import process_order_lines_other

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def process_order_lines(account_id: str, location_id: int, engine):
    """
    Orchestrate sequential processing of all order_lines transaction types
    Execution Order: 7a (Credit) → 7b (Membership) → 7c (Other)
    """
    logger.info(f"[process_order_lines] Starting orchestrated processing for account_id={account_id}, location_id={location_id}")
    
    results = {}
    
    try:
        # Service 7a: Process CreditTransaction order_lines
        logger.info(f"[process_order_lines] Step 7a: Processing CreditTransaction order_lines")
        try:
            result_7a = await process_order_lines_credit(account_id, location_id, engine)
            results["credit_transactions"] = result_7a
            logger.info(f"[process_order_lines] Step 7a SUCCESS: CreditTransaction - Inserted {result_7a['inserted_records']} records")
        except Exception as e:
            logger.error(f"[process_order_lines] Step 7a FAILED: CreditTransaction processing error: {e}")
            results["credit_transactions"] = {"error": str(e)}
            raise
        
        # Service 7b: Process MembershipTransaction order_lines
        logger.info(f"[process_order_lines] Step 7b: Processing MembershipTransaction order_lines")
        try:
            result_7b = await process_order_lines_membership(account_id, location_id, engine)
            results["membership_transactions"] = result_7b
            logger.info(f"[process_order_lines] Step 7b SUCCESS: MembershipTransaction - Inserted {result_7b['inserted_records']} records")
        except Exception as e:
            logger.error(f"[process_order_lines] Step 7b FAILED: MembershipTransaction processing error: {e}")
            results["membership_transactions"] = {"error": str(e)}
            raise
        
        # Service 7c: Process Other transaction types order_lines
        logger.info(f"[process_order_lines] Step 7c: Processing Other transaction order_lines")
        try:
            result_7c = await process_order_lines_other(account_id, location_id, engine)
            results["other_transactions"] = result_7c
            logger.info(f"[process_order_lines] Step 7c SUCCESS: Other transactions - Inserted {result_7c['inserted_records']} records")
        except Exception as e:
            logger.error(f"[process_order_lines] Step 7c FAILED: Other transaction processing error: {e}")
            results["other_transactions"] = {"error": str(e)}
            raise
        
        # Calculate totals across all transaction types
        total_duplicates_found = (
            results["credit_transactions"]["duplicates_found"] + 
            results["membership_transactions"]["duplicates_found"] + 
            results["other_transactions"]["duplicates_found"]
        )
        total_duplicates_removed = (
            results["credit_transactions"]["duplicates_removed"] + 
            results["membership_transactions"]["duplicates_removed"] + 
            results["other_transactions"]["duplicates_removed"]
        )
        
        # Get actual total staging records after cleanup from database
        try:
            with engine.begin() as conn:
                total_staging_result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM mt_order_lines_details_dlk
                    WHERE account_id = :account_id
                      AND location = :location_id
                """), {"account_id": account_id, "location_id": str(location_id)})
                total_staging_after_cleanup = total_staging_result.fetchone()[0]
            logger.info(f"[process_order_lines] Total staging records after cleanup: {total_staging_after_cleanup}")
        except Exception as e:
            logger.error(f"[process_order_lines] ERROR: Failed to count total staging records: {e}")
            # Fallback to sum of individual counts
            total_staging_after_cleanup = (
                results["credit_transactions"]["total_staging_after_cleanup"] + 
                results["membership_transactions"]["total_staging_after_cleanup"] + 
                results["other_transactions"]["total_staging_after_cleanup"]
            )
        total_already_exist = (
            results["credit_transactions"]["already_exist_in_main_table"] + 
            results["membership_transactions"]["already_exist_in_main_table"] + 
            results["other_transactions"]["already_exist_in_main_table"]
        )
        total_invalid_dependencies = (
            results["credit_transactions"]["invalid_dependency_records"] + 
            results["membership_transactions"]["invalid_dependency_records"] + 
            results["other_transactions"]["invalid_dependency_records"]
        )
        total_inserted = (
            results["credit_transactions"]["inserted_records"] + 
            results["membership_transactions"]["inserted_records"] + 
            results["other_transactions"]["inserted_records"]
        )
        
        # Calculate comprehensive percentages
        credit_missing_percentage = results["credit_transactions"].get("missing_dependency_percentage", 0.0)
        membership_missing_percentage = results["membership_transactions"].get("missing_dependency_percentage", 0.0)
        other_missing_percentage = results["other_transactions"].get("missing_dependency_percentage", 0.0)
        
        # Overall percentages based on total staging records vs total inserted
        total_not_inserted = total_staging_after_cleanup - total_inserted  # Records that were not successfully inserted
        overall_missing_percentage = round((total_not_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        overall_inserted_percentage = round((total_inserted / total_staging_after_cleanup * 100), 2) if total_staging_after_cleanup > 0 else 0.0
        
        # Summary logging with percentages
        logger.info(f"[process_order_lines] ORCHESTRATION COMPLETE - ALL TRANSACTION TYPES PROCESSED:")
        logger.info(f"  - CreditTransaction: {results['credit_transactions']['inserted_records']} inserted | Missing deps: {results['credit_transactions']['invalid_dependency_records']} ({credit_missing_percentage}%)")
        logger.info(f"  - MembershipTransaction: {results['membership_transactions']['inserted_records']} inserted | Missing deps: {results['membership_transactions']['invalid_dependency_records']} ({membership_missing_percentage}%)")
        logger.info(f"  - Other transactions: {results['other_transactions']['inserted_records']} inserted | Missing deps: {results['other_transactions']['invalid_dependency_records']} ({other_missing_percentage}%)")
        logger.info(f"  - TOTAL SUMMARY:")
        logger.info(f"    * Total duplicates found: {total_duplicates_found}")
        logger.info(f"    * Total duplicates removed: {total_duplicates_removed}")
        logger.info(f"    * Total staging records after cleanup: {total_staging_after_cleanup}")
        logger.info(f"    * Total already exist in main table: {total_already_exist}")
        logger.info(f"    * Total not inserted: {total_not_inserted} ({overall_missing_percentage}%) | TOTAL INSERTED: {total_inserted} ({overall_inserted_percentage}%)")
        
        # Add summary to results with percentages
        results["summary"] = {
            "total_duplicates_found": total_duplicates_found,
            "total_duplicates_removed": total_duplicates_removed,
            "total_staging_after_cleanup": total_staging_after_cleanup,
            "total_already_exist_in_main_table": total_already_exist,
            "total_invalid_dependency_records": total_invalid_dependencies,
            "total_not_inserted": total_not_inserted,
            "overall_missing_percentage": overall_missing_percentage,
            "total_inserted_records": total_inserted,
            "overall_inserted_percentage": overall_inserted_percentage,
            "credit_missing_percentage": credit_missing_percentage,
            "membership_missing_percentage": membership_missing_percentage,
            "other_missing_percentage": other_missing_percentage
        }
        
        return results
        
    except Exception as e:
        logger.error(f"[process_order_lines] ORCHESTRATION FAILED: {e}")
        results["orchestration_error"] = str(e)
        raise