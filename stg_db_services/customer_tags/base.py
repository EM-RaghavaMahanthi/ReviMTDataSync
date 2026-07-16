"""
Orchestrates customer_tags_custom -> customer_tag_assignments.

customer_tags_default is NOT built here — those rows are seeded during account onboarding,
not by this sync (default_tag_id stays NULL on everything we insert).
"""

import logging
from stg_db_services.customer_tags.custom import process_customer_tags_custom
from stg_db_services.customer_tags.assignments import process_customer_tag_assignments

logger = logging.getLogger(__name__)


async def process_customer_tags(account_id: str, location_id: int, engine):
    """
    Main orchestrator for customer_tags processing (manual tags -> assignments).
    location_id is accepted for PROCESSING_ORDER's uniform call signature but unused — neither
    sub-step is location-scoped.
    """
    logger.info(f"[process_customer_tags] Starting orchestrated processing for account_id={account_id}")

    results = {}
    try:
        logger.info(f"[process_customer_tags] Step A: customer_tags_custom (manual tags)")
        try:
            result_custom = await process_customer_tags_custom(account_id, engine)
            results["customer_tags_custom"] = result_custom
            logger.info(f"[process_customer_tags] Step A SUCCESS: {result_custom['inserted_records']} inserted")
        except Exception as e:
            logger.error(f"[process_customer_tags] Step A FAILED: {e}")
            raise

        logger.info(f"[process_customer_tags] Step B: customer_tag_assignments (manual only)")
        try:
            result_assignments = await process_customer_tag_assignments(account_id, engine)
            results["customer_tag_assignments"] = result_assignments
            logger.info(f"[process_customer_tags] Step B SUCCESS: {result_assignments['inserted_records']} inserted")
        except Exception as e:
            logger.error(f"[process_customer_tags] Step B FAILED: {e}")
            raise

        total_inserted = sum(r["inserted_records"] for r in results.values())
        total_staging = sum(r["total_records"] for r in results.values())
        total_duplicates_removed = sum(r["duplicates_removed"] for r in results.values())

        logger.info(
            f"[process_customer_tags] ORCHESTRATION COMPLETE for account_id={account_id}: "
            f"custom={results['customer_tags_custom']['inserted_records']}, "
            f"assignments={results['customer_tag_assignments']['inserted_records']}, "
            f"TOTAL={total_inserted}"
        )

        results["summary"] = {
            "total_duplicates_removed": total_duplicates_removed,
            "total_staging": total_staging,
            "inserted_records": total_inserted,
        }
        results["inserted_records"] = total_inserted

        return results

    except Exception as e:
        logger.error(f"[process_customer_tags] ORCHESTRATION FAILED: {e}")
        results["orchestration_error"] = str(e)
        raise
