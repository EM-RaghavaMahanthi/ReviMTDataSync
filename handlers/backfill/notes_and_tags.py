"""
event_type = "backfill_notes_and_tags" — runs Stage 3 (staging -> main RDS) for ONLY
customer_notes and customer_tags (customer_tags_default + customer_tag_assignments),
without running the other 9 PROCESSING_ORDER steps in handlers/stg_to_db.py.

Staging (mt_user_notes_details_dlk, mt_user_tags_details_dlk, mt_customer_tags_details_dlk)
must already be populated (by Stage 2 / revi-crm-db-sync) before this runs.
"""

import logging
from stg_db_services.customer_notes import process_customer_notes
from stg_db_services.customer_tags.base import process_customer_tags

logger = logging.getLogger(__name__)


async def run(account_id: str, engine, write: bool = True) -> dict:
    """
    Run customer_notes then customer_tags Stage 3 for one account.
    location_id is not needed by either — both are account_id-only (see their docstrings) —
    so we pass None rather than requiring the caller to supply one.
    """
    logger.info(
        f"[backfill_notes_and_tags] Starting for account_id={account_id} write={write}"
    )

    notes_result = await process_customer_notes(account_id, None, engine, write=write)
    tags_result = await process_customer_tags(account_id, None, engine, write=write)

    total_inserted = notes_result["inserted_records"] + tags_result["inserted_records"]
    logger.info(
        f"[backfill_notes_and_tags] DONE for account_id={account_id}: "
        f"customer_notes={notes_result['inserted_records']}, "
        f"customer_tags={tags_result['inserted_records']}, total={total_inserted}, "
        f"write={write}"
    )

    return {
        "customer_notes": notes_result,
        "customer_tags": tags_result,
        "total_inserted": total_inserted,
    }
