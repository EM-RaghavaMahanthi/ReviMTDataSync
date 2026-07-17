"""
customer_tag_assignments — links a customer to a custom_tag_id (manual tags only).

default_tag_id is intentionally NEVER set here: customer_tags_default is seeded during
account onboarding, not by this sync — we don't build or resolve against it at all.
Every row this module inserts has custom_tag_id set and default_tag_id NULL.

Unique key: (account_id, customer_id, custom_tag_id). Must run AFTER customer_tags_custom
is populated (this module's insert joins against it to resolve custom_tag_id).
"""

import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

# TEMP: writing to the _temp versions of both tables to validate writes are correct before
# pointing at the real ones. Flip both back once verified (must match custom.py's _TABLE).
_CUSTOM_TABLE = "customer_tags_custom_temp"
_ASSIGNMENTS_TABLE = "customer_tag_assignments_temp"


async def step_count_ready(account_id: str, engine):
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count
            FROM mt_customer_tags_details_dlk stg
            INNER JOIN mt_user_tags_details_dlk ut
              ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
            INNER JOIN {_CUSTOM_TABLE} cust
              ON cust.account_id = stg.account_id AND cust.customer_id = stg.customer_id AND cust.name = ut.name
            WHERE stg.account_id = :account_id AND ut.tag_type = 'manual'
              AND NOT EXISTS (
                SELECT 1 FROM {_ASSIGNMENTS_TABLE} a
                WHERE a.account_id = stg.account_id AND a.customer_id = stg.customer_id
                  AND a.custom_tag_id = cust.id
              )
        """), {"account_id": account_id})
        return result.fetchone()[0]


async def step_insert(account_id: str, engine):
    insert_sql = text(f"""
        INSERT INTO public.{_ASSIGNMENTS_TABLE} (
          account_id, customer_ref_id, customer_id, custom_tag_id, default_tag_id, created_at, created_by
        )
        SELECT DISTINCT
          stg.account_id, c.id AS customer_ref_id, stg.customer_id, cust.id AS custom_tag_id, NULL, NOW(), 1
        FROM mt_customer_tags_details_dlk stg
        INNER JOIN mt_user_tags_details_dlk ut
          ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
        INNER JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
        INNER JOIN {_CUSTOM_TABLE} cust
          ON cust.account_id = stg.account_id AND cust.customer_id = stg.customer_id AND cust.name = ut.name
        WHERE stg.account_id = :account_id AND ut.tag_type = 'manual'
          AND NOT EXISTS (
            SELECT 1 FROM {_ASSIGNMENTS_TABLE} a
            WHERE a.account_id = stg.account_id AND a.customer_id = stg.customer_id
              AND a.custom_tag_id = cust.id
          )
    """)
    with engine.begin() as conn:
        return conn.execute(insert_sql, {"account_id": account_id}).rowcount


async def process_customer_tag_assignments(account_id: str, engine):
    """
    Insert customer_tag_assignments (manual tags only). Requires customer_tags_custom to
    already be populated for this account (base.py runs it first).
    """
    logger.info(f"[process_customer_tag_assignments] Starting for account_id={account_id}")

    ready = inserted = 0

    try:
        ready = await step_count_ready(account_id, engine)
        inserted = await step_insert(account_id, engine) if ready else 0
        logger.info(f"[process_customer_tag_assignments] SUCCESS for account_id={account_id}: ready={ready}, inserted={inserted}")

        return {
            "duplicates_removed": 0,
            "total_records": ready,
            "existing_records": 0,
            "already_exist_records": 0,
            "ready_to_insert": ready,
            "inserted_records": inserted,
        }

    except Exception as e:
        logger.error(f"[process_customer_tag_assignments] ERROR: Failed processing for account_id={account_id}: {e}")
        raise
