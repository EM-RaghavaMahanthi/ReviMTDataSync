"""
customer_tag_assignments — links a customer to a tag definition in customer_tags_default.
custom_tag_id always stays NULL for CRM-sourced tags (customer_tags_custom is no longer
populated by this sync — reserved for a future feature where staff create tags directly in our
product, not sourced from MT).

Resolves default_tag_id via customer_tags_default(tenant_name, crm_tag_id) — matching the Prisma
@@unique([customer_ref_id, default_tag_id]) constraint.

Must run AFTER customer_tags_default is populated (this module's insert joins against it to
resolve the FK).
"""

import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

_DEFAULT_TABLE = "customer_tags_default"
_ASSIGNMENTS_TABLE = "customer_tag_assignments"


async def step_count_ready(account_id: str, engine):
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count
            FROM mt_customer_tags_details_dlk stg
            INNER JOIN mt_user_tags_details_dlk ut
              ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
            INNER JOIN {_DEFAULT_TABLE} def
              ON def.tenant_name = ut.tenant_name AND def.crm_tag_id = ut.tag_id
            WHERE stg.account_id = :account_id
              AND NOT EXISTS (
                SELECT 1 FROM {_ASSIGNMENTS_TABLE} a
                WHERE a.account_id = stg.account_id AND a.customer_id = stg.customer_id
                  AND a.default_tag_id = def.id
              )
        """), {"account_id": account_id})
        return result.fetchone()[0]


async def step_insert(account_id: str, engine):
    insert_sql = text(f"""
        INSERT INTO public.{_ASSIGNMENTS_TABLE} (
          account_id, customer_ref_id, customer_id, custom_tag_id, default_tag_id, created_at, created_by
        )
        SELECT DISTINCT
          stg.account_id, c.id AS customer_ref_id, stg.customer_id,
          NULL::integer AS custom_tag_id, def.id AS default_tag_id, NOW(), -1
        FROM mt_customer_tags_details_dlk stg
        INNER JOIN mt_user_tags_details_dlk ut
          ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
        INNER JOIN customers c ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
        INNER JOIN {_DEFAULT_TABLE} def
          ON def.tenant_name = ut.tenant_name AND def.crm_tag_id = ut.tag_id
        WHERE stg.account_id = :account_id
          AND NOT EXISTS (
            SELECT 1 FROM {_ASSIGNMENTS_TABLE} a
            WHERE a.account_id = stg.account_id AND a.customer_id = stg.customer_id
              AND a.default_tag_id = def.id
          )
    """)
    with engine.begin() as conn:
        return conn.execute(insert_sql, {"account_id": account_id}).rowcount


async def process_customer_tag_assignments(account_id: str, engine, write: bool = True):
    """
    Insert customer_tag_assignments rows. Requires customer_tags_default to already be
    populated for this account (base.py runs it first).
    """
    logger.info(f"[process_customer_tag_assignments] Starting for account_id={account_id}")

    try:
        ready = await step_count_ready(account_id, engine)
        if ready and not write:
            logger.warning(
                f"[process_customer_tag_assignments] WRITE DISABLED — {ready} rows would "
                f"have been inserted into customer_tag_assignments; inserting nothing"
            )
        inserted = await step_insert(account_id, engine) if (ready and write) else 0
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
