"""
customer_tag_assignments — links a customer to a tag definition in customer_tags_default.
The schema dropped custom_tag_id and the customer_tags_custom model entirely in the backend's
Group Tag work (Revi-backend 776c0389, 2026-08-13). This insert used to write
`NULL::integer AS custom_tag_id`; against a migrated database that fails outright with
"column custom_tag_id does not exist", so it is gone from the column list here too.

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


async def step_count_breakdown(account_id: str, engine) -> dict:
    """
    Count what step_insert will actually write, and account for every row it will not.

    This used to be a plain COUNT(*) over the staging join, which over-reported: the insert
    applies two further filters that the count did not, so `ready` came back higher than the
    rows inserted with no explanation. Account 5172 read ready=600, inserted=519.

      - INNER JOIN customers — the insert needs c.id for customer_ref_id, so assignments
        whose customer_id is absent from `customers` (deleted/merged) are silently dropped.
      - SELECT DISTINCT — nothing dedups mt_customer_tags_details_dlk, so repeated
        (customer_id, tag_id) staging rows collapse into one inserted row.

    Both are correct behaviour; only the count was wrong. Mirroring them here makes `ready`
    a prediction of the insert rather than an upper bound, and splits the difference into
    named buckets instead of leaving it as an unexplained gap.

    DISTINCT is on (customer_ref_id, customer_id, default_tag_id) rather than
    (customer_id, default_tag_id) to match the insert exactly: account_id is constant, but
    customer_ref_id is NOT functionally determined by customer_id if
    `customers` holds duplicate (account_id, customer_id) rows — in which case the insert
    genuinely does write one assignment per duplicate, and the count must say so too.
    """
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            WITH j AS (
                SELECT stg.customer_id, def.id AS default_tag_id, c.id AS customer_ref_id
                FROM mt_customer_tags_details_dlk stg
                INNER JOIN mt_user_tags_details_dlk ut
                  ON stg.tag_id = ut.tag_id AND stg.account_id = ut.account_id
                INNER JOIN {_DEFAULT_TABLE} def
                  ON def.tenant_name = ut.tenant_name AND def.crm_tag_id = ut.tag_id
                LEFT JOIN customers c
                  ON stg.customer_id = c.customer_id AND stg.account_id = c.account_id
                WHERE stg.account_id = :account_id
                  AND NOT EXISTS (
                    SELECT 1 FROM {_ASSIGNMENTS_TABLE} a
                    WHERE a.account_id = stg.account_id AND a.customer_id = stg.customer_id
                      AND a.default_tag_id = def.id
                  )
            )
            SELECT
              COUNT(*) AS raw_rows,
              COUNT(*) FILTER (WHERE customer_ref_id IS NULL) AS missing_customer,
              COUNT(DISTINCT (customer_ref_id, customer_id, default_tag_id))
                FILTER (WHERE customer_ref_id IS NOT NULL) AS ready
            FROM j
        """), {"account_id": account_id})
        raw_rows, missing_customer, ready = result.fetchone()

    # Whatever survived the customers join but folded away under DISTINCT.
    collapsed = raw_rows - missing_customer - ready
    if missing_customer:
        logger.warning(
            f"[process_customer_tag_assignments] {missing_customer} staging rows reference a "
            f"customer_id not present in customers — excluded by the insert's INNER JOIN"
        )
    if collapsed:
        logger.warning(
            f"[process_customer_tag_assignments] {collapsed} duplicate staging rows collapse "
            f"under DISTINCT (repeated customer_id/tag_id in mt_customer_tags_details_dlk)"
        )
    return {
        "raw_rows": raw_rows,
        "missing_customer": missing_customer,
        "duplicate_rows_collapsed": collapsed,
        "ready": ready,
    }


async def step_insert(account_id: str, engine):
    insert_sql = text(f"""
        INSERT INTO public.{_ASSIGNMENTS_TABLE} (
          account_id, customer_ref_id, customer_id, default_tag_id, created_at, created_by
        )
        SELECT DISTINCT
          stg.account_id, c.id AS customer_ref_id, stg.customer_id,
          def.id AS default_tag_id, NOW(), -1
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
        counts = await step_count_breakdown(account_id, engine)
        ready = counts["ready"]
        logger.info(
            f"[process_customer_tag_assignments] staging={counts['raw_rows']} "
            f"- missing_customer={counts['missing_customer']} "
            f"- duplicate_collapsed={counts['duplicate_rows_collapsed']} = ready={ready}"
        )

        if ready and not write:
            logger.warning(
                f"[process_customer_tag_assignments] WRITE DISABLED — {ready} rows would "
                f"have been inserted into customer_tag_assignments; inserting nothing"
            )
        inserted = await step_insert(account_id, engine) if (ready and write) else 0

        # ready now mirrors the insert's own filters, so these must agree. Reported rather
        # than raised: the insert has already committed by this point, and failing the run
        # after the fact would neither undo it nor tell anyone more than this line does.
        if write and inserted != ready:
            logger.error(
                f"[process_customer_tag_assignments] COUNT DRIFT for account_id={account_id}: "
                f"predicted {ready}, inserted {inserted}. step_count_breakdown and step_insert "
                f"have diverged — their joins and DISTINCT must stay identical."
            )

        logger.info(f"[process_customer_tag_assignments] SUCCESS for account_id={account_id}: ready={ready}, inserted={inserted}")

        return {
            # Nothing is physically deleted from staging here (unlike customer_notes /
            # customer_tags_default, which run drop_staging_duplicates) — duplicates are
            # collapsed by the insert's DISTINCT instead, and counted separately below.
            "duplicates_removed": 0,
            "total_records": counts["raw_rows"],
            "existing_records": 0,
            "already_exist_records": 0,
            "missing_customer_records": counts["missing_customer"],
            "duplicate_rows_collapsed": counts["duplicate_rows_collapsed"],
            "ready_to_insert": ready,
            "inserted_records": inserted,
        }

    except Exception as e:
        logger.error(f"[process_customer_tag_assignments] ERROR: Failed processing for account_id={account_id}: {e}")
        raise
