"""
Read id sets straight from RDS, for Stage 1 shards that need to filter by them.

The customers table in RDS is populated by the bulk/silver pipeline, independently of this
sync, and for an already-onboarded account it is the authoritative set. The notes/tags
backfill therefore has no reason to re-derive that set from a fresh CRM download — it can
read it here.

Why this is safe to do from Stage 1: revi-syncdata-test runs inside the VPC with
DATABASE_URL set and the psycopg2 / sqlalchemy layers attached, so it can reach Aurora
directly. The transaction id lists go via S3 only because they cross a Step Functions
boundary between two Lambda invocations; there is no such boundary here.

Deliberately does NOT import core.stg_db_config — that module declares five required Auth0 /
API fields with no defaults, so importing it would raise a pydantic ValidationError before
any code runs. Same reasoning as handlers/backfill/handler.py. DATABASE_URL is read straight
from the environment.
"""

import os
import logging

from sqlalchemy import create_engine, text

from utils.s3_writer import _normalise_id

logger = logging.getLogger(__name__)


async def read_customer_ids_from_rds(account_id: str) -> list:
    """
    Every distinct customer_id RDS holds for this account.

    customers.customer_id IS the MarianaTek user id — Stage 3 already joins staging to it
    directly (stg.customer_id = c.customer_id AND stg.account_id = c.account_id), so no id
    translation is involved and these values can be used as CRM filter values as-is.

    Normalised through _normalise_id for the same reason the S3 path is: a column that
    permits NULL can surface as a float, and "1847.0" matches nothing — the request succeeds,
    returns zero records, and looks like the ids simply do not exist.

    Account-scoped, not location-scoped. That is the point: RDS is keyed by account_id while
    the CRM fetch is keyed by home_location, so a multi-location account needs one pass here
    rather than one per location.
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set — cannot read customer ids from RDS. "
            "Either set it on this Lambda or use customer_id_source='s3'."
        )

    engine = create_engine(database_url)
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT customer_id
                FROM public.customers
                WHERE account_id = :account_id
                  AND customer_id IS NOT NULL
            """), {"account_id": account_id}).fetchall()
    finally:
        engine.dispose()

    ids = [i for i in (_normalise_id(r[0]) for r in rows) if i]
    logger.info(f"[RDS] Read {len(ids)} distinct customer_ids for account_id={account_id}")
    return ids
