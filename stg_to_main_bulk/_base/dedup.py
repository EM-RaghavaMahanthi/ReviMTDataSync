import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


def drop_staging_duplicates(engine, table: str, key_col: str, account_id) -> dict:
    """
    Remove duplicate rows from a staging table keeping one row per key_col value.

    Bulk variant of stg_db_services/_base/dedup.py: no location_id filter, because the
    bulk staging tables are unique on (account_id, business key) and one pass covers all
    of an account's locations.

    s3_to_stg_bulk already dedups to the newest Silver row per key before COPY, so this is
    a belt-and-braces pass — it should report zero on a healthy run, and a non-zero count
    means the staging unique index was not doing its job.

    Args:
        engine:      SQLAlchemy engine
        table:       Staging table name (e.g. 'stg_orders_bulk')
        key_col:     Column to deduplicate on (e.g. 'order_id', 'reservations_id')
        account_id:  Required filter
    """
    params = {"account_id": account_id}

    with engine.begin() as conn:
        stats = conn.execute(text(f"""
            SELECT COUNT(*) AS total, COUNT(DISTINCT {key_col}) AS unique_records
            FROM {table}
            WHERE account_id = :account_id
        """), params).fetchone()

        total_before, unique_records = stats[0], stats[1]
        duplicates_found = total_before - unique_records

        if duplicates_found > 0:
            logger.warning(f"[{table}] {duplicates_found} duplicates found ({total_before} total, {unique_records} unique) — removing")
            deleted = conn.execute(text(f"""
                DELETE FROM {table}
                WHERE ctid NOT IN (
                    SELECT DISTINCT ON ({key_col}) ctid
                    FROM {table}
                    WHERE account_id = :account_id
                    ORDER BY {key_col}, ctid
                )
                AND account_id = :account_id
            """), params).rowcount
        else:
            logger.info(f"[{table}] No duplicates found")
            deleted = 0

    return {"duplicates_found": duplicates_found, "duplicates_removed": deleted}
