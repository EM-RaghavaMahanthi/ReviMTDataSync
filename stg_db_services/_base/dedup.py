import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


def drop_staging_duplicates(engine, table: str, key_col: str, account_id, location_id=None) -> dict:
    """
    Remove duplicate rows from a staging table keeping one row per key_col value.

    Args:
        engine:      SQLAlchemy engine
        table:       Staging table name (e.g. 'mt_orders_details_dlk')
        key_col:     Column to deduplicate on (e.g. 'order_id', 'reservations_id')
        account_id:  Required filter
        location_id: Optional — if provided, adds AND location = :location_id
    """
    params = {"account_id": account_id}
    loc_filter = ""
    if location_id is not None:
        params["location_id"] = str(location_id)
        loc_filter = "AND location = :location_id"

    with engine.begin() as conn:
        stats = conn.execute(text(f"""
            SELECT COUNT(*) AS total, COUNT(DISTINCT {key_col}) AS unique_records
            FROM {table}
            WHERE account_id = :account_id {loc_filter}
        """), params).fetchone()

        total_before, unique_records = stats[0], stats[1]
        duplicates_found = total_before - unique_records

        if duplicates_found > 0:
            logger.info(f"[{table}] {duplicates_found} duplicates found ({total_before} total, {unique_records} unique) — removing")
            deleted = conn.execute(text(f"""
                DELETE FROM {table}
                WHERE ctid NOT IN (
                    SELECT DISTINCT ON ({key_col}) ctid
                    FROM {table}
                    WHERE account_id = :account_id {loc_filter}
                    ORDER BY {key_col}, ctid
                )
                AND account_id = :account_id {loc_filter}
            """), params).rowcount
        else:
            logger.info(f"[{table}] No duplicates found")
            deleted = 0

    return {"duplicates_found": duplicates_found, "duplicates_removed": deleted}
