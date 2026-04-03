import io
import os
import json
import ast
import math
import time
import logging
import traceback
import concurrent.futures
from typing import List, Dict

import boto3
import pandas as pd
from sqlalchemy import text

from core.config import settings
from db_services.update_stale import update_stale_data

logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

TABLE_COLUMNS_MAP = {
    "customers": [
        "id", "customer_id", "location_id", "account_id", "first_name", "last_name",
        "email", "full_name", "birth_date", "birth_month", "birth_day", "phone_number",
        "address_line1", "address_line2", "address_line3", "city", "country",
        "state_province", "customer_state", "postal_code", "gender", "date_joined",
        "is_opted_in_to_sms", "completed_class_count", "state_id",
        "created_at", "created_by", "updated_at", "updated_by", "deleted_at", "deleted_by",
    ],
    "class_sessions": [
        "id", "class_session_id", "start_datetime", "start_date", "location",
        "end_datetime", "cancellation_datetime",
        "created_at", "created_by", "updated_at", "updated_by", "deleted_at", "deleted_by",
        "account_id",
    ],
    "orders": [
        "id", "order_id", "date_placed", "location", "location_id",
        "payment_sources_labels", "status", "order_lines_id", "parent_order",
        "customer_id", "created_at", "created_by", "updated_at", "updated_by",
        "deleted_at", "deleted_by", "account_id", "customer_ref_id",
    ],
    "credit_transactions": [
        "id", "credit_transactions_id", "transaction_date", "credit_name",
        "is_expired", "remaining_credits_cache", "is_intro_offer",
        "parent_credit_transaction_type", "parent_credit_transaction_id",
        "customer_id", "location", "isin_order_line", "isin_reservation",
        "created_at", "created_by", "updated_at", "updated_by",
        "deleted_at", "deleted_by", "account_id", "customer_ref_id",
    ],
    "membership_transactions": [
        "id", "membership_transactions_id", "transaction_date", "membership_name",
        "parent_membership_transaction_id", "membership_instances_id",
        "customer_id", "location", "payment_interval_end_date",
        "isin_order_line", "isin_reservation",
        "created_at", "created_by", "updated_at", "updated_by",
        "deleted_at", "deleted_by", "account_id", "customer_ref_id",
        "membership_instances_ref_id",
    ],
    "membership_instances": [
        "id", "membership_instances_id", "purchase_date", "membership_name",
        "renewal_rate_incl_tax", "status", "location", "renewal_count",
        "next_charge_date", "created_at", "created_by", "updated_at", "updated_by",
        "deleted_at", "deleted_by", "account_id",
    ],
    "order_lines": [
        "id", "order_line_id", "order_id", "transaction_type", "location",
        "credit_transactions_id", "membership_transactions_id", "title", "line_total",
        "processed_by", "child_orders", "is_valid",
        "created_at", "created_by", "updated_at", "updated_by",
        "deleted_at", "deleted_by", "account_id",
        "credit_transactions_ref_id", "membership_transactions_ref_id", "order_ref_id",
    ],
    "reservations": [
        "id", "reservations_id", "cancel_date", "check_in_date", "creation_date",
        "status", "credit_transactions_type", "credit_transactions_id",
        "credit_transactions_ref_id", "membership_transactions_type",
        "membership_transactions_id", "membership_transactions_ref_id",
        "guest", "customer_id", "customer_ref_id", "class_session_id",
        "class_session_ref_id", "account_id", "first_timer", "reservation_type",
        "location", "created_at", "created_by", "updated_at", "updated_by",
        "deleted_at", "deleted_by", "transaction_type",
    ],
}

DATETIME_COLUMNS = {
    "birth_date", "date_joined", "created_at", "updated_at", "deleted_at",
    "date_placed", "start_datetime", "start_date", "end_datetime",
    "cancellation_datetime", "transaction_date", "payment_interval_end_date",
    "purchase_date", "next_charge_date", "cancel_date", "check_in_date", "creation_date",
}

VARCHAR_LIMITS = {
    "first_name": 64, "last_name": 64, "email": 128, "full_name": 128,
    "phone_number": 32, "address_line1": 128, "address_line2": 128,
    "address_line3": 128, "city": 64, "country": 64, "state_province": 64,
    "customer_state": 64, "postal_code": 32, "gender": 16, "status": 64,
    "credit_name": 128, "membership_name": 128, "title": 256,
    "processed_by": 64, "reservation_type": 64, "transaction_type": 64,
    "credit_transactions_type": 64, "membership_transactions_type": 64,
    "parent_credit_transaction_type": 64, "payment_sources_labels": 256,
}

TABLE_S3_CONFIG = {
    "customers":               {"s3_prefix": settings.CUSTOMERS_S3_PREFIX,               "staging_table": "mt_customers_details_dlk"},
    "orders":                  {"s3_prefix": settings.ORDERS_S3_PREFIX,                  "staging_table": "mt_orders_details_dlk"},
    "order_lines":             {"s3_prefix": settings.ORDER_LINES_S3_PREFIX,             "staging_table": "mt_order_lines_details_dlk"},
    "class_sessions":          {"s3_prefix": settings.CLASS_SESSIONS_S3_PREFIX,          "staging_table": "mt_class_sessions_details_dlk"},
    "reservations":            {"s3_prefix": settings.RESERVATIONS_S3_PREFIX,            "staging_table": "mt_reservations_details_dlk"},
    "membership_instances":    {"s3_prefix": settings.MEMBERSHIP_INSTANCES_S3_PREFIX,    "staging_table": "mt_membership_instances_details_dlk"},
    "credit_transactions":     {"s3_prefix": settings.CREDIT_TRANSACTIONS_S3_PREFIX,     "staging_table": "mt_credit_transactions_details_dlk"},
    "membership_transactions": {"s3_prefix": settings.MEMBERSHIP_TRANSACTIONS_S3_PREFIX, "staging_table": "mt_membership_transactions_details_dlk"},
}

# ---------------------------------------------------------------------------
# S3 fetch
# ---------------------------------------------------------------------------

def _read_parquet(args):
    import awswrangler as wr
    bucket, key = args
    df = wr.s3.read_parquet(f"s3://{bucket}/{key}")
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
    return df


def fetch_from_s3(bucket: str, prefix: str, account_id: str) -> pd.DataFrame:
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [o["Key"] for o in objects.get("Contents", []) if o["Key"].endswith(".parquet")]

    if account_id:
        keys = [k for k in keys if f"account_id_{account_id}/" in k]

    if not keys:
        logger.warning(f"No parquet files in s3://{bucket}/{prefix}")
        return pd.DataFrame()

    max_threads = min(settings.NUM_THREADS, os.cpu_count() or 1)
    dfs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        dfs = list(executor.map(_read_parquet, [(bucket, k) for k in keys]))

    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ---------------------------------------------------------------------------
# Row cleaning
# ---------------------------------------------------------------------------

def _fix_2digit_year(val: str) -> str:
    parts = val.split("-")
    if len(parts) == 3 and len(parts[0]) == 2 and parts[0].isdigit():
        return f"{1900 + int(parts[0])}-{parts[1]}-{parts[2]}"
    return val


def clean_row(row: dict, expected_columns: list) -> dict:
    clean = {}
    for col in expected_columns:
        val = row.get(col)

        if hasattr(val, "__iter__") and not isinstance(val, str):
            if hasattr(val, "tolist"):
                val = str(val.tolist()) if val.size > 0 else "[]"
            elif isinstance(val, (list, tuple)):
                val = str(list(val))
            else:
                val = str(val) if val is not None else None
        elif val == "" or (isinstance(val, str) and val.strip().lower() in ("nan", "none", "null", "nat")):
            val = None
        elif isinstance(val, str) and col in DATETIME_COLUMNS and val.strip():
            val = _fix_2digit_year(val.strip())
        elif isinstance(val, float) and math.isnan(val):
            val = None
        elif pd.isna(val):
            val = None
        elif hasattr(val, "isoformat"):
            val = val.strftime("%Y-%m-%d %H:%M:%S")
        elif col in DATETIME_COLUMNS and isinstance(val, (int, float)) and 1_000_000_000 <= val <= 9_999_999_999_999:
            try:
                dt = pd.to_datetime(val, unit="ms" if val > 1_000_000_000_000 else "s")
                val = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass

        clean[col] = val

    for col in expected_columns:
        clean.setdefault(col, None)
    return clean


# ---------------------------------------------------------------------------
# Bulk insert (PostgreSQL COPY)
# ---------------------------------------------------------------------------

def _to_csv_field(val, col: str) -> str:
    if val is None:
        return "\\N"
    if isinstance(val, float):
        if math.isnan(val):
            return "\\N"
        if col in DATETIME_COLUMNS and 1_000_000_000 <= val <= 9_999_999_999_999:
            try:
                dt = pd.to_datetime(val, unit="ms" if val > 1_000_000_000_000 else "s")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        return str(int(val)) if val == int(val) else str(val)
    if isinstance(val, int):
        if col in DATETIME_COLUMNS and 1_000_000_000 <= val <= 9_999_999_999_999:
            try:
                dt = pd.to_datetime(val, unit="ms" if val > 1_000_000_000_000 else "s")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        return str(val)
    try:
        if pd.isna(val):
            return "\\N"
    except Exception:
        pass
    if isinstance(val, str):
        if val.strip().lower() in ("nan", "none", "nat"):
            return "\\N"
        if col in DATETIME_COLUMNS:
            val = _fix_2digit_year(val.strip())
        if col in VARCHAR_LIMITS and len(val) > VARCHAR_LIMITS[col]:
            logger.warning(f"Truncating {col}: {len(val)} → {VARCHAR_LIMITS[col]} chars")
            val = val[:VARCHAR_LIMITS[col]]
        val = val.replace('"', '""')
        if "," in val or '"' in val or "\n" in val or "\r" in val:
            val = f'"{val}"'
        return val
    return str(val)


def bulk_insert(table_name: str, rows: List[Dict], engine, columns: list = None):
    columns = columns or TABLE_COLUMNS_MAP.get(table_name, [])
    if not rows:
        logger.warning(f"[{table_name}] no rows to insert")
        return

    output = io.StringIO()
    for row in rows:
        output.write(",".join(_to_csv_field(row.get(col), col) for col in columns) + "\n")
    output.seek(0)

    raw = engine.raw_connection()
    cursor = raw.cursor()
    try:
        copy_sql = f"COPY {table_name} ({','.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        cursor.copy_expert(copy_sql, output)
        raw.commit()
        logger.info(f"[{table_name}] inserted {len(rows)} rows")
    except Exception as e:
        logger.error(f"[{table_name}] bulk insert failed: {e}")
        raise
    finally:
        cursor.close()
        raw.close()


# ---------------------------------------------------------------------------
# order_lines validation
# ---------------------------------------------------------------------------

def validate_order_lines(cleaned_rows: List[Dict], processed_order_ids: set) -> List[Dict]:
    """
    Set is_valid=False on order_lines that are deferred payment placeholders.
    A row is invalid if its child_orders contain order_ids that exist in processed_order_ids.
    Returns ALL rows with is_valid flag set — no filtering.
    """
    existing = {str(oid) for oid in processed_order_ids if oid}
    valid_count = invalid_count = 0

    for row in cleaned_rows:
        is_valid = True
        child_orders = row.get("child_orders")

        if child_orders:
            child_ids: set = set()
            if isinstance(child_orders, str) and child_orders.strip() not in ("[]", "", "None", "null"):
                try:
                    parsed = json.loads(child_orders)
                    child_ids = {str(c).strip() for c in parsed if c}
                except (json.JSONDecodeError, ValueError):
                    try:
                        parsed = ast.literal_eval(child_orders)
                        child_ids = {str(c).strip() for c in parsed if c}
                    except (ValueError, SyntaxError):
                        raw = child_orders.strip().strip("[]'\"")
                        child_ids = {c.strip() for c in raw.split(",") if c.strip()} if "," in raw else ({raw} if raw else set())
            elif isinstance(child_orders, list):
                child_ids = {str(c).strip() for c in child_orders if c}

            if child_ids & existing:
                is_valid = False
                logger.warning(f"[order_lines] order_line_id={row.get('order_line_id')} invalid — "
                               f"child_orders {child_ids & existing} exist in orders")

        row["is_valid"] = is_valid
        valid_count += is_valid
        invalid_count += not is_valid

    logger.info(f"[order_lines] validation: {valid_count} valid, {invalid_count} invalid")
    return cleaned_rows


# ---------------------------------------------------------------------------
# Transaction flag helpers
# ---------------------------------------------------------------------------

def add_credit_transaction_flags(rows: List[Dict], ol_ids: set, res_ids: set) -> List[Dict]:
    ol_set = {cid for cid in ol_ids if cid is not None}
    res_set = {cid for cid in res_ids if cid is not None}
    ol_matches = res_matches = 0
    for row in rows:
        cid = row.get("credit_transactions_id")
        row["isin_order_line"] = cid in ol_set if cid is not None else False
        row["isin_reservation"] = cid in res_set if cid is not None else False
        if row["isin_order_line"]: ol_matches += 1
        if row["isin_reservation"]: res_matches += 1
    logger.info(f"[credit_transactions] {ol_matches} in order_lines, {res_matches} in reservations")
    return rows


def add_membership_transaction_flags(rows: List[Dict], ol_ids: set, res_ids: set) -> List[Dict]:
    ol_set = {mid for mid in ol_ids if mid is not None}
    res_set = {mid for mid in res_ids if mid is not None}
    ol_matches = res_matches = 0
    for row in rows:
        mid = row.get("membership_transactions_id")
        row["isin_order_line"] = mid in ol_set if mid is not None else False
        row["isin_reservation"] = mid in res_set if mid is not None else False
        if row["isin_order_line"]: ol_matches += 1
        if row["isin_reservation"]: res_matches += 1
    logger.info(f"[membership_transactions] {ol_matches} in order_lines, {res_matches} in reservations")
    return rows


# ---------------------------------------------------------------------------
# Targeted order_lines staging (backfill use)
# ---------------------------------------------------------------------------

def stage_order_lines_only(bucket: str, account_id: str, engine) -> int:
    """
    Create mt_order_lines_details_dlk, fetch order_lines parquet from S3,
    and bulk-insert into staging. Returns number of rows staged.
    Used by the backfill lambda — does not touch any other staging table.
    """
    create_sql = """
        DROP TABLE IF EXISTS "mt_order_lines_details_dlk";
        CREATE TABLE "mt_order_lines_details_dlk" (
          id integer,
          order_line_id character varying(64),
          order_id text,
          transaction_type character varying(255),
          location character varying(255),
          credit_transactions_id integer,
          membership_transactions_id integer,
          title character varying(255),
          line_total double precision,
          processed_by boolean,
          child_orders text,
          is_valid boolean DEFAULT TRUE,
          created_at timestamp(3) without time zone,
          created_by integer,
          updated_at timestamp(3) without time zone,
          updated_by integer,
          deleted_at timestamp(3) without time zone,
          deleted_by integer,
          account_id integer,
          credit_transactions_ref_id integer,
          membership_transactions_ref_id integer,
          order_ref_id integer
        );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    logger.info("[stage_order_lines_only] Staging table ready")

    config = TABLE_S3_CONFIG["order_lines"]
    prefix = f"{config['s3_prefix']}/account_id_{account_id}"
    df = fetch_from_s3(bucket, prefix, account_id)
    if df.empty:
        logger.warning("[stage_order_lines_only] No parquet files found in S3")
        return 0

    df = df.where(pd.notnull(df), None)
    cleaned_rows = [clean_row(row, TABLE_COLUMNS_MAP["order_lines"]) for row in df.to_dict(orient="records")]
    bulk_insert("mt_order_lines_details_dlk", cleaned_rows, engine, columns=TABLE_COLUMNS_MAP["order_lines"])
    logger.info(f"[stage_order_lines_only] Staged {len(cleaned_rows)} rows")
    return len(cleaned_rows)


def append_order_lines_to_staging(rows: list, engine) -> int:
    """
    Bulk-insert additional order_lines rows into the EXISTING staging table.
    Does NOT drop/recreate — used after stage_order_lines_only() to add
    the batch_missing records fetched by ID.
    """
    if not rows:
        return 0
    cleaned = [clean_row(r, TABLE_COLUMNS_MAP["order_lines"]) for r in rows]
    bulk_insert("mt_order_lines_details_dlk", cleaned, engine, columns=TABLE_COLUMNS_MAP["order_lines"])
    logger.info(f"[append_order_lines_to_staging] Appended {len(cleaned)} rows")
    return len(cleaned)


# ---------------------------------------------------------------------------
# ETL orchestrator
# ---------------------------------------------------------------------------

def etl_all_tables(bucket: str, account_id: str, engine, check_stale: bool = False) -> dict:
    # ID sets local to this run — no globals
    processed_order_ids: set = set()
    ol_credit_ids: set = set()
    res_credit_ids: set = set()
    ol_membership_ids: set = set()
    res_membership_ids: set = set()

    # Create staging tables
    sql_path = os.path.join(os.path.dirname(__file__), "../sql_cmds/create_staging_tables.sql")
    try:
        with open(sql_path) as f:
            create_sql = f.read()
        with engine.begin() as conn:
            conn.execute(text(create_sql))
        logger.info("Staging tables ready")
    except Exception as e:
        logger.error(f"Failed to create staging tables: {e}")
        raise

    successful_tables = []
    failed_tables = []
    etl_start = time.time()

    for table_name, config in TABLE_S3_CONFIG.items():
        logger.info(f"[{table_name}] starting...")
        t0 = time.time()
        try:
            prefix = f"{config['s3_prefix']}/account_id_{account_id}" if account_id else config["s3_prefix"]
            df = fetch_from_s3(bucket, prefix, account_id)
            df = df.where(pd.notnull(df), None)
            cleaned_rows = [clean_row(row, TABLE_COLUMNS_MAP[table_name]) for row in df.to_dict(orient="records")]

            if table_name == "orders":
                processed_order_ids.update(r.get("order_id") for r in cleaned_rows if r.get("order_id"))
                logger.info(f"[orders] collected {len(processed_order_ids)} order_ids")

            if table_name == "order_lines":
                ol_credit_ids.update(r.get("credit_transactions_id") for r in cleaned_rows if r.get("credit_transactions_id"))
                ol_membership_ids.update(r.get("membership_transactions_id") for r in cleaned_rows if r.get("membership_transactions_id"))
                cleaned_rows = validate_order_lines(cleaned_rows, processed_order_ids)

            if table_name == "reservations":
                res_credit_ids.update(r.get("credit_transactions_id") for r in cleaned_rows if r.get("credit_transactions_id"))
                res_membership_ids.update(r.get("membership_transactions_id") for r in cleaned_rows if r.get("membership_transactions_id"))

            if table_name == "credit_transactions":
                cleaned_rows = add_credit_transaction_flags(cleaned_rows, ol_credit_ids, res_credit_ids)
            if table_name == "membership_transactions":
                cleaned_rows = add_membership_transaction_flags(cleaned_rows, ol_membership_ids, res_membership_ids)

            bulk_insert(config["staging_table"], cleaned_rows, engine, columns=TABLE_COLUMNS_MAP[table_name])
            logger.info(f"[{table_name}] done — {len(cleaned_rows)} rows in {round(time.time()-t0,2)}s")
            successful_tables.append(table_name)

        except Exception as e:
            logger.error(f"[{table_name}] failed in {round(time.time()-t0,2)}s: {e}\n{traceback.format_exc()}")
            failed_tables.append(table_name)

    logger.info(f"ETL done: {len(successful_tables)}/{len(TABLE_S3_CONFIG)} tables in {round(time.time()-etl_start,2)}s")
    if failed_tables:
        logger.error(f"Failed tables: {failed_tables}")

    stale_update_success = True
    stale_results = None
    if check_stale:
        logger.info("Running stale data update...")
        stale_results = update_stale_data(engine, account_id, update=True)
        stale_update_success = stale_results.get("success", False)

    return {
        "etl_success": len(failed_tables) == 0,
        "stale_update_success": stale_update_success,
        "failed_tables": failed_tables,
        "stale_results": stale_results,
    }
