
import io
from typing import List, Dict
import pandas as pd
import math
from sqlalchemy import create_engine, text
from core.config import settings
import boto3
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

# Table to columns mapping
TABLE_COLUMNS_MAP = {
    "customers": [
        'id', 'customer_id', 'location_id', 'account_id', 'first_name', 'last_name', 'email', 'full_name', 'birth_date', 'phone_number', 'address_line1', 'address_line2', 'address_line3', 'city', 'country', 'state_province', 'customer_state', 'postal_code', 'gender', 'date_joined', 'is_opted_in_to_sms', 'completed_class_count', 'state_id', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by'
    ],
    "orders": [
        'id', 'order_id', 'date_placed', 'location', 'location_id', 'payment_sources_labels', 'status', 'order_lines_id', 'customer_id', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id', 'customer_ref_id'
    ],
    "order_lines": [
        'id', 'order_line_id', 'order_id', 'transaction_type', 'location', 'credit_transactions_id', 'membership_transactions_id', 'title', 'processed_by', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id', 'credit_transactions_ref_id', 'membership_transactions_ref_id', 'order_ref_id'
    ],
    "class_sessions": [
        'id', 'class_session_id', 'start_datetime', 'start_date', 'location', 'end_datetime', 'cancellation_datetime', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id'
    ],
    "credit_transactions": [
        'id', 'credit_transactions_id', 'transaction_date', 'credit_name', 'is_expired', 'remaining_credits_cache','is_intro_offer', 'parent_credit_transaction_type', 'parent_credit_transaction_id', 'customer_id', 'location', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id', 'customer_ref_id'
    ],
    "membership_transactions": [
        'id', 'membership_transactions_id', 'transaction_date', 'membership_name', 'parent_membership_transaction_id', 'membership_instances_id', 'customer_id', 'location','payment_interval_end_date', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id', 'customer_ref_id', 'membership_instances_ref_id'
    ],
    "membership_instances": [
        'id', 'membership_instances_id', 'purchase_date', 'membership_name', 'renewal_rate_incl_tax', 'status', 'location', 'renewal_count', 'next_charge_date', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id'
    ],
    "reservations": [
        'reservations_id', 'cancel_date', 'check_in_date', 'creation_date', 'status', 'credit_transactions_type', 'credit_transactions_id', 'credit_transactions_ref_id', 'membership_transactions_type', 'membership_transactions_id', 'membership_transactions_ref_id', 'guest', 'customer_id', 'customer_ref_id', 'class_session_id', 'class_session_ref_id', 'account_id', 'first_timer', 'reservation_type', 'location', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by'
    ]
}

# Define datetime columns explicitly - no guessing needed!
DATETIME_COLUMNS = {
    'birth_date', 'date_joined', 'created_at', 'updated_at', 'deleted_at',
    'date_placed', 'start_datetime', 'start_date', 'end_datetime', 
    'cancellation_datetime', 'transaction_date', 'payment_interval_end_date',
    'purchase_date', 'next_charge_date', 'cancel_date', 'check_in_date', 
    'creation_date'
}

TABLE_S3_CONFIG = {
    "customers": {
        "s3_prefix": settings.CUSTOMERS_S3_PREFIX,
        "target_table": "customers",
        "staging_table": "mt_customers_details_dlk"
    },
    "orders": {
        "s3_prefix": settings.ORDERS_S3_PREFIX,
        "target_table": "orders",
        "staging_table": "mt_orders_details_dlk"
    },
    "order_lines": {
        "s3_prefix": settings.ORDER_LINES_S3_PREFIX,
        "target_table": "order_lines",
        "staging_table": "mt_order_lines_details_dlk"
    },
    "class_sessions": {
        "s3_prefix": settings.CLASS_SESSIONS_S3_PREFIX,
        "target_table": "class_sessions",
        "staging_table": "mt_class_sessions_details_dlk"
    },
    "credit_transactions": {
        "s3_prefix": settings.CREDIT_TRANSACTIONS_S3_PREFIX,
        "target_table": "credit_transactions",
        "staging_table": "mt_credit_transactions_details_dlk"
    },
    "membership_transactions": {
        "s3_prefix": settings.MEMBERSHIP_TRANSACTIONS_S3_PREFIX,
        "target_table": "membership_transactions",
        "staging_table": "mt_membership_transactions_details_dlk"
    },
    "membership_instances": {
        "s3_prefix": settings.MEMBERSHIP_INSTANCES_S3_PREFIX,
        "target_table": "membership_instances",
        "staging_table": "mt_membership_instances_details_dlk"
    },
    "reservations": {
        "s3_prefix": settings.RESERVATIONS_S3_PREFIX,
        "target_table": "reservations",
        "staging_table": "mt_reservations_details_dlk"
    },
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bulk_insert_etl")

def bulk_insert(table_name: str, rows: List[Dict], engine, columns=None):
    print(f"Running bulk insert for {table_name}...")
    if columns is None:
        columns = TABLE_COLUMNS_MAP.get(table_name, [])
    if not rows:
        logger.warning(f"No rows to insert for {table_name}.")
        return
    
    def safe_str(val, col_name=""):
        if val is None:
            return '\\N'
        if isinstance(val, float):
            if math.isnan(val):
                return '\\N'
            # Only convert Unix timestamps for known datetime columns
            if col_name in DATETIME_COLUMNS and 1000000000 <= val <= 9999999999999:
                try:
                    dt = pd.to_datetime(val, unit='ms' if val > 1000000000000 else 's')
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            # Convert float to int if it's a whole number
            if val == int(val):
                return str(int(val))
            else:
                return str(val)
        if isinstance(val, int):
            # Only convert Unix timestamps for known datetime columns
            if col_name in DATETIME_COLUMNS and 1000000000 <= val <= 9999999999999:
                try:
                    dt = pd.to_datetime(val, unit='ms' if val > 1000000000000 else 's')
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            return str(val)
        if pd.isna(val):  # Handle pandas NaN
            return '\\N'
        if isinstance(val, str):
            if val.strip().lower() in ['nan', 'none', 'nat']:  # Added 'nat' for pandas NaT
                return '\\N'
            # Handle datetime columns with potential date formatting issues
            if col_name in DATETIME_COLUMNS and val.strip():
                val_clean = val.strip()
                # Handle 2-digit year dates like "66-12-30"
                if len(val_clean) == 8 and val_clean.count('-') == 2:
                    parts = val_clean.split('-')
                    if len(parts[0]) == 2 and parts[0].isdigit():
                        year = int(parts[0])
                        # Convert 2-digit year to 4-digit (assume 1900s for years 00-99)
                        full_year = 1900 + year if year >= 0 else year
                        val = f"{full_year}-{parts[1]}-{parts[2]}"
            # Escape commas, quotes, and newlines for CSV
            val = str(val).replace('"', '""')  # Escape quotes
            if ',' in val or '"' in val or '\n' in val or '\r' in val:
                val = f'"{val}"'  # Quote the field if it contains special chars
            return val
        return str(val)
    
    # Create CSV data directly in memory with better performance
    output = io.StringIO()
    for row in rows:
        csv_row = [safe_str(row.get(col, ''), col) for col in columns]
        output.write(','.join(csv_row) + '\n')
    output.seek(0)
    
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        copy_sql = f"COPY {table_name} ({','.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        # Use the CSV data directly from memory
        cursor.copy_expert(copy_sql, output)
        raw_conn.commit()
        cursor.close()
        logger.info(f"Copied {len(rows)} rows into table {table_name}.")
    except Exception as e:
        logger.error(f"Bulk insert failed for {table_name}: {e}")
        raise
    finally:
        raw_conn.close()

def read_parquet_s3(args):
    bucket, key = args
    
    s3_path = f"s3://{bucket}/{key}"
    logger.info(f"Reading: {s3_path}")
    #import pandas as pd
    #pd.read_parquet(s3_path, engine="pyarrow")
    import awswrangler as wr
    df = wr.s3.read_parquet(s3_path)

    return df

def fetch_from_s3_pandas(bucket: str, prefix: str) -> pd.DataFrame:
    import concurrent.futures
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parquet_keys = [obj["Key"] for obj in objects.get("Contents", []) if obj["Key"].endswith(".parquet")]
    if not parquet_keys:
        logger.warning(f"No Parquet files found in {prefix}")
        return pd.DataFrame()
    dfs = []
    import os
    max_threads = min(8, os.cpu_count() or 1)
    import warnings
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for df in executor.map(read_parquet_s3, [(bucket, key) for key in parquet_keys]):
            # Simple datetime processing - just convert datetime columns to string to preserve Unix timestamps
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    # Convert datetime to string to prevent pandas auto-conversion issues
                    df[col] = df[col].astype(str)
            dfs.append(df)
    # Filter out empty DataFrames before concatenation
    dfs = [df for df in dfs if not df.empty]
    if dfs:
        return pd.concat(dfs, axis=0, ignore_index=True)
    else:
        return pd.DataFrame()

def clean_row(row, expected_columns):
    clean = {}
    for col in expected_columns:
        val = row.get(col, None)
        # Handle various forms of null/NaN values
        if val == "":
            val = None
        elif isinstance(val, str) and val.strip().lower() in ['nan', 'none', 'null', 'nat']:  # Added 'nat'
            val = None
        elif isinstance(val, str) and col in DATETIME_COLUMNS and val.strip():
            # Handle 2-digit year dates like "66-12-30"
            val_clean = val.strip()
            if len(val_clean) == 8 and val_clean.count('-') == 2:
                parts = val_clean.split('-')
                if len(parts[0]) == 2 and parts[0].isdigit():
                    year = int(parts[0])
                    # Convert 2-digit year to 4-digit (assume 1900s for years 00-99)
                    full_year = 1900 + year if year >= 0 else year
                    val = f"{full_year}-{parts[1]}-{parts[2]}"
        elif isinstance(val, float) and math.isnan(val):
            val = None
        elif pd.isna(val):
            val = None
        elif hasattr(val, 'isoformat'):
            val = val.strftime('%Y-%m-%d %H:%M:%S')
        # Handle Unix timestamps only for known datetime columns
        elif col in DATETIME_COLUMNS and isinstance(val, (int, float)) and 1000000000 <= val <= 9999999999999:
            try:
                dt = pd.to_datetime(val, unit='ms' if val > 1000000000000 else 's')
                val = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        clean[col] = val
    # Fill missing columns with None
    for col in expected_columns:
        if col not in clean:
            clean[col] = None
    return clean


def etl_all_tables(bucket: str, account_id: str, engine):
    # Run create_staging_tables.sql before ETL
    sql_path = os.path.join(os.path.dirname(__file__), '../sql_cmds/create_staging_tables.sql')
    try:
        with open(sql_path, 'r') as f:
            create_sql = f.read()
    except FileNotFoundError:
        logger.error(f"Staging SQL file not found: {sql_path}")
        return
    except Exception as e:
        logger.error(f"Error reading staging SQL file: {e}")
        return
    try:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
        logger.info("Staging tables created or verified.")
    except Exception as e:
        # If table already exists, log and continue
        if hasattr(e, 'orig') and hasattr(e.orig, 'pgcode') and e.orig.pgcode == '42P07':
            logger.warning("Staging tables already exist. Skipping creation.")
        elif 'already exists' in str(e):
            logger.warning("Staging tables already exist. Skipping creation.")
        else:
            logger.error(f"Failed to create staging tables: {e}")
            raise
    
    # Track success/failure for each table
    total_tables = len(TABLE_S3_CONFIG)
    successful_tables = []
    failed_tables = []
    
    # Fix iteration over TABLE_S3_CONFIG
    import time
    etl_start_time = time.time()
    for table_name, config in TABLE_S3_CONFIG.items():
        logger.info(f"Starting ETL for {table_name}...")
        table_start_time = time.time()
        try:
            # Use pandas for S3 fetch and cleaning
            s3_prefix = config["s3_prefix"]
            if account_id:
                s3_prefix = f"{s3_prefix}/account_id_{account_id}"
            df = fetch_from_s3_pandas(bucket, s3_prefix)
            
            # Replace NaN values with None before converting to dict
            df = df.where(pd.notnull(df), None)
            
            # Convert to records and clean each row
            cleaned_rows = [clean_row(row, TABLE_COLUMNS_MAP[table_name]) for row in df.to_dict(orient="records")]
            
            # Use staging table for insert, logical table for columns
            bulk_insert(config["staging_table"], cleaned_rows, engine, columns=TABLE_COLUMNS_MAP[table_name])
            logger.info(f"✅ ETL COMPLETED for {table_name}: Successfully inserted {len(cleaned_rows)} rows into {config['staging_table']}")
            successful_tables.append(table_name)
            # If you want to use polars instead, uncomment below:
            # df = fetch_from_s3(bucket, config["s3_prefix"])
            # cleaned_rows = [clean_row(row, TABLE_COLUMNS_MAP[table_name]) for row in df.to_dicts()]
            # bulk_insert(config["staging_table"], cleaned_rows, engine, columns=TABLE_COLUMNS_MAP[table_name])
            # logger.info(f"Inserted {len(cleaned_rows)} rows into {config['staging_table']} (polars)")
        except Exception as e:
            logger.error(f"❌ ETL FAILED for {table_name}: {e}")
            failed_tables.append(table_name)
            print(f"ETL failed for {table_name}: {e}")
            import traceback
            traceback.print_exc()
        table_end_time = time.time()
        logger.info(f"⏱️ Time taken for {table_name}: {table_end_time - table_start_time:.2f} seconds")
    etl_end_time = time.time()
    logger.info(f"⏱️ Total ETL time: {etl_end_time - etl_start_time:.2f} seconds")
    
    # Summary logging
    logger.info(f"ETL SUMMARY: {len(successful_tables)}/{total_tables} tables completed successfully")
    if successful_tables:
        logger.info(f"✅ Successful tables: {', '.join(successful_tables)}")
    if failed_tables:
        logger.error(f"❌ Failed tables: {', '.join(failed_tables)}")
    
    return len(failed_tables) == 0  # Return True if all succeeded
if __name__ == "__main__":
    import sys
    bucket = sys.argv[1] if len(sys.argv) > 1 else getattr(settings, "S3_BUCKET", None)
    if not bucket:
        print("Please provide S3 bucket name as argument or set S3_BUCKET in settings.")
        sys.exit(1)
    from sqlalchemy import create_engine
    engine = create_engine(settings.DATABASE_URL)
    logger.info(f"Starting ETL process for bucket: {bucket}")
    account_id = sys.argv[2] if len(sys.argv) > 2 else getattr(settings, "ACCOUNT_ID", None)
    success = etl_all_tables(bucket, account_id, engine)
    if success:
        logger.info("🎉 ETL process completed successfully - All tables processed!")
        print("ETL process completed successfully.")
    else:
        logger.error("⚠️  ETL process completed with errors - Some tables failed!")
        print("ETL process completed with errors.")
        sys.exit(1)

