
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
        'id', 'customer_id', 'location_id', 'account_id', 'first_name', 'last_name', 'email', 'full_name', 'birth_date', 'birth_month', 'birth_day', 'phone_number', 'address_line1', 'address_line2', 'address_line3', 'city', 'country', 'state_province', 'customer_state', 'postal_code', 'gender', 'date_joined', 'is_opted_in_to_sms', 'completed_class_count', 'state_id', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by'
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
        'id', 'credit_transactions_id', 'transaction_date', 'credit_name', 'is_expired', 'remaining_credits_cache','is_intro_offer', 'parent_credit_transaction_type', 'parent_credit_transaction_id', 'customer_id', 'location', 'isin_order_line', 'isin_reservation', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id', 'customer_ref_id'
    ],
    "membership_transactions": [
        'id', 'membership_transactions_id', 'transaction_date', 'membership_name', 'parent_membership_transaction_id', 'membership_instances_id', 'customer_id', 'location','payment_interval_end_date', 'isin_order_line', 'isin_reservation', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id', 'customer_ref_id', 'membership_instances_ref_id'
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
    # "customers": {
    #     "s3_prefix": settings.CUSTOMERS_S3_PREFIX,
    #     "target_table": "customers",
    #     "staging_table": "mt_customers_details_dlk"
    # },
    # "orders": {
    #     "s3_prefix": settings.ORDERS_S3_PREFIX,
    #     "target_table": "orders",
    #     "staging_table": "mt_orders_details_dlk"
    # },
    # "order_lines": {
    #     "s3_prefix": settings.ORDER_LINES_S3_PREFIX,
    #     "target_table": "order_lines",
    #     "staging_table": "mt_order_lines_details_dlk"
    # },
    # "class_sessions": {
    #     "s3_prefix": settings.CLASS_SESSIONS_S3_PREFIX,
    #     "target_table": "class_sessions",
    #     "staging_table": "mt_class_sessions_details_dlk"
    # },
    "reservations": {
        "s3_prefix": settings.RESERVATIONS_S3_PREFIX,
        "target_table": "reservations",
        "staging_table": "mt_reservations_details_dlk"
    },
    # "membership_instances": {
    #     "s3_prefix": settings.MEMBERSHIP_INSTANCES_S3_PREFIX,
    #     "target_table": "membership_instances",
    #     "staging_table": "mt_membership_instances_details_dlk"
    # },
    # "credit_transactions": {
    #     "s3_prefix": settings.CREDIT_TRANSACTIONS_S3_PREFIX,
    #     "target_table": "credit_transactions",
    #     "staging_table": "mt_credit_transactions_details_dlk"
    # },
    # "membership_transactions": {
    #     "s3_prefix": settings.MEMBERSHIP_TRANSACTIONS_S3_PREFIX,
    #     "target_table": "membership_transactions",
    #     "staging_table": "mt_membership_transactions_details_dlk"
    # },
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bulk_insert_etl")

# Global variables to store IDs processed in the current ETL run
PROCESSED_ORDER_IDS = set()
ORDER_LINE_CREDIT_TRANSACTION_IDS = set()
RESERVATION_CREDIT_TRANSACTION_IDS = set()
ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS = set()
RESERVATION_MEMBERSHIP_TRANSACTION_IDS = set()

def bulk_insert(table_name: str, rows: List[Dict], engine, columns=None):
    print(f"Running bulk insert for {table_name}...")
    if columns is None:
        columns = TABLE_COLUMNS_MAP.get(table_name, [])
    if not rows:
        logger.warning(f"No rows to insert for {table_name}.")
        return 0
    
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
    num_rows_copied = 0
    try:
        cursor = raw_conn.cursor()
        copy_sql = f"COPY {table_name} ({','.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        # Use the CSV data directly from memory
        cursor.copy_expert(copy_sql, output)
        num_rows_copied = len(rows)
        raw_conn.commit()
        cursor.close()
        logger.info(f"Copied {num_rows_copied} rows into table {table_name}.")
    except Exception as e:
        logger.error(f"Bulk insert failed for {table_name}: {e}")
        raise
    finally:
        raw_conn.close()
    return num_rows_copied

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
        # Handle numpy arrays and lists (especially for child_orders field)
        if hasattr(val, '__iter__') and not isinstance(val, str):
            # Convert arrays/lists to JSON string format
            if hasattr(val, 'tolist'):  # numpy array
                val = str(val.tolist()) if val.size > 0 else None
            elif isinstance(val, (list, tuple)):
                val = str(list(val)) if val else None
            else:
                val = str(val) if val is not None else None
        # Handle various forms of null/NaN values
        elif val == "":
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


def validate_order_lines_with_child_orders(cleaned_rows: List[Dict]) -> List[Dict]:
    """
    Validate order_lines based on child_orders logic using processed order IDs:
    - If child_orders is empty -> Valid (keep)
    - If child_orders has values and ANY exist in processed orders -> Invalid (remove)
    - If child_orders has values and NONE exist in processed orders -> Valid (keep)
    """
    global PROCESSED_ORDER_IDS
    
    if not cleaned_rows:
        return []
    
    logger.info(f"Starting validation of {len(cleaned_rows)} order_lines entries using {len(PROCESSED_ORDER_IDS)} processed order IDs...")
    
    # Convert processed order IDs to strings for comparison (handles different data types)
    existing_order_ids = {str(oid) for oid in PROCESSED_ORDER_IDS if oid}
    
    # Filter rows based on validation logic
    valid_rows = []
    invalid_count = 0
    
    for row in cleaned_rows:
        child_orders = row.get('child_orders')
        is_valid = True
        
        if child_orders:
            # Parse child_orders for this specific row
            row_child_order_ids = set()
            if isinstance(child_orders, str):
                # Try to parse as JSON list first, then fall back to comma-separated
                try:
                    import json
                    child_order_list = json.loads(child_orders)
                    if isinstance(child_order_list, list):
                        row_child_order_ids.update(str(cid).strip() for cid in child_order_list if cid)
                    else:
                        # Single string value
                        if child_orders.strip():
                            row_child_order_ids.add(str(child_orders).strip())
                except (json.JSONDecodeError, ValueError):
                    # Handle comma-separated string
                    if ',' in child_orders:
                        row_child_order_ids.update(cid.strip() for cid in child_orders.split(',') if cid.strip())
                    else:
                        if child_orders.strip():
                            row_child_order_ids.add(str(child_orders).strip())
            elif isinstance(child_orders, list):
                row_child_order_ids.update(str(cid).strip() for cid in child_orders if cid)
            
            # Check if ANY child order ID exists in processed orders -> Invalid
            if row_child_order_ids:
                matching_order_ids = [oid for oid in row_child_order_ids if oid in existing_order_ids]
                if matching_order_ids:
                    is_valid = False
                    invalid_count += 1
                    # Print detailed information about the invalid entry
                    order_line_id = row.get('order_line_id', 'N/A')
                    print(f"❌ INVALID order_line_id: {order_line_id}")
                    print(f"   └── Child orders that exist in orders table: {matching_order_ids}")
                    print(f"   └── All child orders for this line: {list(row_child_order_ids)}")
                    logger.warning(f"Invalid order_line_id {order_line_id} with existing child orders: {matching_order_ids}")
        
        if is_valid:
            valid_rows.append(row)
    
    print(f"Order lines validation completed: {len(valid_rows)} valid entries, {invalid_count} invalid entries removed")
    if invalid_count > 0:
        print(f"📊 TOTAL INVALID ORDER LINES: {invalid_count}")
        logger.warning(f"Total invalid order lines filtered out: {invalid_count}")
    return valid_rows


def add_credit_transaction_flags(cleaned_rows: List[Dict]) -> List[Dict]:
    """
    Add isin_order_line and isin_reservation boolean flags to credit_transactions:
    - isin_order_line: True if credit_transactions_id exists in order_lines table
    - isin_reservation: True if credit_transactions_id exists in reservations table
    """
    global ORDER_LINE_CREDIT_TRANSACTION_IDS, RESERVATION_CREDIT_TRANSACTION_IDS
    
    if not cleaned_rows:
        return []
    
    logger.info(f"Adding credit transaction flags to {len(cleaned_rows)} credit_transactions entries...")
    logger.info(f"Using {len(ORDER_LINE_CREDIT_TRANSACTION_IDS)} order_line credit IDs and {len(RESERVATION_CREDIT_TRANSACTION_IDS)} reservation credit IDs")
    
    # Keep as integers for comparison
    order_line_credit_ids = {cid for cid in ORDER_LINE_CREDIT_TRANSACTION_IDS if cid is not None}
    reservation_credit_ids = {cid for cid in RESERVATION_CREDIT_TRANSACTION_IDS if cid is not None}
    
    updated_rows = []
    order_line_matches = 0
    reservation_matches = 0

    print(f"Total credit transactions to process: {len(cleaned_rows)}")
    
    for row in cleaned_rows:
        credit_tx_id = row.get('credit_transactions_id')
        
        # Set boolean flags (keep integer comparison)
        row['isin_order_line'] = credit_tx_id in order_line_credit_ids if credit_tx_id is not None else False
        row['isin_reservation'] = credit_tx_id in reservation_credit_ids if credit_tx_id is not None else False
        
        # Count matches for logging
        if row['isin_order_line']:
            order_line_matches += 1
        if row['isin_reservation']:
            reservation_matches += 1
        
        updated_rows.append(row)
    
    print(f"Credit transaction flags added: {order_line_matches} matched order_lines, {reservation_matches} matched reservations")
    print(f"Percentage of credit transactions linked to order_lines: {round((order_line_matches / len(cleaned_rows) * 100), 2) if len(cleaned_rows) > 0 else 0.0}%")
    print(f"Percentage of credit transactions linked to reservations: {round((reservation_matches / len(cleaned_rows) * 100), 2) if len(cleaned_rows) > 0 else 0.0}%")
    return updated_rows


def add_membership_transaction_flags(cleaned_rows: List[Dict]) -> List[Dict]:
    """
    Add isin_order_line and isin_reservation boolean flags to membership_transactions:
    - isin_order_line: True if membership_transactions_id exists in order_lines table
    - isin_reservation: True if membership_transactions_id exists in reservations table
    """
    global ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS, RESERVATION_MEMBERSHIP_TRANSACTION_IDS
    
    if not cleaned_rows:
        return []
    
    logger.info(f"Adding membership transaction flags to {len(cleaned_rows)} membership_transactions entries...")
    logger.info(f"Using {len(ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS)} order_line membership IDs and {len(RESERVATION_MEMBERSHIP_TRANSACTION_IDS)} reservation membership IDs")
    
    # Keep as integers for comparison
    order_line_membership_ids = {mid for mid in ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS if mid is not None}
    reservation_membership_ids = {mid for mid in RESERVATION_MEMBERSHIP_TRANSACTION_IDS if mid is not None}
    
    updated_rows = []
    order_line_matches = 0
    reservation_matches = 0

    print(f"Total membership transactions to process: {len(cleaned_rows)}")
    
    for row in cleaned_rows:
        membership_tx_id = row.get('membership_transactions_id')
        
        # Set boolean flags (keep integer comparison)
        row['isin_order_line'] = membership_tx_id in order_line_membership_ids if membership_tx_id is not None else False
        row['isin_reservation'] = membership_tx_id in reservation_membership_ids if membership_tx_id is not None else False
        
        # Count matches for logging
        if row['isin_order_line']:
            order_line_matches += 1
        if row['isin_reservation']:
            reservation_matches += 1
        
        updated_rows.append(row)

    print(f"Membership transaction flags added: {order_line_matches} matched order_lines, {reservation_matches} matched reservations")
    print(f"Percentage of Membership transactions linked to order_lines: {round((order_line_matches / len(cleaned_rows) * 100), 2) if len(cleaned_rows) > 0 else 0.0}%")
    print(f"Percentage of Membership transactions linked to reservations: {round((reservation_matches / len(cleaned_rows) * 100), 2) if len(cleaned_rows) > 0 else 0.0}%")
    return updated_rows


def update_membership_instances_status(account_id, engine, update=False):
    """
    Syncs membership_instances.status with mt_membership_instances_details_dlk.status for a given account_id.
    Logs and returns counts of mismatches and updates.
    """
    select_count_query = """
        SELECT COUNT(*)
        FROM public.mt_membership_instances_details_dlk s
        INNER JOIN public.membership_instances o
            ON s.membership_instances_id = o.membership_instances_id
            AND s.account_id = o.account_id
        WHERE s.status IS DISTINCT FROM o.status
            AND s.account_id = :account_id;
    """

    update_query = """
        UPDATE public.membership_instances o
        SET status = s.status
        FROM public.mt_membership_instances_details_dlk s
        WHERE o.membership_instances_id = s.membership_instances_id
            AND o.account_id = s.account_id
            AND o.status IS DISTINCT FROM s.status
            AND o.account_id = :account_id
    """

    with engine.connect() as conn:
        count_result = conn.execute(text(select_count_query), {"account_id": account_id})
        mismatch_count = count_result.scalar() or 0
        logger.info(f"[update_membership_instances_status] Found {mismatch_count} mismatched membership_instances for account_id={account_id}")

        updated_count = 0
        if update:
            if mismatch_count > 0:
                update_result = conn.execute(text(update_query), {"account_id": account_id})
                updated_count = update_result.rowcount
                logger.info(f"[update_membership_instances_status] Updated {updated_count} membership_instances for account_id={account_id}")
            else:
                logger.info(f"[update_membership_instances_status] No updates needed for account_id={account_id}")

    return {
        "account_id": account_id,
        "mismatched_count": mismatch_count,
        "updated_count": updated_count
    }


def calculate_first_timer_field_with_pandas(account_id: str, location_id: int, engine):
    """
    Step 1: Calculate and update first_timer field in staging table using pandas (CORRECTED LOGIC)
    """
    logger.info(f"[STEP 1] Calculating first_timer field using pandas approach")
    
    try:
        with engine.begin() as conn:
            # Load staging reservations data
            reservations_df = pd.read_sql_query(text("""
                SELECT reservations_id, customer_id, class_session_id, status, account_id, location
                FROM mt_reservations_details_dlk 
                WHERE account_id = :account_id
                  AND location = :location_id
            """), conn, params={"account_id": account_id, "location_id": str(location_id)})
            
            logger.info(f"[STEP 1] Loaded {len(reservations_df)} reservations from staging")
            
            # Get valid customer IDs
            valid_customers_df = pd.read_sql_query(text("""
                SELECT DISTINCT customer_id
                FROM customers 
                WHERE account_id = :account_id
                  AND location_id = :location_id
            """), conn, params={"account_id": account_id, "location_id": location_id})
            
            # Get class session start times
            class_sessions_df = pd.read_sql_query(text("""
                SELECT class_session_id, start_datetime
                FROM class_sessions 
                WHERE account_id = :account_id
                  AND location = :location_id
            """), conn, params={"account_id": account_id, "location_id": str(location_id)})
            
            # Filter reservations to valid customers only
            filtered_reservations = reservations_df[
                reservations_df['customer_id'].isin(valid_customers_df['customer_id'])
            ].copy()
            
            # Add class session start times
            filtered_reservations = filtered_reservations.merge(
                class_sessions_df, 
                on='class_session_id', 
                how='inner'
            )
            
            if len(filtered_reservations) == 0:
                logger.info(f"[STEP 1] No valid reservations found")
                return {"first_timer_updated_count": 0}
            
            # Calculate reservation counts per customer (like reservation_counts CTE)
            reservation_counts = filtered_reservations.groupby('customer_id').size().reset_index(name='total_reservations')
            
            # Find first eligible reservations - ONLY for ('check in', 'pending') status
            eligible_reservations = filtered_reservations[
                filtered_reservations['status'].isin(['check in', 'pending'])
            ].copy()
            
            if len(eligible_reservations) > 0:
                # Sort by customer and start_datetime to find first reservation
                eligible_reservations = eligible_reservations.sort_values(['customer_id', 'start_datetime','reservations_id'])
                eligible_reservations['rn'] = eligible_reservations.groupby('customer_id').cumcount() + 1
                
                # Get first eligible reservation per customer (rn = 1)
                first_eligible = eligible_reservations[eligible_reservations['rn'] == 1][['reservations_id', 'customer_id']]
            else:
                first_eligible = pd.DataFrame(columns=['reservations_id', 'customer_id'])
            
            # Apply exact SQL logic
            result_df = filtered_reservations.merge(reservation_counts, on='customer_id', how='left')
            result_df = result_df.merge(first_eligible, on=['reservations_id', 'customer_id'], how='left', indicator='is_first_eligible')
            
            # CORRECTED: Match exact SQL logic
            first_timer_conditions = (
                # Single reservation customers (no status filter - matches SQL)
                (result_df['total_reservations'] == 1) |
                # Multi-reservation customers where this is their first eligible reservation
                ((result_df['total_reservations'] > 1) & 
                 (result_df['is_first_eligible'] == 'both'))
            )
            
            first_timer_ids = result_df[first_timer_conditions]['reservations_id'].tolist()
            
            logger.info(f"[STEP 1] Identified {len(first_timer_ids)} first-timer reservations")
            
            # Update database with first timers
            first_timer_count = 0
            if first_timer_ids:
                batch_size = 1000
                for i in range(0, len(first_timer_ids), batch_size):
                    batch_ids = first_timer_ids[i:i + batch_size]
                    placeholders = ','.join([':id' + str(j) for j in range(len(batch_ids))])
                    params = {f'id{j}': batch_ids[j] for j in range(len(batch_ids))}
                    params.update({"account_id": account_id, "location_id": str(location_id)})
                    
                    batch_result = conn.execute(text(f"""
                        UPDATE mt_reservations_details_dlk 
                        SET first_timer = TRUE 
                        WHERE reservations_id IN ({placeholders})
                          AND account_id = :account_id 
                          AND location = :location_id
                    """), params)
                    
                    first_timer_count += batch_result.rowcount
            
        logger.info(f"[STEP 1] SUCCESS: Updated first_timer field for {first_timer_count} reservations")
        return {"first_timer_updated_count": first_timer_count}
        
    except Exception as e:
        logger.error(f"[STEP 1] ERROR: Failed to calculate first_timer field: {e}")
        raise

def update_reservations_details(account_id, location_id, engine, update=False):
    """
    Syncs reservations fields (status, cancel_date, check_in_date) with mt_reservations_details_dlk for a given account_id.
    Logs and returns counts of mismatches and updates for each field.
    """
    import time
    overall_start_time = time.time()
    results = {}
    
    # 1. Status field sync
    status_start_time = time.time()
    status_count_query = """
        SELECT COUNT(*) as total_mismatched_records
        FROM reservations r
        INNER JOIN public.mt_reservations_details_dlk d
            ON d.reservations_id = r.reservations_id
            AND d.account_id = r.account_id
        WHERE r.account_id = :account_id
          AND r.updated_at < d.updated_at
          AND (
              r.status != d.status 
              OR (r.status IS NULL AND d.status IS NOT NULL)
              OR (r.status IS NOT NULL AND d.status IS NULL)
          );
    """
    
    status_update_query = """
        UPDATE reservations r
        SET status = d.status
        FROM public.mt_reservations_details_dlk d
        WHERE r.reservations_id = d.reservations_id
          AND r.account_id = :account_id
          AND d.account_id = :account_id
          AND r.updated_at < d.updated_at
          AND (
              r.status != d.status 
              OR (r.status IS NULL AND d.status IS NOT NULL)
              OR (r.status IS NOT NULL AND d.status IS NULL)
          );
    """
    
    # Status field sync - Staging table update (reverse direction)
    status_count_stg_query = """
        SELECT COUNT(*) as total_mismatched_records
        FROM public.mt_reservations_details_dlk d
        INNER JOIN reservations r
            ON d.reservations_id = r.reservations_id
            AND d.account_id = r.account_id
        WHERE r.account_id = :account_id
          AND d.updated_at < r.updated_at
          AND (
              d.status != r.status 
              OR (d.status IS NULL AND r.status IS NOT NULL)
              OR (d.status IS NOT NULL AND r.status IS NULL)
          );
    """
    
    status_update_stg_query = """
        UPDATE public.mt_reservations_details_dlk d
        SET status = r.status
        FROM reservations r
        WHERE d.reservations_id = r.reservations_id
          AND d.account_id = :account_id
          AND r.account_id = :account_id
          AND d.updated_at < r.updated_at
          AND (
              d.status != r.status 
              OR (d.status IS NULL AND r.status IS NOT NULL)
              OR (d.status IS NOT NULL AND r.status IS NULL)
          );
    """
    
    # 2. Cancel_date field sync
    cancel_date_count_query = """
        SELECT COUNT(*) as total_mismatched_records
        FROM reservations r
        INNER JOIN public.mt_reservations_details_dlk d
            ON d.reservations_id = r.reservations_id
            AND d.account_id = r.account_id
        WHERE r.account_id = :account_id
          AND r.updated_at < d.updated_at
          AND (
              r.cancel_date != d.cancel_date 
              OR (r.cancel_date IS NULL AND d.cancel_date IS NOT NULL)
              OR (r.cancel_date IS NOT NULL AND d.cancel_date IS NULL)
          );
    """
    
    cancel_date_update_query = """
        UPDATE reservations r
        SET cancel_date = d.cancel_date
        FROM public.mt_reservations_details_dlk d
        WHERE r.reservations_id = d.reservations_id
          AND r.account_id = :account_id
          AND d.account_id = :account_id
          AND r.updated_at < d.updated_at
          AND (
              r.cancel_date != d.cancel_date 
              OR (r.cancel_date IS NULL AND d.cancel_date IS NOT NULL)
              OR (r.cancel_date IS NOT NULL AND d.cancel_date IS NULL)
          );
    """
    
    # 3. Check_in_date field sync
    check_in_date_count_query = """
        SELECT COUNT(*) as total_mismatched_records
        FROM reservations r
        INNER JOIN public.mt_reservations_details_dlk d
            ON d.reservations_id = r.reservations_id
            AND d.account_id = r.account_id
        WHERE r.account_id = :account_id
          AND r.updated_at < d.updated_at
          AND (
              r.check_in_date != d.check_in_date 
              OR (r.check_in_date IS NULL AND d.check_in_date IS NOT NULL)
              OR (r.check_in_date IS NOT NULL AND d.check_in_date IS NULL)
          );
    """
    
    check_in_date_update_query = """
        UPDATE reservations r
        SET check_in_date = d.check_in_date
        FROM public.mt_reservations_details_dlk d
        WHERE r.reservations_id = d.reservations_id
          AND r.account_id = :account_id
          AND d.account_id = :account_id
          AND r.updated_at < d.updated_at
          AND (
              r.check_in_date != d.check_in_date 
              OR (r.check_in_date IS NULL AND d.check_in_date IS NOT NULL)
              OR (r.check_in_date IS NOT NULL AND d.check_in_date IS NULL)
          );
    """
    
    with engine.begin() as conn:
        # Process status field
        status_count_result = conn.execute(text(status_count_query), {"account_id": account_id})
        status_mismatch_count = status_count_result.scalar() or 0
        logger.info(f"[update_reservations_details] Found {status_mismatch_count} mismatched status records for account_id={account_id}")
        
        status_updated_count = 0
        if update:
            if status_mismatch_count > 0:
                status_update_result = conn.execute(text(status_update_query), {"account_id": account_id})
                status_updated_count = status_update_result.rowcount
                logger.info(f"[update_reservations_details] Updated {status_updated_count} status records for account_id={account_id}")
            else:
                logger.info(f"[update_reservations_details] No status updates needed for account_id={account_id}")
        
        if status_mismatch_count > 0 and not update:
            logger.warning(f"⚠️ {status_mismatch_count} mismatched status records found but update=False")
        
        # Process status field - Staging table update (reverse direction)
        status_stg_count_result = conn.execute(text(status_count_stg_query), {"account_id": account_id})
        status_stg_mismatch_count = status_stg_count_result.scalar() or 0
        logger.info(f"[update_reservations_details] Found {status_stg_mismatch_count} mismatched status records in staging for account_id={account_id}")
        
        status_stg_updated_count = 0
        if update:
            if status_stg_mismatch_count > 0:
                status_stg_update_result = conn.execute(text(status_update_stg_query), {"account_id": account_id})
                status_stg_updated_count = status_stg_update_result.rowcount
                logger.info(f"[update_reservations_details] Updated {status_stg_updated_count} status records in staging for account_id={account_id}")
            else:
                logger.info(f"[update_reservations_details] No staging status updates needed for account_id={account_id}")
        
        if status_stg_mismatch_count > 0 and not update:
            logger.warning(f"⚠️ {status_stg_mismatch_count} mismatched staging status records found but update=False")
        
        status_end_time = time.time()
        status_elapsed = status_end_time - status_start_time
        logger.info(f"⏱️ Status field sync completed in {status_elapsed:.2f} seconds")
        
        # Process cancel_date field
        cancel_date_start_time = time.time()
        cancel_date_count_result = conn.execute(text(cancel_date_count_query), {"account_id": account_id})
        cancel_date_mismatch_count = cancel_date_count_result.scalar() or 0
        logger.info(f"[update_reservations_details] Found {cancel_date_mismatch_count} mismatched cancel_date records for account_id={account_id}")
        
        cancel_date_updated_count = 0
        if update:
            if cancel_date_mismatch_count > 0:
                cancel_date_update_result = conn.execute(text(cancel_date_update_query), {"account_id": account_id})
                cancel_date_updated_count = cancel_date_update_result.rowcount
                logger.info(f"[update_reservations_details] Updated {cancel_date_updated_count} cancel_date records for account_id={account_id}")
            else:
                logger.info(f"[update_reservations_details] No cancel_date updates needed for account_id={account_id}")
        
        if cancel_date_mismatch_count > 0 and not update:
            logger.warning(f"⚠️ {cancel_date_mismatch_count} mismatched cancel_date records found but update=False")
        
        cancel_date_end_time = time.time()
        cancel_date_elapsed = cancel_date_end_time - cancel_date_start_time
        logger.info(f"⏱️ Cancel_date field sync completed in {cancel_date_elapsed:.2f} seconds")
        
        # Process check_in_date field
        check_in_date_start_time = time.time()
        check_in_date_count_result = conn.execute(text(check_in_date_count_query), {"account_id": account_id})
        check_in_date_mismatch_count = check_in_date_count_result.scalar() or 0
        logger.info(f"[update_reservations_details] Found {check_in_date_mismatch_count} mismatched check_in_date records for account_id={account_id}")
        
        check_in_date_updated_count = 0
        if update:
            if check_in_date_mismatch_count > 0:
                check_in_date_update_result = conn.execute(text(check_in_date_update_query), {"account_id": account_id})
                check_in_date_updated_count = check_in_date_update_result.rowcount
                logger.info(f"[update_reservations_details] Updated {check_in_date_updated_count} check_in_date records for account_id={account_id}")
            else:
                logger.info(f"[update_reservations_details] No check_in_date updates needed for account_id={account_id}")
        
        if check_in_date_mismatch_count > 0 and not update:
            logger.warning(f"⚠️ {check_in_date_mismatch_count} mismatched check_in_date records found but update=False")

        
        
        check_in_date_end_time = time.time()
        check_in_date_elapsed = check_in_date_end_time - check_in_date_start_time
        logger.info(f"⏱️ Check_in_date field sync completed in {check_in_date_elapsed:.2f} seconds")

    # Calculate first_timer field
    first_timer_start_time = time.time()
    first_timers = calculate_first_timer_field_with_pandas(account_id, location_id, engine)
    first_timer_end_time = time.time()
    first_timer_elapsed = first_timer_end_time - first_timer_start_time
    logger.info(f"⏱️ First_timer calculation completed in {first_timer_elapsed:.2f} seconds")

    overall_end_time = time.time()
    overall_elapsed = overall_end_time - overall_start_time
    logger.info(f"⏱️ TOTAL update_reservations_details completed in {overall_elapsed:.2f} seconds")
    logger.info(f"[update_reservations_details] Completed updates for account_id={account_id}") 
    
    return {
        "account_id": account_id,
        "status": {
            "mismatched_count": status_mismatch_count,
            "updated_count": status_updated_count,
            "time_taken_seconds": round(status_elapsed, 2)
        },
        "status_staging": {
            "mismatched_count": status_stg_mismatch_count,
            "updated_count": status_stg_updated_count,
            "time_taken_seconds": round(status_elapsed, 2)
        },
        "cancel_date": {
            "mismatched_count": cancel_date_mismatch_count,
            "updated_count": cancel_date_updated_count,
            "time_taken_seconds": round(cancel_date_elapsed, 2)
        },
        "check_in_date": {
            "mismatched_count": check_in_date_mismatch_count,
            "updated_count": check_in_date_updated_count,
            "time_taken_seconds": round(check_in_date_elapsed, 2)
        },
        "first_timer_in_staging": {
            **first_timers,
            "time_taken_seconds": round(first_timer_elapsed, 2)
        },
        "total_time_taken_seconds": round(overall_elapsed, 2)
    }


def etl_all_tables(bucket: str, account_id: str, location_id: str, engine):
    # Initialize global ID sets for this ETL run
    global PROCESSED_ORDER_IDS, ORDER_LINE_CREDIT_TRANSACTION_IDS, RESERVATION_CREDIT_TRANSACTION_IDS, ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS, RESERVATION_MEMBERSHIP_TRANSACTION_IDS
    PROCESSED_ORDER_IDS.clear()
    ORDER_LINE_CREDIT_TRANSACTION_IDS.clear()
    RESERVATION_CREDIT_TRANSACTION_IDS.clear()
    ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS.clear()
    RESERVATION_MEMBERSHIP_TRANSACTION_IDS.clear()
    logger.info("🔄 Initialized global ID storage for ETL run")
    
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
    table_row_counts = {}
    
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

            # Collect order IDs when processing orders table for later validation
            if table_name == "orders":
                order_ids_in_batch = {row.get('order_id') for row in cleaned_rows if row.get('order_id')}
                PROCESSED_ORDER_IDS.update(order_ids_in_batch)
                logger.info(f"📝 Collected {len(order_ids_in_batch)} order IDs from orders table (Total: {len(PROCESSED_ORDER_IDS)})")

            # Collect credit transaction IDs from order_lines table
            if table_name == "order_lines":
                credit_tx_ids_in_batch = {row.get('credit_transactions_id') for row in cleaned_rows if row.get('credit_transactions_id')}
                membership_tx_ids_in_batch = {row.get('membership_transactions_id') for row in cleaned_rows if row.get('membership_transactions_id')}
                ORDER_LINE_CREDIT_TRANSACTION_IDS.update(credit_tx_ids_in_batch)
                ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS.update(membership_tx_ids_in_batch)
                print(f"📝 Collected {len(credit_tx_ids_in_batch)} credit transaction IDs from order_lines table (Total: {len(ORDER_LINE_CREDIT_TRANSACTION_IDS)})")
                print(f"📝 Collected {len(membership_tx_ids_in_batch)} membership transaction IDs from order_lines table (Total: {len(ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS)})")

            # Collect credit and membership transaction IDs from reservations table
            # if table_name == "reservations":
            #     credit_tx_ids_in_batch = {row.get('credit_transactions_id') for row in cleaned_rows if row.get('credit_transactions_id')}
            #     membership_tx_ids_in_batch = {row.get('membership_transactions_id') for row in cleaned_rows if row.get('membership_transactions_id')}
            #     RESERVATION_CREDIT_TRANSACTION_IDS.update(credit_tx_ids_in_batch)
            #     RESERVATION_MEMBERSHIP_TRANSACTION_IDS.update(membership_tx_ids_in_batch)
            #     print(f"📝 Collected {len(credit_tx_ids_in_batch)} credit transaction IDs from reservations table (Total: {len(RESERVATION_CREDIT_TRANSACTION_IDS)})")
            #     print(f"📝 Collected {len(membership_tx_ids_in_batch)} membership transaction IDs from reservations table (Total: {len(RESERVATION_MEMBERSHIP_TRANSACTION_IDS)})")

            # Special validation for order_lines table based on child_orders
            if table_name == "order_lines":
                original_count = len(cleaned_rows)
                cleaned_rows = validate_order_lines_with_child_orders(cleaned_rows)
                filtered_count = original_count - len(cleaned_rows)
                if filtered_count > 0:
                    logger.info(f"🔍 order_lines validation: Filtered out {filtered_count} invalid entries with existing child orders")

            # Add boolean flags for credit_transactions table
            if table_name == "credit_transactions":
                cleaned_rows = add_credit_transaction_flags(cleaned_rows)

            # Add boolean flags for membership_transactions table
            if table_name == "membership_transactions":
                cleaned_rows = add_membership_transaction_flags(cleaned_rows)

            # Use staging table for insert, logical table for columns
            num_rows_copied = bulk_insert(config["staging_table"], cleaned_rows, engine, columns=TABLE_COLUMNS_MAP[table_name])
            table_row_counts[table_name] = num_rows_copied
            logger.info(f"✅ ETL COMPLETED for {table_name}: Successfully inserted {num_rows_copied} rows into {config['staging_table']}")
            # If reservations, run status sync and log/append result
            if table_name == "reservations":
                sync_result = update_reservations_details(account_id, location_id, engine, update=True)
                logger.info(f"🔄 Reservations update details: {sync_result}")
                successful_tables.append(f"{table_name} (status sync: {sync_result})")
            else:
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
        print(f"✅ Successful tables: {', '.join(successful_tables)}")
        logger.info(f"✅ Successful tables: {', '.join(successful_tables)}")
    if failed_tables:
        print(f"❌ Failed tables: {', '.join(failed_tables)}")
        logger.error(f"❌ Failed tables: {', '.join(failed_tables)}")

    # Return both success status and table row counts
    return {
        "success": len(failed_tables) == 0,
        "table_row_counts": table_row_counts,
        "successful_tables": successful_tables,
        "failed_tables": failed_tables
    }
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

