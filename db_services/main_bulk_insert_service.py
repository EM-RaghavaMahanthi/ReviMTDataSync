
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
from db_services.update_stale import update_stale_data

logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

# Table to columns mapping
TABLE_COLUMNS_MAP = {
    "customers": [
        'id', 'customer_id', 'location_id', 'account_id', 'first_name', 'last_name', 'email', 'full_name', 'birth_date', 'birth_month', 'birth_day', 'phone_number', 'address_line1', 'address_line2', 'address_line3', 'city', 'country', 'state_province', 'customer_state', 'postal_code', 'gender', 'date_joined', 'is_opted_in_to_sms', 'completed_class_count', 'state_id', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by'
    ],
    "class_sessions": [
        'id', 'class_session_id', 'start_datetime', 'start_date', 'location', 'end_datetime', 'cancellation_datetime', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id'
    ],
    "orders": [
        'id', 'order_id', 'date_placed', 'location', 'location_id', 'payment_sources_labels', 'status', 'order_lines_id', 'parent_order', 'customer_id', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id', 'customer_ref_id'
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
    "order_lines": [
        'id', 'order_line_id', 'order_id', 'transaction_type', 'location', 'credit_transactions_id', 'membership_transactions_id', 'title', 'processed_by', 'child_orders', 'is_valid', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'account_id', 'credit_transactions_ref_id', 'membership_transactions_ref_id', 'order_ref_id'
    ],
    "reservations": [
        'id', 'reservations_id', 'cancel_date', 'check_in_date', 'creation_date', 'status', 'credit_transactions_type', 'credit_transactions_id', 'credit_transactions_ref_id', 'membership_transactions_type', 'membership_transactions_id', 'membership_transactions_ref_id', 'guest', 'customer_id', 'customer_ref_id', 'class_session_id', 'class_session_ref_id', 'account_id', 'first_timer', 'reservation_type', 'location', 'created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'transaction_type'
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
    "reservations": {
        "s3_prefix": settings.RESERVATIONS_S3_PREFIX,
        "target_table": "reservations",
        "staging_table": "mt_reservations_details_dlk"
    },
    "membership_instances": {
        "s3_prefix": settings.MEMBERSHIP_INSTANCES_S3_PREFIX,
        "target_table": "membership_instances",
        "staging_table": "mt_membership_instances_details_dlk"
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
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bulk_insert_etl")

# Global variables to store IDs processed in the current ETL run
PROCESSED_ORDER_IDS = set()
CHILD_ORDER_IDS = set()  # Track orders that have a parent_order (child orders)
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

def fetch_from_s3_pandas(bucket: str, prefix: str, account_id: str = None) -> pd.DataFrame:
    import concurrent.futures
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parquet_keys = [obj["Key"] for obj in objects.get("Contents", []) if obj["Key"].endswith(".parquet")]
    
    # Filter to exact account_id match if account_id is provided
    if account_id and parquet_keys:
        # Ensure we only match the exact account_id folder, not substring matches
        # e.g., account_id_81/ should not match account_id_816/ or account_id_817/
        account_prefix = f"account_id_{account_id}/"
        parquet_keys = [key for key in parquet_keys if account_prefix in key]
        logger.info(f"Filtered to {len(parquet_keys)} files for exact account_id {account_id}")
    
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
                val = str(val.tolist()) if val.size > 0 else '[]'
            elif isinstance(val, (list, tuple)):
                # Always convert to string, even if empty (important for child_orders field)
                val = str(list(val))
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
    Validate order_lines using TWO validation methods and set is_valid flag:
    
    Method 1: Check child_orders field in order_lines
    - If child_orders has values and ANY exist in PROCESSED_ORDER_IDS -> Invalid (is_valid=False)
      Example: Order Line 19144 has child_orders=['18861'] and 18861 exists → INVALID (deferred payment placeholder)
    
    Method 2: Check if order_line belongs to a PARENT order (not child order)
    - If order_id is NOT in CHILD_ORDER_IDS (meaning it's a parent order) AND has child_orders → Invalid
      Example: Order Line 19145 belongs to Order 18861 (child order) → VALID (actual transaction)
    
    Logic: We want to KEEP order_lines from child orders (actual transactions) 
           and FILTER order_lines from parent orders that have children (deferred placeholders)
    
    Sets is_valid flag (True/False) for ALL rows for audit purposes.
    Returns ALL rows with is_valid flag set - NO FILTERING!
    """
    global PROCESSED_ORDER_IDS, CHILD_ORDER_IDS
    
    if not cleaned_rows:
        return []
    
    logger.info(f"Starting dual validation of {len(cleaned_rows)} order_lines entries...")
    logger.info(f"Using {len(PROCESSED_ORDER_IDS)} processed order IDs and {len(CHILD_ORDER_IDS)} child order IDs")
    
    # Convert to strings for comparison
    existing_order_ids = {str(oid) for oid in PROCESSED_ORDER_IDS if oid}
    child_order_ids_str = {str(oid) for oid in CHILD_ORDER_IDS if oid}
    
    valid_count = 0
    invalid_count = 0
    invalid_by_child_orders = 0
    invalid_by_parent_order = 0
    invalid_order_lines_list = []  # Collect all invalid order_lines for detailed logging
    
    for row in cleaned_rows:
        is_valid = True
        invalid_reason = []
        
        order_id = str(row.get('order_id')) if row.get('order_id') else None
        order_line_id = row.get('order_line_id', 'N/A')
        
        # Validation Method 1: Check child_orders field
        # If this order_line has child_orders that exist, it's a deferred payment placeholder → INVALID
        child_orders = row.get('child_orders')
        
        logger.debug(f"[DEBUG] order_line_id={order_line_id}, order_id={order_id}, child_orders={child_orders}, type={type(child_orders)}")
        
        if child_orders:
            row_child_order_ids = set()
            
            if isinstance(child_orders, str):
                # Skip empty list strings
                if child_orders.strip() in ['[]', '', 'None', 'null']:
                    pass  # Empty, skip validation
                else:
                    # Try JSON parsing first (for proper JSON arrays like '["18861"]')
                    import json
                    import ast
                    try:
                        child_order_list = json.loads(child_orders)
                        if isinstance(child_order_list, list):
                            row_child_order_ids.update(str(cid).strip() for cid in child_order_list if cid)
                    except (json.JSONDecodeError, ValueError):
                        # Try Python literal eval for strings like "['18861']"
                        try:
                            child_order_list = ast.literal_eval(child_orders)
                            if isinstance(child_order_list, list):
                                row_child_order_ids.update(str(cid).strip() for cid in child_order_list if cid)
                        except (ValueError, SyntaxError):
                            # Fall back to comma-separated or single value
                            if ',' in child_orders:
                                row_child_order_ids.update(cid.strip() for cid in child_orders.split(',') if cid.strip())
                            else:
                                # Single value that's not a list representation
                                clean_val = child_orders.strip().strip("[]'\"")
                                if clean_val:
                                    row_child_order_ids.add(clean_val)
                                    
            elif isinstance(child_orders, list):
                # Already a list
                row_child_order_ids.update(str(cid).strip() for cid in child_orders if cid)
            
            logger.debug(f"[DEBUG] Extracted child_order_ids: {row_child_order_ids}")
            
            if row_child_order_ids:
                matching_order_ids = [oid for oid in row_child_order_ids if oid in existing_order_ids]
                logger.debug(f"[DEBUG] Matching order_ids in existing_order_ids: {matching_order_ids}")
                if matching_order_ids:
                    is_valid = False
                    invalid_by_child_orders += 1
                    invalid_reason.append(f"has child_orders {matching_order_ids} (deferred payment placeholder)")
        
        # Validation Method 2: Check if belongs to a parent order
        # If order_id is NOT a child order (not in CHILD_ORDER_IDS) but has child_orders → INVALID
        # If order_id IS a child order (in CHILD_ORDER_IDS) → VALID (actual transaction)
        # Note: We skip this check since Method 1 already handles it correctly
        
        # Set is_valid flag for ALL rows (for audit)
        row['is_valid'] = is_valid
        
        # Count valid/invalid
        if is_valid:
            valid_count += 1
        else:
            invalid_count += 1
            # Collect invalid order_line details
            invalid_order_lines_list.append({
                'order_line_id': order_line_id,
                'order_id': order_id,
                'child_orders': str(child_orders) if child_orders else None,
                'title': row.get('title', 'N/A'),
                'reasons': '; '.join(invalid_reason)
            })
            logger.warning(f"Invalid order_line_id {order_line_id}, order_id {order_id}: {'; '.join(invalid_reason)}")
    
    # Log complete list of invalid order_lines
    if invalid_order_lines_list:
        print(f"\n{'='*80}")
        print(f"❌ COMPLETE LIST OF INVALID ORDER LINES ({len(invalid_order_lines_list)} entries)")
        print(f"{'='*80}")
        for idx, invalid_ol in enumerate(invalid_order_lines_list, 1):
            print(f"❌ [{idx}/{len(invalid_order_lines_list)}] order_line_id={invalid_ol['order_line_id']}, order_id={invalid_ol['order_id']}, title={invalid_ol['title']}, child_orders={invalid_ol['child_orders']}, reasons={invalid_ol['reasons']}")
        print(f"{'='*80}\n")
    
    print(f"\n{'='*80}")
    print(f"ORDER LINES VALIDATION SUMMARY")
    print(f"{'='*80}")
    print(f"Total order_lines processed: {len(cleaned_rows)}")
    print(f"✅ Valid entries (is_valid=TRUE): {valid_count}")
    print(f"❌ Invalid entries (is_valid=FALSE): {invalid_count}")
    print(f"   - Invalid by child_orders field: {invalid_by_child_orders}")
    print(f"   - Invalid by parent_order relationship: {invalid_by_parent_order}")
    print(f"📊 ALL {len(cleaned_rows)} rows will be inserted with is_valid flag set")
    print(f"{'='*80}\n")
    
    logger.info(f"Order lines validation completed: {valid_count} valid, {invalid_count} invalid - ALL {len(cleaned_rows)} rows returned")
    
    # Return ALL rows with is_valid flag set
    return cleaned_rows
    logger.info(f"Invalid breakdown: {invalid_by_child_orders} by child_orders, {invalid_by_parent_order} by parent_order")
    
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


def etl_all_tables(bucket: str, account_id: str, engine, check_stale: bool = False):
    # Initialize global ID sets for this ETL run
    global PROCESSED_ORDER_IDS, CHILD_ORDER_IDS, ORDER_LINE_CREDIT_TRANSACTION_IDS, RESERVATION_CREDIT_TRANSACTION_IDS, ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS, RESERVATION_MEMBERSHIP_TRANSACTION_IDS
    PROCESSED_ORDER_IDS.clear()
    CHILD_ORDER_IDS.clear()
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
            df = fetch_from_s3_pandas(bucket, s3_prefix, account_id)
            
            # Replace NaN values with None before converting to dict
            df = df.where(pd.notnull(df), None)
            
            # Convert to records and clean each row
            cleaned_rows = [clean_row(row, TABLE_COLUMNS_MAP[table_name]) for row in df.to_dict(orient="records")]
            
            # Collect order IDs when processing orders table for later validation
            if table_name == "orders":
                order_ids_in_batch = {row.get('order_id') for row in cleaned_rows if row.get('order_id')}
                PROCESSED_ORDER_IDS.update(order_ids_in_batch)
                logger.info(f"📝 Collected {len(order_ids_in_batch)} order IDs from orders table (Total: {len(PROCESSED_ORDER_IDS)})")
                
                # Collect child order IDs (orders that have a parent_order field)
                child_order_ids_in_batch = {row.get('order_id') for row in cleaned_rows if row.get('parent_order')}
                CHILD_ORDER_IDS.update(child_order_ids_in_batch)
                logger.info(f"📝 Collected {len(child_order_ids_in_batch)} child order IDs (orders with parent_order) (Total: {len(CHILD_ORDER_IDS)})")
            
            # Collect credit transaction IDs from order_lines table
            if table_name == "order_lines":
                credit_tx_ids_in_batch = {row.get('credit_transactions_id') for row in cleaned_rows if row.get('credit_transactions_id')}
                membership_tx_ids_in_batch = {row.get('membership_transactions_id') for row in cleaned_rows if row.get('membership_transactions_id')}
                ORDER_LINE_CREDIT_TRANSACTION_IDS.update(credit_tx_ids_in_batch)
                ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS.update(membership_tx_ids_in_batch)
                print(f"📝 Collected {len(credit_tx_ids_in_batch)} credit transaction IDs from order_lines table (Total: {len(ORDER_LINE_CREDIT_TRANSACTION_IDS)})")
                print(f"📝 Collected {len(membership_tx_ids_in_batch)} membership transaction IDs from order_lines table (Total: {len(ORDER_LINE_MEMBERSHIP_TRANSACTION_IDS)})")
            
            # Collect credit and membership transaction IDs from reservations table
            if table_name == "reservations":
                credit_tx_ids_in_batch = {row.get('credit_transactions_id') for row in cleaned_rows if row.get('credit_transactions_id')}
                membership_tx_ids_in_batch = {row.get('membership_transactions_id') for row in cleaned_rows if row.get('membership_transactions_id')}
                RESERVATION_CREDIT_TRANSACTION_IDS.update(credit_tx_ids_in_batch)
                RESERVATION_MEMBERSHIP_TRANSACTION_IDS.update(membership_tx_ids_in_batch)
                print(f"📝 Collected {len(credit_tx_ids_in_batch)} credit transaction IDs from reservations table (Total: {len(RESERVATION_CREDIT_TRANSACTION_IDS)})")
                print(f"📝 Collected {len(membership_tx_ids_in_batch)} membership transaction IDs from reservations table (Total: {len(RESERVATION_MEMBERSHIP_TRANSACTION_IDS)})")

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
        print(f"✅ Successful tables: {', '.join(successful_tables)}")
        logger.info(f"✅ Successful tables: {', '.join(successful_tables)}")
    if failed_tables:
        print(f"❌ Failed tables: {', '.join(failed_tables)}")
        logger.error(f"❌ Failed tables: {', '.join(failed_tables)}")
    
    # Check and update stale data if requested
    stale_update_success = True
    stale_results = None
    if check_stale:
        logger.info("🔍 check_stale=True, starting stale data update process...")
        stale_results = update_stale_data(engine, account_id, update=False)
        stale_update_success = stale_results.get('success', False)
        if stale_update_success:
            logger.info("✅ Stale data update completed successfully")
        else:
            logger.error("❌ Stale data update completed with errors")
    else:
        logger.info("⏭️  check_stale=False, skipping stale data update")
    
    # Return detailed results
    return {
        'etl_success': len(failed_tables) == 0,
        'stale_update_success': stale_update_success,
        'failed_tables': failed_tables,
        'stale_results': stale_results
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

