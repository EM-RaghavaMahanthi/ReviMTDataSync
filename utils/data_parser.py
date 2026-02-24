"""
Generic data parsing utilities for S3 to staging CSV pipeline.

These utilities are table-agnostic and can be reused for parsing
any table type (customers, orders, class_sessions, etc.)
"""

import logging
import pandas as pd
import awswrangler as wr
import boto3
import math
from typing import List, Dict, Optional, Any, Type, Tuple
from datetime import datetime
from pydantic import ValidationError, BaseModel

# Initialize logger
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3')


def safe_convert_nan_to_none(value: Any) -> Any:
    """
    Generic function to convert NaN/NaT/Inf values to None.
    Handles all pandas NaN types safely.
    
    Args:
        value: Any value that might be NaN
    
    Returns:
        None if value is NaN/NaT/Inf, otherwise original value
    """
    if value is None:
        return None
    
    # Check for pandas NaT (Not a Time)
    if pd.isna(value):
        return None
    
    # Check for float NaN and Inf
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    
    # Check for numpy types (if numpy is available)
    try:
        import numpy as np
        if isinstance(value, (np.floating, np.integer)):
            if np.isnan(value) or np.isinf(value):
                return None
    except (ImportError, TypeError, ValueError):
        pass
    
    return value


def extract_date_parts(date_value: Any, date_field_name: str = "date") -> Tuple[Optional[int], Optional[int]]:
    """
    Generic function to extract month and day from a date value.
    Works with any date field (birth_date, order_date, etc.)
    
    Args:
        date_value: Date value (string, timestamp, or datetime object)
        date_field_name: Name of the date field for logging purposes
    
    Returns:
        Tuple of (month, day) or (None, None) if extraction fails
    """
    # Check for None, empty string, or NaN values
    if date_value is None:
        return None, None
    if isinstance(date_value, str) and not date_value.strip():
        return None, None
    if isinstance(date_value, float) and (math.isnan(date_value) or math.isinf(date_value)):
        return None, None
    if pd.isna(date_value):
        return None, None
    
    try:
        # For string dates, try to extract month/day directly from format YYYY-MM-DD
        if isinstance(date_value, str):
            if '-' in date_value and len(date_value) >= 10:
                parts = date_value.split('-')
                try:
                    month = int(parts[1]) if len(parts) > 1 else None
                    day = int(parts[2][:2]) if len(parts) > 2 else None  # Take first 2 chars
                    return month, day
                except (ValueError, IndexError):
                    pass
            # For non-standard formats, try pd.to_datetime
            dt = pd.to_datetime(date_value)
            if pd.isna(dt):  # Check if pd.to_datetime returned NaT
                return None, None
            return dt.month, dt.day
        elif isinstance(date_value, (int, float)):
            # Handle Unix timestamp
            dt = pd.to_datetime(date_value, unit='ms' if date_value > 1000000000000 else 's')
            if pd.isna(dt):  # Check if pd.to_datetime returned NaT
                return None, None
            return dt.month, dt.day
        elif hasattr(date_value, 'month') and hasattr(date_value, 'day'):
            # Handle datetime objects
            return date_value.month, date_value.day
    except Exception as e:
        # Only log if it's not a known weird date format
        if not (isinstance(date_value, str) and date_value.startswith(('0000', '0001', '0002', '0003', '0004', '0005', '0006', '0007', '0008', '0009'))):
            logger.debug(f"Failed to parse {date_field_name} '{date_value}': {e}")
    
    return None, None


def clean_datetime_value(val: Any) -> Optional[str]:
    """
    Generic function to clean a datetime value.
    Handles Unix timestamps, datetime objects, and string dates.
    
    Args:
        val: Value to clean
    
    Returns:
        Cleaned datetime string in 'YYYY-MM-DD HH:MM:SS' format or original string
    """
    if pd.isna(val) or val is None:
        return None
    # Handle Unix timestamps
    if isinstance(val, (int, float)) and 1000000000 <= val <= 9999999999999:
        try:
            dt = pd.to_datetime(val, unit='ms' if val > 1000000000000 else 's')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    # Handle datetime objects
    elif hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    # Keep string dates as-is (including weird dates like '0087-08-16')
    elif isinstance(val, str):
        val_clean = val.strip()
        # Handle 2-digit year dates like "66-12-30"
        if len(val_clean) == 8 and val_clean.count('-') == 2:
            parts = val_clean.split('-')
            if len(parts[0]) == 2 and parts[0].isdigit():
                year = int(parts[0])
                full_year = 1900 + year if year >= 0 else year
                return f"{full_year}-{parts[1]}-{parts[2]}"
        return val_clean  # Keep as-is for weird dates
    return val


def clean_row(
    row: dict, 
    expected_columns: List[str], 
    datetime_columns: List[str],
    boolean_columns: List[str] = None,
    integer_columns: List[str] = None
) -> dict:
    """
    Generic function to clean a row dict for CSV output.
    Matches the logic from main_bulk_insert_service.py clean_row function.
    
    Args:
        row: Raw extracted dict
        expected_columns: List of expected column names
        datetime_columns: List of datetime column names for special handling
        boolean_columns: List of boolean column names to convert to int (0/1)
        integer_columns: List of columns that should be integers
    
    Returns:
        Cleaned dict ready for CSV
    """
    if boolean_columns is None:
        boolean_columns = []
    if integer_columns is None:
        integer_columns = []
    
    clean = {}
    
    for col in expected_columns:
        val = row.get(col, None)
        
        # Use generic NaN converter first
        val = safe_convert_nan_to_none(val)
        
        # Handle various forms of null/NaN values (additional safety)
        if val == "":
            val = None
        elif isinstance(val, str) and val.strip().lower() in ['nan', 'none', 'null', 'nat']:
            val = None
        
        # Now handle type-specific conversions (only if val is not None)
        if val is not None:
            # Handle datetime columns
            if col in datetime_columns:
                val = clean_datetime_value(val)
            
            # Handle boolean columns - convert to int (0/1)
            elif col in boolean_columns:
                if isinstance(val, bool):
                    val = int(val)
                elif isinstance(val, (int, float)):
                    val = int(val)
                elif isinstance(val, str):
                    val = 1 if val.lower() in ['true', '1', 'yes'] else 0
            
            # Keep integer columns as integers (not strings)
            elif col in integer_columns:
                try:
                    val = int(val)
                except (ValueError, TypeError):
                    val = None
        
        clean[col] = val
    
    # Fill missing columns with None
    for col in expected_columns:
        if col not in clean:
            clean[col] = None
    
    return clean


def validate_with_schema(record: dict, schema_class: Type[BaseModel]) -> Optional[BaseModel]:
    """
    Generic function to validate a record against a Pydantic schema.
    
    Args:
        record: Cleaned record dict
        schema_class: Pydantic model class to validate against
    
    Returns:
        Model instance or None if validation fails
    """
    try:
        # Pydantic validation
        validated = schema_class(**record)
        return validated
    except ValidationError as e:
        # Log validation error with record ID for debugging
        record_id = record.get('id', 'unknown')
        customer_id = record.get('customer_id', 'unknown')
        logger.error(f"Validation failed for record ID {record_id} or customer_id {customer_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected validation error: {e}")
        return None


def read_parquet_from_s3(bucket: str, prefix: str) -> pd.DataFrame:
    """
    Generic function to read and concatenate all parquet files from an S3 prefix.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix path
    
    Returns:
        Combined DataFrame
    """
    logger.info(f"Reading parquet files from s3://{bucket}/{prefix}")
    
    # List all parquet files in prefix
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parquet_keys = [obj["Key"] for obj in objects.get("Contents", []) if obj["Key"].endswith(".parquet")]
    
    if not parquet_keys:
        logger.warning(f"No parquet files found in s3://{bucket}/{prefix}")
        return pd.DataFrame()
    
    logger.info(f"Found {len(parquet_keys)} parquet files to process")
    
    # Read and concatenate all parquet files
    dfs = []
    for key in parquet_keys:
        s3_path = f"s3://{bucket}/{key}"
        logger.info(f"Reading: {s3_path}")
        df = wr.s3.read_parquet(s3_path)
        dfs.append(df)
    
    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total records loaded: {len(combined_df)}")
    
    return combined_df
