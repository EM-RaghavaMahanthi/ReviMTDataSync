import boto3
import polars as pl
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, List
import math
import time
from core.config import settings
from schemas.revi_schema import Reservation  
from db.session import SessionLocal
from models.account import Account, States


S3_BUCKET = settings.S3_BUCKET
S3_PREFIX = settings.RESERVATIONS_S3_PREFIX 
DB_CONNECTION_STRING = settings.DATABASE_URL
BATCH_SIZE = 5000
MAX_WORKERS = 4

s3_client = boto3.client("s3")
engine = create_engine(DB_CONNECTION_STRING)


def get_active_accounts() -> List[str]:
    """
    Query the database for active account IDs.
    """
    session = SessionLocal()
    try:
        results = session.query(Account.id).filter(
            Account.status == 'ACTIVE',
            Account.crm_config.isnot(None),
            Account.crm_api_end_point.isnot(None)
        ).all()
        # Return dict for fast lookup
        return {row.id: True for row in results if row.id}
    finally:
        session.close()


def get_active_states() -> List[int]:
    """
    Query the database for active state IDs.
    """
    session = SessionLocal()
    try:
        results = session.query(States.id).all()
        # Return dict for fast lookup
        return {row.id: True for row in results if row.id}
    finally:
        session.close()

def read_reservations_parquet_from_s3(bucket: str, prefix: str) -> pl.DataFrame:
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parquet_keys = [obj["Key"] for obj in objects.get("Contents", []) if obj["Key"].endswith(".parquet")]
    if not parquet_keys:
        raise RuntimeError(f"No Parquet files found in {prefix}")
    dfs = []
    for key in parquet_keys:
        s3_path = f"s3://{bucket}/{key}"
        print(f"Reading: {s3_path}")
        df = pl.read_parquet(s3_path, use_pyarrow=True)
        

        df = df.with_columns([
            pl.col("id").cast(pl.Int64),
            pl.col("reservations_id").cast(pl.Int64),
            pl.col("credit_transactions_id").cast(pl.Int64),
            pl.col("credit_transactions_ref_id").cast(pl.Int64),
            pl.col("membership_transactions_id").cast(pl.Int64),
            pl.col("membership_transactions_ref_id").cast(pl.Int64),
            pl.col("class_session_ref_id").cast(pl.Int64),
            pl.col("account_id").cast(pl.Int64),
            pl.col("location").cast(pl.Int64),
            pl.col("created_by").cast(pl.Int64),
            pl.col("updated_by").cast(pl.Int64),
            pl.col("deleted_by").cast(pl.Int64)
        ])

        

        # Format all datetime columns as string
        for col in df.columns:
            if df[col].dtype == pl.Datetime:
                df = df.with_columns(df[col].dt.strftime("%Y-%m-%d %H:%M:%S").alias(col))
        dfs.append(df)
    df_all = pl.concat(dfs, how="vertical")
    print(f"[INFO] Total shape after concatenation: {df_all.shape}")
    return df_all

def concat_reservation_dfs(dfs):
    df = pl.concat(dfs, how="vertical")
    print(f"[INFO] Total shape after concatenation: {df.shape}")
    # Cast float columns to Int64 if all non-null values are integer-valued
    for col in df.columns:
        if df[col].dtype == pl.Float64:
            # Vectorized check: Only cast if all non-null values are integer-valued
            if ((df[col].drop_nulls() % 1) == 0).all():
                df = df.with_columns(df[col].cast(pl.Int64))

    
    return df

def filter_and_validate_reservations(df: pl.DataFrame, valid_accounts: set, account_id: int = None) -> List[dict]:
    # if account_id is not None:
    #     if account_id not in valid_accounts:
    #         raise ValueError(f"Provided account_id {account_id} is not in valid accounts.")
    #     print(f"Setting account_id={account_id} for all rows...")
    #     df = df.with_columns(pl.lit(account_id).alias("account_id"))
    valid_rows = []
    for record in df.to_dicts():
        try:
            res = Reservation(**record)
            # Optionally check account_id FK
            # if res.account_id not in valid_accounts:
            #     print(f"Skipping reservation id={res.id} - invalid account_id {res.account_id}")
            #     continue
            valid_rows.append(res.model_dump())
        except Exception as e:
            print(f"Validation error for reservation id={record.get('id')}: {e}")
    return valid_rows

def insert_reservations(reservations: List[dict], table_name: str = "reservations_rmtest"):
    if not reservations:
        print("No valid reservations to insert.")
        return
    import concurrent.futures
    total_chunks = math.ceil(len(reservations) / BATCH_SIZE)
    print(f"Inserting {len(reservations)} reservations in {total_chunks} chunks of size {BATCH_SIZE}...")
    sql = f"""
        INSERT INTO {table_name} (
            id, reservations_id, cancel_date, check_in_date, creation_date, status,
            credit_transactions_type, credit_transactions_id, credit_transactions_ref_id,
            membership_transactions_type, membership_transactions_id, membership_transactions_ref_id,
            guest, customer_id, customer_ref_id, class_session_id, class_session_ref_id,
            account_id, first_timer, reservation_type, location, created_at, created_by,
            updated_at, updated_by, deleted_at, deleted_by
        )
        VALUES (
            :id, :reservations_id, :cancel_date, :check_in_date, :creation_date, :status,
            :credit_transactions_type, :credit_transactions_id, :credit_transactions_ref_id,
            :membership_transactions_type, :membership_transactions_id, :membership_transactions_ref_id,
            :guest, :customer_id, :customer_ref_id, :class_session_id, :class_session_ref_id,
            :account_id, :first_timer, :reservation_type, :location, :created_at, :created_by,
            :updated_at, :updated_by, :deleted_at, :deleted_by
        )
        ON CONFLICT (id) DO NOTHING
    """
    def insert_chunk(chunk, idx):
        start = time.time()
        local_engine = create_engine(DB_CONNECTION_STRING)
        with local_engine.begin() as conn:
            conn.execute(text(sql), chunk)
        elapsed = time.time() - start
        print(f"Inserted chunk {idx+1}/{total_chunks} with {len(chunk)} records. Time: {elapsed:.2f}s (committed)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for i in range(total_chunks):
            chunk = reservations[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
            futures.append(executor.submit(insert_chunk, chunk, i))
        concurrent.futures.wait(futures)

import io
import polars as pl
import psycopg2

def bulk_insert_reservations(reservations: List[dict], table_name: str = "reservations_rmtest"):
    if not reservations:
        print("No valid reservations to insert.")
        return

    # Define columns to insert excluding 'id' if it's auto-generated
    expected_columns = [
        'reservations_id', 'cancel_date', 'check_in_date', 'creation_date', 'status',
        'credit_transactions_type', 'credit_transactions_id', 'credit_transactions_ref_id',
        'membership_transactions_type', 'membership_transactions_id', 'membership_transactions_ref_id',
        'guest', 'customer_id', 'customer_ref_id', 'class_session_id', 'class_session_ref_id',
        'account_id', 'first_timer', 'reservation_type', 'location', 'created_at', 'created_by',
        'updated_at', 'updated_by', 'deleted_at', 'deleted_by'
    ]

    # Clean data, removing empty strings and ensuring all keys are present
    def clean_reservation_row(row, expected_cols):
        clean_row = {}
        for col in expected_cols:
            val = row.get(col, None)
            if val == "":
                val = None
            # Convert datetime objects to string
            if hasattr(val, 'isoformat'):
                val = val.strftime('%Y-%m-%d %H:%M:%S')
            clean_row[col] = val
        return clean_row

    # Ensure all rows have all expected columns, fill missing with None
    cleaned_reservations = []
    for row in reservations:
        clean_row = clean_reservation_row(row, expected_columns)
        for col in expected_columns:
            if col not in clean_row:
                clean_row[col] = None
        cleaned_reservations.append(clean_row)

    # Create Polars DataFrame with increased infer_schema_length
    df = pl.DataFrame(cleaned_reservations, infer_schema_length=10000)
    # Fill nulls in created_at and updated_at with current timestamp
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if "created_at" in df.columns:
        df = df.with_columns(
            pl.col("created_at").fill_null(now)
        )
    if "updated_at" in df.columns:
        df = df.with_columns(
            pl.col("updated_at").fill_null(now)
        )

    # Convert DataFrame to CSV string
    csv_data = df.write_csv(None)  # returns string

    # Prepare CSV buffer for COPY
    csv_buffer = io.StringIO(csv_data)

    # COPY to database
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()

    copy_sql = f"""COPY {table_name} ({','.join(df.columns)}) FROM STDIN WITH (FORMAT csv, HEADER true)"""
    cursor.copy_expert(copy_sql, csv_buffer)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Copied {len(reservations)} rows into table {table_name}.")


def main():
    print("Loading valid accounts...")
    # You can reuse your get_active_accounts() from customers_service.py
    valid_accounts = get_active_accounts()
    print("Reading Reservation Parquet files from S3...")
    t0 = time.time()
    df = read_reservations_parquet_from_s3(S3_BUCKET, S3_PREFIX)
    t1 = time.time()
    print(f"Read Parquet files in {t1-t0:.2f}s")
    print("First 10 rows of combined DataFrame:")
    print(df.head(10))
    sample_csv_path = "reservations_sample.csv"
    df.head(1000).write_csv(sample_csv_path)
    print(f"Sample (first 1000 rows) saved as {sample_csv_path}")
    print("Validating and filtering reservations...")
    account_id = 739  # Change as needed
    t2 = time.time()
    valid_reservations = filter_and_validate_reservations(df, valid_accounts, account_id=account_id)
    t3 = time.time()
    print(f"Valid rows after FK check: {len(valid_reservations)}. Filtered in {t3-t2:.2f}s")
    print("Inserting reservations into database...")
    table_name = "mt_reservations_details_dlk"
    t4 = time.time()
    bulk_insert_reservations(valid_reservations, table_name = table_name)
    #insert_reservations(valid_reservations, table_name=table_name)
    t5 = time.time()
    print(f"Inserted all batches in {t5-t4:.2f}s")



def test_insert_first_100_reservations():
    print("Loading valid accounts...")
    valid_accounts = get_active_accounts()
    print("Reading Reservation Parquet files from S3...")
    df = read_reservations_parquet_from_s3(S3_BUCKET, S3_PREFIX)
    print("Validating and filtering reservations...")
    account_id = 739  # Change as needed
    valid_reservations = filter_and_validate_reservations(df, valid_accounts, account_id=account_id)
    print(f"Testing insert for first 100 records out of {len(valid_reservations)}")
    table_name = "mt_reservations_details_dlk"
    test_records = valid_reservations[:100]
    print(f"Total valid reservations: {len(valid_reservations)}")
    bulk_insert_reservations(valid_reservations, table_name = table_name)
    print(f"Bulk insert complete. in {DB_CONNECTION_STRING}")
    #insert_reservations(test_records, table_name=table_name)
    print("Test insert complete.")

if __name__ == "__main__":
    main()
    
    # Uncomment below to run test insert for first 100 records
    # test_insert_first_100_reservations()