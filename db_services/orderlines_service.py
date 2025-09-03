import io
import psycopg2
import polars as pl
from datetime import datetime
from core.config import settings

# Use production DB
DB_CONNECTION_STRING = settings.DATABASE_URL

def bulk_insert_order_lines(order_lines: list, table_name: str = "order_lines"):
    if not order_lines:
        print("No valid order lines to insert.")
        return

    expected_columns = [
        'id',
        'order_line_id',
        'order_id',
        'transaction_type',
        'location',
        'credit_transactions_id',
        'membership_transactions_id',
        'title',
        'processed_by',
        'created_at',
        'created_by',
        'updated_at',
        'updated_by',
        'deleted_at',
        'deleted_by',
        'account_id',
        'credit_transactions_ref_id',
        'membership_transactions_ref_id',
        'order_ref_id'
    ]

    def clean_row(row, expected_columns):
        clean = {}
        for col in expected_columns:
            val = row.get(col, None)
            if val == "":
                val = None
            if hasattr(val, 'isoformat'):
                val = val.strftime('%Y-%m-%d %H:%M:%S')
            clean[col] = val
        return clean

    cleaned = []
    for row in order_lines:
        clean = clean_row(row, expected_columns)
        for col in expected_columns:
            if col not in clean:
                clean[col] = None
        cleaned.append(clean)

    df = pl.DataFrame(cleaned, infer_schema_length=10000)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if "created_at" in df.columns:
        df = df.with_columns(pl.col("created_at").fill_null(now))
    if "updated_at" in df.columns:
        df = df.with_columns(pl.col("updated_at").fill_null(now))

    csv_data = df.write_csv(None)
    csv_buffer = io.StringIO(csv_data)

    conn = psycopg2.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    copy_sql = f'''COPY "{table_name}" ({','.join([f'"{col}"' for col in expected_columns])}) FROM STDIN WITH (FORMAT csv, HEADER true)'''
    cursor.copy_expert(copy_sql, csv_buffer)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Copied {len(order_lines)} rows into table {table_name}.")
import boto3
import polars as pl
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, List
import math
import time
from core.config import settings
from schemas.revi_schema import OrderLines
from db.session import SessionLocal
from models.account import Account

S3_BUCKET = settings.S3_BUCKET
S3_PREFIX = settings.ORDER_LINES_S3_PREFIX 
BATCH_SIZE = 5000
MAX_WORKERS = 4

s3_client = boto3.client("s3")
engine = create_engine(DB_CONNECTION_STRING)

def get_active_accounts() -> dict:
    session = SessionLocal()
    try:
        results = session.query(Account.id).filter(
            Account.status == 'ACTIVE',
            Account.crm_config.isnot(None),
            Account.crm_api_end_point.isnot(None)
        ).all()
        return {row.id: True for row in results if row.id}
    finally:
        session.close()

def read_orderlines_parquet_from_s3(bucket: str, prefix: str) -> pl.DataFrame:
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
    pl.col("order_ref_id").cast(pl.Int64),
    pl.col("account_id").cast(pl.Int64),
    pl.col("credit_transactions_id").cast(pl.Int64),
    pl.col("credit_transactions_ref_id").cast(pl.Int64),
    pl.col("membership_transactions_id").cast(pl.Int64),
    pl.col("membership_transactions_ref_id").cast(pl.Int64),
    pl.col("created_by").cast(pl.Int64),
    pl.col("updated_by").cast(pl.Int64),
    pl.col("deleted_by").cast(pl.Int64),
])
        for col in df.columns:
            if df[col].dtype == pl.Datetime:
                df = df.with_columns(df[col].dt.strftime("%Y-%m-%d %H:%M:%S").alias(col))
        dfs.append(df)
    df_all = pl.concat(dfs, how="vertical")
    print(f"[INFO] Total shape after concatenation: {df_all.shape}")
    return df_all

def filter_and_validate_orderlines(df: pl.DataFrame, valid_accounts: dict, account_id: int = None) -> List[dict]:
    if account_id is not None:
        if account_id not in valid_accounts:
            raise ValueError(f"Provided account_id {account_id} is not in valid accounts.")
        print(f"Setting account_id={account_id} for all rows...")
        df = df.with_columns(pl.lit(account_id).alias("account_id"))
    valid_rows = []
    for record in df.to_dicts():
        try:
            order_line = OrderLines(**record)
            valid_rows.append(order_line.model_dump())
        except Exception as e:
            print(f"Validation error for order_line id={record.get('id')}: {e}")
    return valid_rows

def insert_orderlines(orderlines: List[dict], table_name: str = "orderlines_rmtest"):
    if not orderlines:
        print("No valid order lines to insert.")
        return
    import concurrent.futures
    total_chunks = math.ceil(len(orderlines) / BATCH_SIZE)
    print(f"Inserting {len(orderlines)} order lines in {total_chunks} chunks of size {BATCH_SIZE}...")
    sql = f"""
        INSERT INTO {table_name} (
            id, order_line_id, order_id, order_ref_id, account_id, transaction_type, location,
            credit_transactions_id, credit_transactions_ref_id, membership_transactions_id, membership_transactions_ref_id,
            title, processed_by, created_at, created_by, updated_at, updated_by, deleted_at, deleted_by
        )
        VALUES (
            :id, :order_line_id, :order_id, :order_ref_id, :account_id, :transaction_type, :location,
            :credit_transactions_id, :credit_transactions_ref_id, :membership_transactions_id, :membership_transactions_ref_id,
            :title, :processed_by, :created_at, :created_by, :updated_at, :updated_by, :deleted_at, :deleted_by
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
            chunk = orderlines[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
            futures.append(executor.submit(insert_chunk, chunk, i))
        concurrent.futures.wait(futures)

def main():
    print("Loading valid accounts...")
    valid_accounts = get_active_accounts()
    print("Reading OrderLines Parquet files from S3...")
    t0 = time.time()
    df = read_orderlines_parquet_from_s3(S3_BUCKET, S3_PREFIX)
    t1 = time.time()
    print(f"Read Parquet files in {t1-t0:.2f}s")
    print("First 10 rows of combined DataFrame:")
    print(df.head(10))
    sample_csv_path = "orderlines_sample.csv"
    flat_columns = [col for col in df.columns if df[col].dtype not in [pl.List, pl.Struct]]
    df.select(flat_columns).head(1000).write_csv(sample_csv_path)
    print(f"Sample (first 1000 rows, flat columns only) saved as {sample_csv_path}")
    print("Validating and filtering order lines...")
    account_id = 739  # Change as needed
    t2 = time.time()
    valid_orderlines = filter_and_validate_orderlines(df, valid_accounts, account_id=account_id)
    t3 = time.time()
    print(f"Valid rows after FK check: {len(valid_orderlines)}. Filtered in {t3-t2:.2f}s")
    print("Inserting order lines into database...")
    table_name = "mt_order_lines_details_dlk"
    t4 = time.time()
    bulk_insert_order_lines(valid_orderlines, table_name=table_name)
    t5 = time.time()
    print(f"Inserted all batches in {t5-t4:.2f}s")

if __name__ == "__main__":
    main()
