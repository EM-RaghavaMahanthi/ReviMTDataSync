import io
import psycopg2
import polars as pl
from datetime import datetime
import boto3
import polars as pl
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, List
import math
import time
from core.config import settings
from schemas.revi_schema import ClassSessions
from db.session import SessionLocal
from models.account import Account

S3_BUCKET = settings.S3_BUCKET
S3_PREFIX = settings.CLASS_SESSIONS_S3_PREFIX
DB_CONNECTION_STRING = settings.DATABASE_URL
BATCH_SIZE = 5000
MAX_WORKERS = 4

s3_client = boto3.client("s3")
engine = create_engine(DB_CONNECTION_STRING)

def drop_nested_columns(df):
    # Drop columns with nested (struct, list, dict) data
    nested_types = [pl.List, pl.Struct]
    cols_to_drop = [col for col in df.columns if any(isinstance(df[col].dtype, t) for t in nested_types)]
    if cols_to_drop:
        print(f"Dropping nested columns: {cols_to_drop}")
        df = df.drop(cols_to_drop)
    return df

def read_class_sessions_parquet_from_s3(bucket: str, prefix: str) -> pl.DataFrame:
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parquet_keys = [obj["Key"] for obj in objects.get("Contents", []) if obj["Key"].endswith(".parquet")]
    if not parquet_keys:
        raise RuntimeError(f"No Parquet files found in {prefix}")
    dfs = []
    for key in parquet_keys:
        s3_path = f"s3://{bucket}/{key}"
        print(f"Reading: {s3_path}")
        df = pl.read_parquet(s3_path, use_pyarrow=True)
        df = drop_nested_columns(df)
        df = df.with_columns([
    pl.col("id").cast(pl.Int64),
    pl.col("account_id").cast(pl.Int64),
    pl.col("created_by").cast(pl.Int64),
    pl.col("updated_by").cast(pl.Int64),
    pl.col("deleted_by").cast(pl.Int64),
])
        # Format all datetime columns as string
        for col in df.columns:
            if df[col].dtype == pl.Datetime:
                df = df.with_columns(df[col].dt.strftime("%Y-%m-%d %H:%M:%S").alias(col))
        dfs.append(df)
    return concat_class_sessions_dfs(dfs)

def concat_class_sessions_dfs(dfs):
    df = pl.concat(dfs, how="vertical")
    print(f"[INFO] Total shape after concatenation: {df.shape}")
    for col in df.columns:
        if df[col].dtype == pl.Float64:
            if ((df[col].drop_nulls() % 1) == 0).all():
                df = df.with_columns(df[col].cast(pl.Int64))
    return df

def filter_and_validate_class_sessions(df: pl.DataFrame) -> List[dict]:
    valid_rows = []
    for record in df.to_dicts():
        try:
            cs = ClassSessions(**record)
            valid_rows.append(cs.model_dump())
        except Exception as e:
            print(f"Validation error for class_session id={record.get('id')}: {e}")
    return valid_rows

def insert_class_sessions(class_sessions: List[dict], table_name: str = "class_sessions_rmtest"):
    if not class_sessions:
        print("No valid class sessions to insert.")
        return
    import concurrent.futures
    total_chunks = math.ceil(len(class_sessions) / BATCH_SIZE)
    print(f"Inserting {len(class_sessions)} class sessions in {total_chunks} chunks of size {BATCH_SIZE}...")
    sql = f"""
        INSERT INTO {table_name} (
            id, class_session_id, start_datetime, start_date, location, end_datetime, cancellation_datetime,
            account_id, created_at, created_by, updated_at, updated_by, deleted_at, deleted_by
        )
        VALUES (
            :id, :class_session_id, :start_datetime, :start_date, :location, :end_datetime, :cancellation_datetime,
            :account_id, :created_at, :created_by, :updated_at, :updated_by, :deleted_at, :deleted_by
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
            chunk = class_sessions[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
            futures.append(executor.submit(insert_chunk, chunk, i))
        concurrent.futures.wait(futures)

def bulk_insert_class_sessions(class_sessions: list, table_name: str = "class_sessions"):
    if not class_sessions:
        print("No valid class sessions to insert.")
        return
    
    expected_columns = [
    'id',
    'class_session_id',
    'start_datetime',
    'start_date',
    'location',
    'end_datetime',
    'cancellation_datetime',
    'created_at',
    'created_by',
    'updated_at',
    'updated_by',
    'deleted_at',
    'deleted_by',
    'account_id'
]

    # Clean and ensure all columns are present
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
    for row in class_sessions:
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
    copy_sql = f"""COPY "{table_name}" ({','.join([f'"{col}"' for col in expected_columns])}) FROM STDIN WITH (FORMAT csv, HEADER true)"""
    cursor.copy_expert(copy_sql, csv_buffer)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Copied {len(class_sessions)} rows into table {table_name}.")

def main():
    print("Reading ClassSessions Parquet files from S3...")
    t0 = time.time()
    df = read_class_sessions_parquet_from_s3(S3_BUCKET, S3_PREFIX)
    t1 = time.time()
    print(f"Read Parquet files in {t1-t0:.2f}s")
    print("First 10 rows of combined DataFrame:")
    print(df.head(10))
    sample_csv_path = "class_sessions_sample.csv"
    df.write_csv(sample_csv_path)
    print(f"Sample (first 1000 rows) saved as {sample_csv_path}")
    print("Validating class sessions...")
    t2 = time.time()
    valid_class_sessions = filter_and_validate_class_sessions(df)
    t3 = time.time()
    print(f"Valid rows: {len(valid_class_sessions)}. Filtered in {t3-t2:.2f}s")
    print("Inserting class sessions into database...")
    table_name = "mt_class_sessions_details_dlk"
    t4 = time.time()
    bulk_insert_class_sessions(valid_class_sessions, table_name=table_name)
    t5 = time.time()
    print(f"Inserted all batches in {t5-t4:.2f}s")

if __name__ == "__main__":
    main()
