import io
import psycopg2
import polars as pl
from datetime import datetime
from core.config import settings

# Use production DB
DB_CONNECTION_STRING = settings.DATABASE_URL

def bulk_insert_membership_transactions(membership_transactions: list, table_name: str = "membership_transactions"):
    if not membership_transactions:
        print("No valid membership transactions to insert.")
        return

    expected_columns = [
        'id',
        'membership_transactions_id',
        'transaction_date',
        'membership_name',
        'parent_membership_transaction_id',
        'membership_instances_id',
        'customer_id',
        'location',
        'created_at',
        'created_by',
        'updated_at',
        'updated_by',
        'deleted_at',
        'deleted_by',
        'account_id',
        'customer_ref_id',
        'membership_instances_ref_id'
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
    for row in membership_transactions:
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
    print(f"Copied {len(membership_transactions)} rows into table {table_name}.")
import boto3
import polars as pl
from core.config import settings
from schemas.revi_schema import MembershipTransaction
from db.session import SessionLocal
from models.account import Account
from sqlalchemy import create_engine, text
from typing import List
import time

S3_BUCKET = settings.S3_BUCKET
S3_PREFIX = settings.S3_PREFIXES.get("membership_transactions", "membership-transactions-details")
#DB_CONNECTION_STRING = settings.DATABASE_URL
BATCH_SIZE = 5000
MAX_WORKERS = 4

s3_client = boto3.client("s3")
engine = create_engine(DB_CONNECTION_STRING)

def drop_nested_columns(df):
    nested_types = [pl.List, pl.Struct]
    cols_to_drop = [col for col in df.columns if any(isinstance(df[col].dtype, t) for t in nested_types)]
    if cols_to_drop:
        print(f"Dropping nested columns: {cols_to_drop}")
        df = df.drop(cols_to_drop)
    return df

def read_membership_transactions_parquet_from_s3(bucket: str, prefix: str) -> pl.DataFrame:
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
        # Cast common int columns
        df = df.with_columns([
            pl.col("id").cast(pl.Int64),
            pl.col("membership_transactions_id").cast(pl.Int64),
            pl.col("parent_membership_transaction_id").cast(pl.Int64),
            pl.col("membership_instances_id").cast(pl.Int64),
            pl.col("membership_instances_ref_id").cast(pl.Int64),
            pl.col("customer_ref_id").cast(pl.Int64),
            pl.col("account_id").cast(pl.Int64),
            pl.col("location").cast(pl.Int64),
            pl.col("created_by").cast(pl.Int64),
            pl.col("updated_by").cast(pl.Int64),
            pl.col("deleted_by").cast(pl.Int64),
        ])
        # Format all datetime columns as string
        for col in df.columns:
            if df[col].dtype == pl.Datetime:
                df = df.with_columns(df[col].dt.strftime("%Y-%m-%d %H:%M:%S").alias(col))
        dfs.append(df)
    return concat_membership_transactions_dfs(dfs)

def concat_membership_transactions_dfs(dfs):
    df = pl.concat(dfs, how="vertical")
    print(f"[INFO] Total shape after concatenation: {df.shape}")
    for col in df.columns:
        if df[col].dtype == pl.Float64:
            if ((df[col].drop_nulls() % 1) == 0).all():
                df = df.with_columns(df[col].cast(pl.Int64))
    return df

def filter_and_validate_membership_transactions(df: pl.DataFrame) -> List[dict]:
    valid_rows = []
    for record in df.to_dicts():
        try:
            mt = MembershipTransaction(**record)
            valid_rows.append(mt.model_dump())
        except Exception as e:
            print(f"Validation error for membership_transaction id={record.get('id')}: {e}")
    return valid_rows

def insert_membership_transactions(membership_transactions: List[dict], table_name: str = "membership_transactions_rmtest"):
    if not membership_transactions:
        print("No membership transactions to insert.")
        return 0
    session = SessionLocal()
    try:
        for mt in membership_transactions:
            stmt = text(f"""
                INSERT INTO {table_name} (
                    membership_transactions_id, transaction_date, membership_name, parent_membership_transaction_id,
                    membership_instances_id, customer_id, location, created_at, created_by, updated_at, updated_by,
                    deleted_at, deleted_by, customer_ref_id, account_id, membership_instances_ref_id, next_charge_date
                ) VALUES (
                    :membership_transactions_id, :transaction_date, :membership_name, :parent_membership_transaction_id,
                    :membership_instances_id, :customer_id, :location, :created_at, :created_by, :updated_at, :updated_by,
                    :deleted_at, :deleted_by, :customer_ref_id, :account_id, :membership_instances_ref_id, :next_charge_date
                )
            """)
            session.execute(stmt, mt)
        session.commit()
        print(f"Inserted {len(membership_transactions)} membership transactions into {table_name}")
        return len(membership_transactions)
    except Exception as e:
        print(f"Error inserting membership transactions: {e}")
        session.rollback()
        return 0
    finally:
        session.close()

def main():
    print("Reading MembershipTransaction Parquet files from S3...")
    t0 = time.time()
    df = read_membership_transactions_parquet_from_s3(S3_BUCKET, S3_PREFIX)
    t1 = time.time()
    print(f"Read Parquet files in {t1-t0:.2f}s")
    print("First 10 rows of combined DataFrame:")
    print(df.head(10))
    sample_csv_path = "membership_transactions_sample.csv"
    df.head(1000).write_csv(sample_csv_path)
    print(f"Sample (first 1000 rows) saved as {sample_csv_path}")
    print("Validating membership transactions...")
    t2 = time.time()
    valid_membership_transactions = filter_and_validate_membership_transactions(df)
    t3 = time.time()
    print(f"Valid rows: {len(valid_membership_transactions)}. Filtered in {t3-t2:.2f}s")
    print("Inserting membership transactions into database...")
    table_name = "mt_membership_transactions_details_dlk"
    t4 = time.time()
    bulk_insert_membership_transactions(valid_membership_transactions, table_name=table_name)
    t5 = time.time()
    print(f"Inserted all batches in {t5-t4:.2f}s")

if __name__ == "__main__":
    main()
