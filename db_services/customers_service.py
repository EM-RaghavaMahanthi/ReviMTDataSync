import boto3
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List
import math
import time
from core.config import settings  
from db.session import SessionLocal
from models.account import Account, States

# ==============================
# CONFIG
# ==============================
S3_BUCKET = settings.S3_BUCKET
S3_PREFIX = settings.CUSTOMERS_S3_PREFIX  
DB_CONNECTION_STRING = settings.DATABASE_URL
BATCH_SIZE = 5000  # Optimized for db.t3.medium
MAX_WORKERS = 4    # Safe concurrency for db.t3.medium

# ==============================
# Initialize Clients
# ==============================
s3_client = boto3.client("s3")
engine = create_engine(DB_CONNECTION_STRING)

# ==============================
# 1. Load Parent IDs from DB
# ==============================



def get_active_accounts() -> List[str]:
    """
    Query the database for active account IDs.
    """
    session = SessionLocal()
    try:
        results = session.query(Account.id).filter(
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

def load_parent_ids():
    with engine.connect() as conn:
        accounts = get_active_accounts()
        states = get_active_states()
    return accounts, states

# ==============================
# 2. Pydantic Model for Row Validation
# ==============================
class CustomerRecord(BaseModel):
    customer_id: Optional[str]
    account_id: Optional[int]
    location_id: Optional[int]
    first_name: str
    last_name: str
    full_name: Optional[str]
    email: Optional[str]
    birth_date: Optional[datetime]
    phone_number: Optional[str]
    address_line1: Optional[str]
    address_line2: Optional[str]
    address_line3: Optional[str]
    city: Optional[str]
    country: Optional[str]
    state_province: Optional[str]
    postal_code: Optional[str]
    gender: Optional[str]
    date_joined: Optional[datetime]
    is_opted_in_to_sms: Optional[bool]
    completed_class_count: Optional[int]
    created_at: Optional[datetime]
    created_by: Optional[int]
    state_id: Optional[int]
    updated_at: Optional[datetime]
    updated_by: Optional[int]
    deleted_at: Optional[datetime]
    deleted_by: Optional[int]
    emailUnsubscribeHash: Optional[str]
    isSubscribedToEmail: Optional[bool]
    is_prospect: Optional[bool]
    is_company: Optional[bool]
    first_appointment_date: Optional[datetime]
    first_class_date: Optional[datetime]

    class Config:
        extra = "ignore"

# ==============================
# 3. Read Parquet Files from S3 into Polars
# ==============================
def read_parquet_from_s3(bucket: str, prefix: str) -> pl.DataFrame:
    """Read all Parquet files under an S3 prefix into a Polars DataFrame."""
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parquet_keys = [obj["Key"] for obj in objects.get("Contents", []) if obj["Key"].endswith(".parquet")]

    if not parquet_keys:
        raise RuntimeError("No Parquet files found in {}".format(prefix))

    dfs = []
    total_size = 0
    # Read each Parquet file and concatenate into a single DataFrame
    for key in parquet_keys:
        s3_path = f"s3://{bucket}/{key}"
        print(f"Reading: {s3_path}")
        df = pl.read_parquet(s3_path, use_pyarrow=True)
        # Format all datetime columns as string
        for col in df.columns:
            if df[col].dtype == pl.Datetime:
                df = df.with_columns(df[col].dt.strftime("%Y-%m-%d %H:%M:%S").alias(col))
        # Ensure columns are camelCase for DB
        rename_map = {
            "email_unsubscribe_hash": "emailUnsubscribeHash",
            "is_subscribed_to_email": "isSubscribedToEmail"
        }
        for old, new in rename_map.items():
            if old in df.columns:
                df = df.rename({old: new})
        dfs.append(df)
        total_size += df.shape[0]
    print(f"[INFO] Sum of sizes of all DataFrames before concatenation: {total_size}")
    df_all = pl.concat(dfs, how="vertical")
    print(f"[INFO] Total shape after concatenation: {df_all.shape}")
    return df_all

# ==============================
# 4. Process & Filter Data
# ==============================
def filter_and_validate(df: pl.DataFrame, valid_accounts: set, valid_states: set, account_id: int = None) -> List[dict]:
    if account_id is not None:
        # if account_id not in valid_accounts:
        #     raise ValueError(f"Provided account_id {account_id} is not in valid accounts.")
        print(f"Setting account_id={account_id} for all rows...")
        df = df.with_columns(pl.lit(account_id).alias("account_id"))
    valid_rows = []
    for record in df.to_dicts():
        try:
            cust = CustomerRecord(**record)
            # Parent FK checks
            # if cust.account_id not in valid_accounts:
            #     print(f"Skipping customer_id={cust.customer_id} - invalid account_id {cust.account_id}")
            #     continue
            # if cust.state_id is not None and cust.state_id not in valid_states:
            #     print(f"Skipping customer_id={cust.customer_id} - invalid state_id {cust.state_id}")
            #     continue
            valid_rows.append(cust.model_dump())
        except Exception as e:
            print(f"Validation error for customer_id={record.get('customer_id')}: {e}")
    return valid_rows


import io
import psycopg2

def bulk_insert_customers_csv(valid_customers, table_name="customers_rmtest"):
    if not valid_customers:
        print("No valid customers to insert.")
        return

    expected_columns = [
        'id',
        'customer_id',
        'location_id',
        'account_id',
        'first_name',
        'last_name',
        'email',
        'full_name',
        'birth_date',
        'phone_number',
        'address_line1',
        'address_line2',
        'address_line3',
        'city',
        'country',
        'state_province',
        'customer_state',
        'postal_code',
        'gender',
        'date_joined',
        'is_opted_in_to_sms',
        'completed_class_count',
        'state_id',
        'created_at',
        'created_by',
        'updated_at',
        'updated_by',
        'deleted_at',
        'deleted_by'
    ]

    def clean_customer_row(row, expected_columns):
        clean_row = {}
        for col in expected_columns:
            val = row.get(col, None)
            if val == "":
                val = None
            clean_row[col] = val
        return clean_row

    cleaned_customers = [clean_customer_row(row, expected_columns) for row in valid_customers]
    import polars as pl
    from datetime import datetime
    df = pl.DataFrame(cleaned_customers)
    # Fill nulls in created_at and updated_at with current timestamp
    now = datetime.now()
    if "created_at" in df.columns:
        df = df.with_columns(
            pl.col("created_at").fill_null(now)
        )
    if "updated_at" in df.columns:
        df = df.with_columns(
            pl.col("updated_at").fill_null(now)
        )
    csv_data = df.write_csv(None)  # returns string
    # Use csv_data directly with StringIO
    csv_buffer = io.StringIO(csv_data)

    # COPY to database
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    copy_sql = f"""COPY "{table_name}" ({','.join([f'"{col}"' for col in df.columns])}) FROM STDIN WITH (FORMAT csv, HEADER true)"""
    try:
        cursor.copy_expert(copy_sql, csv_buffer)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Copied {len(valid_customers)} rows into table {table_name}.")
    except Exception as e:
        print(f"Bulk insert failed for table {table_name}: {e}")
        import traceback
        traceback.print_exc()

# ==============================
# 5. Insert into DB in Chunks
# ==============================
def insert_customers(customers: List[dict], table_name: str = "customers"):
    if not customers:
        print("No valid customers to insert.")
        return

    import concurrent.futures
    total_chunks = math.ceil(len(customers) / BATCH_SIZE)
    sql = f"""
        INSERT INTO {table_name} (
            customer_id, account_id, location_id, first_name, last_name, full_name,
            email, birth_date, phone_number, address_line1, address_line2, address_line3,
            city, country, state_province, postal_code, gender, date_joined, is_opted_in_to_sms,
            completed_class_count, created_at, created_by, state_id, updated_at, updated_by,
            deleted_at, deleted_by, email_unsubscribe_hash, is_subscribed_to_email, is_prospect,
            is_company, first_appointment_date, first_class_date
        )
        VALUES (
            :customer_id, :account_id, :location_id, :first_name, :last_name, :full_name,
            :email, :birth_date, :phone_number, :address_line1, :address_line2, :address_line3,
            :city, :country, :state_province, :postal_code, :gender, :date_joined, :is_opted_in_to_sms,
            :completed_class_count, :created_at, :created_by, :state_id, :updated_at, :updated_by,
            :deleted_at, :deleted_by, :email_unsubscribe_hash, :is_subscribed_to_email, :is_prospect,
            :is_company, :first_appointment_date, :first_class_date
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
            chunk = customers[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
            futures.append(executor.submit(insert_chunk, chunk, i))
        concurrent.futures.wait(futures)


def main():
    print("Loading parent IDs...")
    valid_accounts, valid_states = load_parent_ids()

    print("Reading Parquet files from S3...")
    t0 = time.time()
    df = read_parquet_from_s3(S3_BUCKET, S3_PREFIX)
    t1 = time.time()
    print(f"Read Parquet files in {t1-t0:.2f}s")

    print("Validating and filtering...")
    account_id = 999999  # Change this to your desired account_id
    t2 = time.time()
    valid_customers = filter_and_validate(df, valid_accounts, valid_states, account_id=account_id)
    t3 = time.time()
    #print(f"Valid rows after FK check: {len(valid_customers)}. Filtered in {t3-t2:.2f}s")

    sample_csv_path = "customers_sample.csv"
    flat_columns = [col for col in df.columns if df[col].dtype not in [pl.List, pl.Struct]]
    df.select(flat_columns).head(1000).write_csv(sample_csv_path)

    print("Inserting into database...")
    table_name = "mt_customers_details_dlk"  
    t4 = time.time()
    #insert_customers(valid_customers, table_name=table_name)
    bulk_insert_customers_csv(valid_customers, table_name=table_name)
    t5 = time.time()
    print(f"Inserted all batches in {t5-t4:.2f}s")

    

if __name__ == "__main__":
    main()
