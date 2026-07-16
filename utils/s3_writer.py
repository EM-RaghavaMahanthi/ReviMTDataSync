import boto3
# Utility to refresh (delete) a log file for a resource in S3

# utils/s3_writer.py
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from core.config import settings
import logging
import os
from aiobotocore.session import get_session

logger = logging.getLogger(__name__)

region = os.environ.get("AWS_REGION", "us-east-1")

async def write_parquet_to_s3(
    data, entity_id: str, batch_num: int, account_id: str,
    compression: str = "snappy", s3_prefix: str = None, entity_type: str = "location"
):
    if not data:
        logger.info(f"[S3] No data to write for {entity_type}={entity_id}, batch={batch_num}, account_id={account_id}")
        return None

    logger.info(f"[S3] Processing batch {batch_num} for {entity_type}={entity_id}, account_id={account_id} with {len(data)} records")

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression=compression)
    buffer.seek(0)

    key_part = f"account_id_{account_id}/{entity_type}_{entity_id}_batch_{batch_num}.parquet"
    if s3_prefix:
        key = f"{s3_prefix}/{key_part}"
    else:
        key = f"{settings.S3_PREFIX}/{key_part}"

    logger.info(f"[S3] Saving parquet to path: s3://{settings.S3_BUCKET}/{key}")

    session = get_session()
    async with session.create_client("s3", region_name=region) as s3_client:
        await s3_client.put_object(Bucket=settings.S3_BUCKET, Key=key, Body=buffer.getvalue())

    logger.info(f"[S3] Uploaded {len(data)} records for {entity_type}={entity_id}, account_id={account_id} to s3://{settings.S3_BUCKET}/{key}")
    return key


async def delete_account_prefix(account_id: str, s3_prefix: str) -> int:
    """
    Delete every existing object under {s3_prefix}/account_id_{account_id}/ before a
    fresh sync, so stale parquet from a previous run can't linger and get re-ingested
    by the downstream S3->staging step.

    The prefix is built identically to write_parquet_to_s3's key, so it matches exactly
    whatever that function wrote (including the trailing-slash quirk in some s3_prefix
    values). Returns the number of objects deleted.
    """
    prefix = f"{s3_prefix}/account_id_{account_id}/"
    deleted = 0
    session = get_session()
    async with session.create_client("s3", region_name=region) as s3_client:
        paginator = s3_client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=settings.S3_BUCKET, Prefix=prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue
            # list_objects_v2 returns <=1000 keys/page, within delete_objects' 1000 limit
            await s3_client.delete_objects(
                Bucket=settings.S3_BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in contents], "Quiet": True},
            )
            deleted += len(contents)

    logger.info(f"[S3] Cleared {deleted} stale object(s) under s3://{settings.S3_BUCKET}/{prefix}")
    return deleted


async def read_customer_ids_from_s3(account_id: str, customers_prefix: str = None) -> list:
    """
    Read customer_id values from the customers parquet written earlier in this run.
    Files live at: {customers_prefix}/account_id_{account_id}/location_*_batch_*.parquet
    Used to seed user-based resource syncs without re-hitting the customers API.
    Reads only the 'customer_id' column to avoid loading full row groups.
    """
    if customers_prefix is None:
        customers_prefix = settings.S3_PREFIXES["customers"]
    prefix = f"{customers_prefix}/account_id_{account_id}/"

    session = get_session()
    customer_ids: list = []
    async with session.create_client("s3", region_name=region) as s3_client:
        paginator = s3_client.get_paginator("list_objects_v2")
        keys = []
        async for page in paginator.paginate(Bucket=settings.S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])

        for key in keys:
            resp = await s3_client.get_object(Bucket=settings.S3_BUCKET, Key=key)
            body = await resp["Body"].read()
            table = pq.read_table(io.BytesIO(body), columns=["customer_id"])
            customer_ids.extend(str(v) for v in table.column("customer_id").to_pylist() if v)

    logger.info(f"[S3] Read {len(customer_ids)} customer_ids from s3://{settings.S3_BUCKET}/{prefix}")
    return customer_ids


def refresh_source(name):
    """
    Deletes the log file (name.log) for a resource in S3.
    Usage: refresh_source('customers-details') will delete customers-details/customer.log
    """
    log_bucket = getattr(settings, "LOG_BUCKET", settings.S3_BUCKET)
    log_key = f"{name}/customer.log"
    s3_client = boto3.client("s3")
    try:
        s3_client.delete_object(Bucket=log_bucket, Key=log_key)
        logger.info(f"[LOG] Deleted old {log_key} from s3://{log_bucket}/{log_key}")
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"[LOG] No log file found to delete at s3://{log_bucket}/{log_key}")
        pass