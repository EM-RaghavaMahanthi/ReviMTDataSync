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