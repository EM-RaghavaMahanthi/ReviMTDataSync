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

async def write_parquet_to_s3_crm(
    data, 
    domain: str, 
    table_name: str, 
    batch_id: str, 
    start_page: int, 
    end_page: int,
    compression: str = "snappy", 
    s3_prefix: str = None
):
    """
    Write parquet data to S3 with CRM-specific naming convention.
    
    Args:
        data: List of records to write
        domain: Domain name (e.g., 'reformedpilates')
        table_name: Name of the table (e.g., 'customers', 'orders')
        batch_id: Unique batch identifier
        start_page: Starting page number for this batch
        end_page: Ending page number for this batch
        compression: Compression type for parquet
        s3_prefix: Optional S3 prefix override
        
    Returns:
        str: S3 key where data was written, or None if no data
    """
    if not data:
        logger.info(f"[S3 CRM] No data to write for domain={domain}, table={table_name}, batch={batch_id}")
        return None

    logger.info(f"[S3 CRM] Processing batch {batch_id} for domain={domain}, table={table_name} with {len(data)} records (pages {start_page}-{end_page})")

    # Convert data to parquet
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression=compression)
    buffer.seek(0)

    # Create filename with CRM-specific naming convention
    # Format: {domain}-{table_name}-batch:{batch_id}:start_page:{start_page}-end_page:{end_page}.parquet
    filename = f"{domain}-{table_name}-batch:{batch_id}:start_page:{start_page}-end_page:{end_page}.parquet"
    
    # Create S3 key with CRM structure: prefix/domain/table_name/filename
    if s3_prefix:
        # Even with custom prefix, maintain domain/table_name structure
        #key = f"{s3_prefix}/{domain}/{table_name}/{filename}"
        key = f"{s3_prefix}/{table_name}/{domain}/{filename}"
    else:
        # Use default CRM prefix structure with domain folder
        default_prefix = getattr(settings, "S3_CRM_PREFIX", "raw-data")
        key = f"{default_prefix}/{table_name}/{domain}/{filename}"
        #key = f"{default_prefix}/{domain}/{table_name}/{filename}"

   

    logger.info(f"[S3 CRM] Saving parquet to path: s3://{settings.S3_BUCKET}/{key}")

    # Upload to S3
    session = get_session()
    async with session.create_client("s3", region_name=region) as s3_client:
        await s3_client.put_object(Bucket=settings.S3_BUCKET, Key=key, Body=buffer.getvalue())

    logger.info(f"[S3 CRM] Uploaded {len(data)} records for domain={domain}, table={table_name}, batch={batch_id} (pages {start_page}-{end_page}) to s3://{settings.S3_BUCKET}/{key}")
    return key