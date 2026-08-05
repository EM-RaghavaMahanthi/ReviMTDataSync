import boto3
# Utility to refresh (delete) a log file for a resource in S3

# utils/s3_writer.py
import io
import json
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


async def read_distinct_ids_from_s3(account_id: str, sources: list) -> list:
    """
    Distinct, non-NULL ids across one or more (s3_prefix, column) parquet sources.

    Used by the id_batch planner: `credit_transactions` is the union of
    order_lines.credit_transactions_id and reservations.credit_transactions_id, so the
    fetch asks for exactly the transactions this location references.

    Reads only the one column per file, like read_customer_ids_from_s3 — the parquet row
    groups here are wide and there is no reason to pull them.
    """
    session = get_session()
    ids: set = set()

    async with session.create_client("s3", region_name=region) as s3_client:
        paginator = s3_client.get_paginator("list_objects_v2")
        for s3_prefix, column in sources:
            prefix = f"{s3_prefix}/account_id_{account_id}/"
            keys = []
            async for page in paginator.paginate(Bucket=settings.S3_BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        keys.append(obj["Key"])

            before = len(ids)
            for key in keys:
                resp = await s3_client.get_object(Bucket=settings.S3_BUCKET, Key=key)
                body = await resp["Body"].read()
                table = pq.read_table(io.BytesIO(body), columns=[column])
                ids.update(
                    str(v) for v in table.column(column).to_pylist()
                    if v is not None and str(v) != ""
                )
            logger.info(
                f"[S3] {s3_prefix}.{column}: {len(keys)} files, "
                f"+{len(ids) - before} new ids (running total {len(ids)})"
            )

    # Sorted so shard slices are stable across re-planning — a re-run of one shard fetches
    # the same ids it did the first time.
    return sorted(ids, key=lambda x: (len(x), x))


# Prefix for the id lists the id_batch planner writes. Not one of settings.S3_PREFIXES —
# those are parquet datasets consumed downstream; this is planner scratch. Named here so the
# writer and the stale-clear in handlers/crm_to_s3.py cannot drift apart.
ID_LIST_PREFIX = "_idlists"


def _id_list_key(account_id: str, location_id, resource: str) -> str:
    # Scoped by location as well as account: one execution processes several
    # (account, location) pairs in sequence, and a half-finished run is much easier to read
    # when each location's list is its own object.
    return f"{ID_LIST_PREFIX}/account_id_{account_id}/location_{location_id}/{resource}.json"


async def write_id_list_to_s3(account_id: str, location_id, resource: str, ids: list) -> str:
    """
    Persist a resource's id list so shards read a slice instead of receiving it inline.

    20k ids is ~156KB and 200k is ~1.5MB, against a 256KB Step Functions payload limit — so
    the list cannot travel through the state machine. It also makes a failed shard
    re-runnable on its own, since the list is still here rather than needing recomputation
    from the parquets.
    """
    key = _id_list_key(account_id, location_id, resource)
    body = json.dumps(ids).encode()
    session = get_session()
    async with session.create_client("s3", region_name=region) as s3_client:
        await s3_client.put_object(Bucket=settings.S3_BUCKET, Key=key, Body=body)
    logger.info(f"[S3] wrote {len(ids)} {resource} ids to s3://{settings.S3_BUCKET}/{key}")
    return key


async def read_id_list_from_s3(
    account_id: str, location_id, resource: str, offset: int = 0, limit: int = None
) -> list:
    """One shard's slice of the id list written by write_id_list_to_s3."""
    key = _id_list_key(account_id, location_id, resource)
    session = get_session()
    async with session.create_client("s3", region_name=region) as s3_client:
        resp = await s3_client.get_object(Bucket=settings.S3_BUCKET, Key=key)
        ids = json.loads(await resp["Body"].read())
    sliced = ids[offset:] if limit is None else ids[offset:offset + int(limit)]
    logger.info(
        f"[S3] read {len(sliced)} {resource} ids "
        f"(offset={offset}, limit={limit}, total={len(ids)})"
    )
    return sliced


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