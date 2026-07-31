"""
Athena access for the Silver reads — start a query, poll it to completion, and hand back
where its result CSV landed in S3.

Deliberately thin: nothing here materialises a result set. staging.py streams the result
CSV straight from S3 into COPY, because a day-wide delta over credit_transactions is
hundreds of MB and building it as a Python list would OOM the Lambda.
"""

import logging
import time
from urllib.parse import urlparse

import boto3

from core.bulk_config import settings

logger = logging.getLogger(__name__)

_TERMINAL_FAILED = ("FAILED", "CANCELLED")


def client():
    return boto3.client("athena", region_name=settings.REGION)


def fqn(table: str) -> str:
    """Catalog-qualified Silver table reference."""
    return f'{settings.ATHENA_CATALOG}."{settings.SILVER_NAMESPACE}"."{table}"'


def ts_literal(dt) -> str:
    """Athena TIMESTAMP literal body — millisecond precision, matches timestamp(3)."""
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def sq(value: str) -> str:
    """Escape a single-quoted SQL string literal."""
    return str(value).replace("'", "''")


def run_query(query: str, label: str) -> str:
    """
    Execute a query and return its QueryExecutionId once it has SUCCEEDED.
    Raises on FAILED / CANCELLED or on timeout.
    """
    if not settings.ATHENA_OUTPUT_LOCATION:
        raise RuntimeError("ATHENA_OUTPUT_LOCATION is not set")

    athena = client()
    qid = athena.start_query_execution(
        QueryString=query,
        WorkGroup=settings.ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": settings.ATHENA_OUTPUT_LOCATION},
        QueryExecutionContext={"Catalog": settings.ATHENA_CATALOG},
    )["QueryExecutionId"]

    logger.info(f"[athena:{label}] started qid={qid}")
    _wait(athena, qid, label)
    return qid


def _wait(athena, qid: str, label: str) -> None:
    """Poll to completion with backoff, capped at ATHENA_TIMEOUT_SECONDS."""
    deadline = time.time() + settings.ATHENA_TIMEOUT_SECONDS
    delay = 0.5

    while True:
        execution = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = execution["Status"]["State"]

        if state == "SUCCEEDED":
            stats = execution.get("Statistics", {})
            logger.info(
                f"[athena:{label}] succeeded qid={qid} "
                f"scanned={stats.get('DataScannedInBytes', 0)}B "
                f"runtime={stats.get('TotalExecutionTimeInMillis', 0)}ms"
            )
            return

        if state in _TERMINAL_FAILED:
            reason = execution["Status"].get("StateChangeReason", "no reason given")
            raise RuntimeError(f"[athena:{label}] query {state}: {reason}")

        if time.time() > deadline:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(
                f"[athena:{label}] qid={qid} still {state} after "
                f"{settings.ATHENA_TIMEOUT_SECONDS}s — cancelled"
            )

        time.sleep(delay)
        delay = min(delay * 1.5, 5.0)


def result_location(qid: str) -> tuple:
    """(bucket, key) of the result CSV for a completed query."""
    parsed = urlparse(settings.ATHENA_OUTPUT_LOCATION)
    prefix = parsed.path.lstrip("/").rstrip("/")
    key = f"{prefix}/{qid}.csv" if prefix else f"{qid}.csv"
    return parsed.netloc, key


def result_body(qid: str):
    """Streaming body of the result CSV — never read whole into memory."""
    bucket, key = result_location(qid)
    return boto3.client("s3", region_name=settings.REGION).get_object(
        Bucket=bucket, Key=key
    )["Body"]
