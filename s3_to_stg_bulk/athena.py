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

# There are two ways to address an S3 Tables catalog from Athena, and which one works
# depends on the workgroup and the caller's Lake Formation grants:
#
#   1. context Catalog=AwsDataCatalog + fully-qualified SQL
#        FROM "s3tablescatalog/revi-crm-data"."silver"."customers"
#      Used by reviDataInsightsAPI/revi-cloud-campaign on workgroup `primary`.
#
#   2. context Catalog=<child catalog> + Database=<namespace>, BARE table names in SQL
#        FROM "customers"
#      Used by reviDataInsightsAPI/revi-dlk-gold/src/gold_etl/compute.py on workgroup
#      `revi-dlk-gold`.
#
# We run on `revi-dlk-gold` with revi-dlk-gold-lambda-exec, which is exactly gold_etl's
# combination, so we follow (2). Form (1) fails here with
#     CATALOG_NOT_FOUND: Catalog 's3tablescatalog/revi-crm-data' does not exist
# for this role on this workgroup, even though it succeeds for a Lake Formation admin —
# which is what makes it easy to "verify" from a developer shell and still break in the
# Lambda. Change this only alongside the workgroup, and re-test as the Lambda role.


def client():
    return boto3.client("athena", region_name=settings.REGION)


def catalog() -> str:
    """
    The catalog name as the Athena API wants it — bare, no SQL quoting.

    Tolerates a value that already carries quotes, since an operator setting
    ATHENA_CATALOG in the console may well include them.
    """
    return settings.ATHENA_CATALOG.strip().strip('"')


def table_ref(table: str) -> str:
    """
    How a Silver table is named inside the SQL: bare, just `"customers"`.

    Catalog and namespace are supplied out-of-band via QueryExecutionContext — see the
    CONTEXT note above. Quoted anyway so a table name that collides with a reserved word
    still parses.
    """
    return f'"{table}"'


def ts_literal(dt) -> str:
    """Athena TIMESTAMP literal body — millisecond precision, matches timestamp(3)."""
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def sq(value: str) -> str:
    """Escape a single-quoted SQL string literal."""
    return str(value).replace("'", "''")


def preflight(table: str) -> None:
    """
    Cheap proof that Athena is usable before the caller commits to anything expensive.

    Exercises the whole chain the load depends on — workgroup, S3 Tables catalog, Lake
    Formation, and write access to the result location — with `SELECT 1 … LIMIT 0`, which
    plans and resolves the table but scans no data.

    Deliberately a SELECT and not `SHOW TABLES`: Athena parses DDL with a Hive-derived
    grammar whose name resolution differs from the Trino grammar SELECT uses, so a
    SHOW TABLES that passes proves nothing about whether the load's own reads will. Using
    the same statement shape as the real load means preflight cannot pass while the load
    fails.

    The point is ordering — the run slot and 13 staging tables are created after this, so
    a misconfiguration surfaces without leaving the slot held.
    """
    run_query(f"SELECT 1 FROM {table_ref(table)} LIMIT 0", label="preflight")
    logger.info(
        f"[athena:preflight] ok — catalog={catalog()!r} "
        f"namespace={settings.SILVER_NAMESPACE!r} workgroup={settings.ATHENA_WORKGROUP!r}"
    )


def run_query(query: str, label: str) -> str:
    """
    Execute a query and return its QueryExecutionId once it has SUCCEEDED.
    Raises on FAILED / CANCELLED or on timeout.
    """
    # ATHENA_OUTPUT_LOCATION is a required setting, so a missing value fails at import —
    # no runtime guard needed here.
    athena = client()
    qid = athena.start_query_execution(
        QueryString=query,
        WorkGroup=settings.ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": settings.ATHENA_OUTPUT_LOCATION},
        QueryExecutionContext={
            "Catalog":  catalog(),                    # "s3tablescatalog/revi-crm-data"
            "Database": settings.SILVER_NAMESPACE,    # "silver"
        },
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
    """
    (bucket, key) of the result CSV for a completed query, read back from Athena.

    Deliberately not computed from ATHENA_OUTPUT_LOCATION + qid. A workgroup with
    EnforceWorkGroupConfiguration=true overrides the location we asked for, and several
    workgroups in this account do exactly that (revi-dlk-workgroup, revi-dlk-gold) — a
    computed path would then point at a key that does not exist and fail with a
    confusing 404. Athena reports where it actually wrote.
    """
    execution = client().get_query_execution(QueryExecutionId=qid)["QueryExecution"]
    uri = execution["ResultConfiguration"]["OutputLocation"]
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def result_body(qid: str):
    """Streaming body of the result CSV — never read whole into memory."""
    bucket, key = result_location(qid)
    return boto3.client("s3", region_name=settings.REGION).get_object(
        Bucket=bucket, Key=key
    )["Body"]
