"""
Id-batch sync orchestrator — fetch exactly the records we need, by id.

The resources this serves (credit_transactions, membership_transactions) used to be
downloaded whole per tenant and filtered client-side by customer_id. Onboarding one
location of a 50-location tenant therefore pulled roughly 50x what it kept. But we already
know which transactions matter: order_lines and reservations are location-scoped, already
downloaded, and both carry credit_transactions_id and membership_transactions_id. So the
id set is a by-product of work already done, and the fetch can ask for precisely it.

An earlier attempt at the same idea sent 100 customer ids per call as repeated `&user=`
and was abandoned because the endpoint returned nothing once the query string got long
enough (see crm_sync/config.py). This sends ONE comma-joined `filter[id]` parameter
instead — about 800 characters for 100 seven-digit ids — which is the shape MarianaTek
actually supports.

fetch_by_ids_fn signature (provided by the resource module):
  async (ids, account_id, api_base_url, location_id) -> (rows: list[dict], resp: dict)
"""

import logging
import time

from core.config import settings
from utils.s3_writer import write_parquet_to_s3

logger = logging.getLogger(__name__)

def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


async def run(
    resource: str,
    fetch_by_ids_fn,
    ids: list,
    account_id: str,
    location_id: int,
    api_base_url: str,
    s3_prefix: str,
    shard_tag: str = None,
    batch_size: int = None,
    parquet_batch_size: int = None,
) -> tuple[int, int]:
    """
    Fetch `ids` in chunks via filter[id], write the mapped rows to S3.
    Returns (total_processed, total_fetched).
    """
    tag = resource.upper()
    start_time = time.time()
    # settings.MAX_IDS, not a module constant — one env var controls it everywhere.
    batch_size = batch_size or int(getattr(settings, "MAX_IDS", 200))
    if parquet_batch_size is None:
        parquet_batch_size = int(settings.PARQUET_BATCH_SIZE)

    ids = [str(i) for i in ids if i is not None and str(i) != ""]
    if not ids:
        logger.info(f"[{tag}] no ids in this slice — nothing to fetch")
        return 0, 0

    logger.info(
        f"[{tag}] fetching {len(ids)} ids in {-(-len(ids) // batch_size)} requests "
        f"of {batch_size}"
    )

    rows = []
    total_fetched = 0

    for n, chunk in enumerate(_chunks(ids, batch_size), start=1):
        chunk_rows, resp = await fetch_by_ids_fn(chunk, account_id, api_base_url, location_id)
        # `or []` not a .get default — a filter[id] response can carry an explicit null for
        # keys a paginated response fills in, and the default only applies to a MISSING key.
        returned = len(resp.get("data") or [])
        total_fetched += returned

        # The filter is the whole optimisation, so prove it was applied. A silently ignored
        # filter[id] returns a full page instead of at most one record per requested id —
        # the request would succeed, the data would look plausible, and we would quietly be
        # back to downloading the entire tenant. Fail on the first chunk instead.
        if returned > len(chunk):
            raise RuntimeError(
                f"{resource}: filter[id] appears to be ignored by this endpoint — asked for "
                f"{len(chunk)} ids and got {returned} records back. Refusing to continue; "
                f"this would silently fetch the whole tenant."
            )

        rows.extend(chunk_rows)
        if n % 20 == 0:
            logger.info(f"[{tag}] {n} requests done, {len(rows)} rows so far")

    # Ids we asked for and did not get back. Expected in small numbers — a transaction
    # deleted in the CRM but still referenced by an order_line — and worth seeing, because a
    # large count means the id source is wrong rather than the data being sparse.
    missing = len(ids) - total_fetched
    if missing > 0:
        logger.warning(
            f"[{tag}] {missing} of {len(ids)} requested ids returned no record "
            f"(deleted in the CRM, or referenced but never created)"
        )

    file_num = 1
    total_processed = 0
    for chunk in _chunks(rows, parquet_batch_size):
        if not chunk:
            continue
        await write_parquet_to_s3(
            chunk, shard_tag or str(location_id), file_num, account_id,
            s3_prefix=s3_prefix, entity_type=resource,
        )
        total_processed += len(chunk)
        file_num += 1

    elapsed = time.time() - start_time
    logger.info(
        f"[{tag}] DONE: requested={len(ids)}, fetched={total_fetched}, "
        f"written={total_processed}, elapsed={elapsed:.2f}s"
    )
    return total_processed, total_fetched
