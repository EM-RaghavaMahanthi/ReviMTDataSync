import json
import asyncio
import time
import re
import importlib
import logging
from typing import List
from core.config import settings
from core.logger import setup_logging
import utils.api_client as _api_client

setup_logging()
logger = logging.getLogger(__name__)

VALID_RESOURCES = (
    "customers",
    "orders",
    "order_lines",
    "class_sessions",
    "reservations",
    "credit_transactions",
    "membership_instances",
    "membership_transactions",
)



# ---------------------------------------------------------------------------
# Input parsing & validation
# ---------------------------------------------------------------------------

def get_event_body(event: dict) -> dict:
    """Normalize Lambda event: handles Postman/Function URL and AWS console test formats."""
    if not isinstance(event, dict):
        return {}
    if isinstance(event.get("body"), str):
        try:
            return json.loads(event["body"])
        except json.JSONDecodeError:
            return {}
    return event


_URL_RE = re.compile(
    r"^https?://"
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|localhost|\d{1,3}(?:\.\d{1,3}){3})"
    r"(?::\d+)?(?:/?|[/?]\S+)$",
    re.IGNORECASE,
)


def validate_params(account_id, api_base_url, location_id) -> dict | None:
    """Returns an error response dict if invalid, else None."""
    if not account_id:
        return {"statusCode": 400, "body": json.dumps("Missing account_id in event.")}
    if not api_base_url:
        return {"statusCode": 400, "body": json.dumps("Missing api_base_url in event.")}
    if not location_id:
        return {"statusCode": 400, "body": json.dumps("Missing location_id in event.")}
    if not _URL_RE.match(api_base_url):
        return {"statusCode": 400, "body": json.dumps(f"Invalid api_base_url format: {api_base_url}")}
    if not str(location_id).strip():
        return {"statusCode": 400, "body": json.dumps("Invalid location_id: cannot be empty.")}
    return None


def get_account_config(account_id: str) -> dict | None:
    """
    Fetch integration_id and crm_api_end_point for the given account_id from DB.
    Returns dict with those fields, or None if account not found.
    """
    from clients.db_client import DatabaseManager, Account as DBAccount
    db = DatabaseManager()
    try:
        with db.get_session() as session:
            result = session.query(
                DBAccount.integration_id,
                DBAccount.crm_api_end_point,
            ).filter(DBAccount.id == int(account_id)).first()
            if not result:
                return None
            return {
                "integration_id": result.integration_id,
                "crm_api_end_point": result.crm_api_end_point,
            }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# User ID collection (seed for user-based syncs)
# ---------------------------------------------------------------------------

async def get_user_ids_from_customers(account_id: str, api_base_url: str, location_id: str) -> List[str]:
    from crm_sync.customers import process_customers_for_location
    logger.info(f"[get_user_ids] Fetching user_ids via customers for account={account_id}, location={location_id}")
    _, _, user_ids = await process_customers_for_location(location_id, account_id, api_base_url, save_to_s3=False)
    logger.info(f"[get_user_ids] account={account_id}: {len(user_ids)} user_ids found")
    return user_ids


# ---------------------------------------------------------------------------
# Resource processors
# ---------------------------------------------------------------------------

async def _process_location_resource(resource: str, location_id: str, account_id: str, api_base_url: str):
    """Dispatch for location-keyed resources."""
    if resource == "customers":
        from crm_sync.customers import process_customers_for_location
        processed, expected, _ = await process_customers_for_location(location_id, account_id, api_base_url)
        return {"location": location_id, "status": "success", "records": processed, "expected": expected}

    if resource == "orders":
        from crm_sync.orders import process_orders_for_location
        processed, expected = await process_orders_for_location(location_id, account_id, api_base_url)
        return {"location": location_id, "status": "success", "records": processed, "expected": expected}

    if resource == "order_lines":
        from crm_sync.order_lines import process_order_lines_for_location
        processed, expected = await process_order_lines_for_location(location_id, account_id, api_base_url)
        return {"location": location_id, "status": "success", "records": processed, "expected": expected}

    if resource == "class_sessions":
        from crm_sync.class_sessions import process_class_sessions_for_location
        processed, expected = await process_class_sessions_for_location(location_id, account_id, api_base_url)
        return {"location": location_id, "status": "success", "records": processed, "expected": expected}

    if resource == "reservations":
        from crm_sync.reservations import process_reservations_for_entity
        processed, expected = await process_reservations_for_entity(location_id, account_id, api_base_url)
        return {"location": location_id, "status": "success", "records": processed, "expected": expected}

    raise ValueError(f"Unknown location resource: {resource}")



async def _process_user_batch_resource(resource: str, event_body: dict, context, account_id: str, api_base_url: str, location_id: str):
    """
    Handles credit_transactions, membership_instances, membership_transactions.
    If user_ids are in the event (child lambda call), processes them directly.
    Otherwise fetches all user_ids from customers and fans out to child lambdas.
    """
    import boto3
    import itertools

    BATCH_FN = {
        "credit_transactions": "crm_sync.credit_transactions.process_credit_transactions_batch",
        "membership_instances": "crm_sync.membership_instances.process_membership_instances_batch",
        "membership_transactions": "crm_sync.membership_transactions.process_membership_transactions_batch",
    }

    user_ids = event_body.get("user_ids")
    batch_id = event_body.get("batch_id", 1)

    # --- Child lambda path: process a specific batch of user_ids ---
    if user_ids:
        module_path, fn_name = BATCH_FN[resource].rsplit(".", 1)
        import importlib
        mod = importlib.import_module(module_path)
        process_fn = getattr(mod, fn_name)
        try:
            logger.info(f"[{resource}] batch={batch_id} account={account_id}: processing {len(user_ids)} users")
            processed, expected = await process_fn(user_ids, batch_id, account_id, api_base_url, location_id)
            return [{"batch_id": batch_id, "users_count": len(user_ids), "status": "success", "records": processed, "expected": expected}]
        except Exception as e:
            logger.error(f"[{resource}] batch={batch_id} account={account_id} failed: {e}")
            return [{"batch_id": batch_id, "users_count": len(user_ids), "status": "failed", "error": str(e)}]

    # --- Parent lambda path: get all users, fan out to child lambdas ---
    all_user_ids = await get_user_ids_from_customers(account_id, api_base_url, location_id)
    logger.info(f"[{resource}] account={account_id}: {len(all_user_ids)} users, fanning out to child lambdas")

    batch_size = getattr(settings, "API_BATCH_SIZE", 1000)
    lambda_client = boto3.client("lambda")

    def chunked(iterable, size):
        it = iter(iterable)
        for first in it:
            yield [first] + list(itertools.islice(it, size - 1))

    async def invoke_child(batch, batch_number):
        payload = json.dumps({
            "resource": resource,
            "account_id": account_id,
            "api_base_url": api_base_url,
            "location_id": location_id,
            "user_ids": batch,
            "batch_id": batch_number,
        })
        try:
            response = await asyncio.to_thread(
                lambda_client.invoke,
                FunctionName=context.function_name,
                InvocationType="RequestResponse",
                Payload=payload,
            )
            result = json.loads(response["Payload"].read())
            if result.get("statusCode") == 200:
                body = result["body"]
                child_body = json.loads(body) if isinstance(body, str) else body
                return child_body.get("results", [{"batch_id": batch_number, "users_count": len(batch), "status": "success"}])
            return [{"batch_id": batch_number, "users_count": len(batch), "status": "failed",
                     "error": f"Child lambda status {result.get('statusCode')}"}]
        except Exception as e:
            return [{"batch_id": batch_number, "users_count": len(batch), "status": "failed", "error": str(e)}]

    tasks = [invoke_child(batch, i + 1) for i, batch in enumerate(chunked(all_user_ids, batch_size))]
    batch_results = await asyncio.gather(*tasks)
    return [item for sublist in batch_results for item in sublist]


# ---------------------------------------------------------------------------
# Step Functions shard-mode handlers (Stage 1: Plan -> LocationMap -> UserMap)
# ---------------------------------------------------------------------------

async def _handle_plan(body: dict) -> dict:
    """
    mode=plan — clear stale S3 for this account (once), probe each resource's page
    count and return fixed page-range shard lists for LocationMap and UserMap.
    """
    account_id = body["account_id"]
    location_id = body["location_id"]
    api_base_url = body["api_base_url"]

    # One-time stale-clear for every resource prefix — shards only append afterwards,
    # so clearing here (not per-shard) prevents shards from wiping each other's output.
    from utils.s3_writer import delete_account_prefix
    for resource, prefix in settings.S3_PREFIXES.items():
        if prefix:
            await delete_account_prefix(account_id, prefix)

    from crm_sync.state import plan_location_shards, plan_user_shards
    location_shards, user_shards = await asyncio.gather(
        plan_location_shards(account_id, location_id, api_base_url),
        plan_user_shards(account_id, location_id, api_base_url),
    )
    logger.info(f"[plan] account={account_id}: {len(location_shards)} location + {len(user_shards)} user shards")
    return {
        "status": "success",
        "account_id": account_id,
        "location_id": location_id,
        "api_base_url": api_base_url,
        "location_shards": location_shards,
        "user_shards": user_shards,
        "shard_count": len(location_shards) + len(user_shards),
    }


# Notes/tags backfill: fetch ONLY these CRM resources (customers is needed for the tag
# assignments and for the customer_ids that filter notes). Dedicated to the backfill
# state machine — the main `plan` above is untouched.
_BACKFILL_LOCATION_TABLES = ["customers"]
_BACKFILL_USER_TABLES = ["user_notes", "user_tags"]


async def _handle_backfill_plan(body: dict) -> dict:
    """
    mode=backfill_plan — like plan, but scoped to customers + user_notes + user_tags,
    so old accounts can be backfilled with just tags & notes (no orders/reservations/etc).
    """
    account_id = body["account_id"]
    location_id = body["location_id"]
    api_base_url = body["api_base_url"]

    # Scoped stale-clear: only the datasets this backfill writes (customers + its
    # customer_tags side output + user_notes + user_tags).
    from utils.s3_writer import delete_account_prefix
    clear_keys = set(_BACKFILL_LOCATION_TABLES) | set(_BACKFILL_USER_TABLES) | {"customer_tags"}
    for key in clear_keys:
        prefix = settings.S3_PREFIXES.get(key)
        if prefix:
            await delete_account_prefix(account_id, prefix)

    from crm_sync.state import plan_location_shards, plan_user_shards
    location_shards, user_shards = await asyncio.gather(
        plan_location_shards(account_id, location_id, api_base_url, tables=_BACKFILL_LOCATION_TABLES),
        plan_user_shards(account_id, location_id, api_base_url, tables=_BACKFILL_USER_TABLES),
    )
    logger.info(f"[backfill_plan] account={account_id}: {len(location_shards)} location + {len(user_shards)} user shards")
    return {
        "status": "success",
        "account_id": account_id,
        "location_id": location_id,
        "api_base_url": api_base_url,
        "location_shards": location_shards,
        "user_shards": user_shards,
        "shard_count": len(location_shards) + len(user_shards),
    }


async def _handle_location_shard(body: dict) -> dict:
    """mode=location_shard — fetch a fixed page range for one location resource."""
    from crm_sync._base import location_sync
    resource = body["resource"]
    account_id = body["account_id"]
    location_id = body["location_id"]
    api_base_url = body["api_base_url"]
    page_start = int(body["page_start"])
    page_end = int(body["page_end"])

    mod = importlib.import_module(f"crm_sync.{resource}")
    # A resource may declare a side output (e.g. customers → customer_tags assignments)
    # that is derived from the same fetch — attach it if present.
    side_kwargs = {}
    if getattr(mod, "SIDE_MAP_FN", None) is not None:
        side_kwargs = {
            "side_map_fn": mod.SIDE_MAP_FN,
            "side_s3_prefix": mod.side_s3_prefix(),
            "side_entity_type": getattr(mod, "SIDE_ENTITY_TYPE", "side"),
        }
    processed, expected = await location_sync.run(
        location_id, account_id, api_base_url,
        fetch_page_fn=mod.fetch_page,
        s3_prefix=settings.S3_PREFIXES[resource],
        resource=resource,
        page_start=page_start,
        page_end=page_end,
        **side_kwargs,
    )
    return {"status": "success", "resource": resource, "page_start": page_start,
            "page_end": page_end, "records": processed}


async def _handle_tenant_shard(body: dict) -> dict:
    """mode=tenant_shard — tiny tenant-wide lookup (user_tags); one shard, fetch all pages."""
    resource = body["resource"]
    account_id = body["account_id"]
    location_id = body["location_id"]
    api_base_url = body["api_base_url"]

    mod = importlib.import_module(f"crm_sync.{resource}")
    processed, expected = await mod.process_tenant(account_id, location_id, api_base_url)
    return {"status": "success", "resource": resource, "records": processed}


async def _handle_user_shard(body: dict) -> dict:
    """mode=user_shard — unfiltered page range for membership_instances, filtered by customer_ids."""
    from utils.s3_writer import read_customer_ids_from_s3
    resource = body["resource"]
    account_id = body["account_id"]
    location_id = body["location_id"]
    api_base_url = body["api_base_url"]
    page_start = int(body["page_start"])
    page_end = int(body["page_end"])

    customer_ids = await read_customer_ids_from_s3(account_id)
    mod = importlib.import_module(f"crm_sync.{resource}")
    processed, fetched = await mod.process_unfiltered_shard(
        account_id, location_id, api_base_url, customer_ids, page_start, page_end
    )
    return {"status": "success", "resource": resource, "page_start": page_start,
            "page_end": page_end, "records": processed, "fetched": fetched}


async def _handle_plan_transactions(body: dict) -> dict:
    """
    mode=plan_transactions — phase 2 of planning, run AFTER Stage1_LocationMap.

    Collects the transaction ids this location actually references from the order_lines and
    reservations parquet the location phase just wrote, persists each resource's id list to
    S3, and returns offset/limit shards over it. Only the shards travel through the state
    machine; the ids stay in S3.
    """
    account_id = body["account_id"]
    location_id = body["location_id"]
    api_base_url = body["api_base_url"]

    from crm_sync.state import plan_transaction_shards
    transaction_shards = await plan_transaction_shards(
        account_id, location_id, api_base_url, tables=body.get("tables")
    )
    logger.info(f"[plan_transactions] account={account_id}: {len(transaction_shards)} shards")
    return {
        "status": "success",
        "account_id": account_id,
        "location_id": location_id,
        "api_base_url": api_base_url,
        "transaction_shards": transaction_shards,
        "shard_count": len(transaction_shards),
    }


async def _handle_id_batch_shard(body: dict) -> dict:
    """
    mode=id_batch_shard — one slice of a resource's id list, fetched via filter[id].

    Replaces the whole-tenant download these resources used to do. The slice is read from
    S3 rather than passed inline, so the payload stays small and a failed shard can be
    re-run on its own.
    """
    from utils.s3_writer import read_id_list_from_s3
    resource = body["resource"]
    account_id = body["account_id"]
    location_id = body["location_id"]
    api_base_url = body["api_base_url"]
    id_offset = int(body.get("id_offset", 0))
    id_limit = body.get("id_limit")

    ids = await read_id_list_from_s3(account_id, location_id, resource, id_offset, id_limit)
    if not ids:
        logger.info(f"[id_batch_shard] {resource} offset={id_offset}: empty slice — skipping")
        return {"status": "success", "resource": resource, "records": 0, "fetched": 0,
                "id_offset": id_offset, "ids": 0}

    # Shard-unique S3 tag so slices of the same resource cannot collide on S3 keys,
    # mirroring the "_ub_" tag the user-batch path uses.
    shard_tag = f"{location_id}_ib_{id_offset}"
    mod = importlib.import_module(f"crm_sync.{resource}")
    processed, fetched = await mod.process_id_shard(
        ids, account_id, location_id, api_base_url, shard_tag=shard_tag
    )
    return {"status": "success", "resource": resource, "records": processed,
            "fetched": fetched, "id_offset": id_offset, "ids": len(ids)}


async def _handle_user_batch_shard(body: dict) -> dict:
    """
    mode=user_batch_shard — a slice of customers (user_offset..user_offset+user_limit),
    100 ids/call via repeated &user=. The slice keeps each shard bounded (~batches/pages).
    """
    from utils.s3_writer import read_customer_ids_from_s3
    resource = body["resource"]
    account_id = body["account_id"]
    location_id = body["location_id"]
    api_base_url = body["api_base_url"]
    user_offset = int(body.get("user_offset", 0))
    user_limit = body.get("user_limit")

    all_ids = await read_customer_ids_from_s3(account_id)
    ids = all_ids[user_offset:] if user_limit is None else all_ids[user_offset:user_offset + int(user_limit)]
    if not ids:
        logger.info(f"[user_batch_shard] {resource} offset={user_offset}: empty slice — skipping")
        return {"status": "success", "resource": resource, "records": 0, "fetched": 0,
                "user_offset": user_offset}

    # Shard-unique S3 tag so slices of the same resource don't collide on S3 keys.
    shard_tag = f"{location_id}_ub_{user_offset}"
    mod = importlib.import_module(f"crm_sync.{resource}")
    processed, fetched = await mod.process_resource_batched(
        ids, account_id, api_base_url, location_id, shard_tag=shard_tag
    )
    return {"status": "success", "resource": resource, "records": processed,
            "fetched": fetched, "user_offset": user_offset, "users": len(ids)}


_MODE_HANDLERS = {
    "plan": _handle_plan,
    "backfill_plan": _handle_backfill_plan,
    "location_shard": _handle_location_shard,
    "user_shard": _handle_user_shard,
    "plan_transactions": _handle_plan_transactions,
    "id_batch_shard": _handle_id_batch_shard,
    "user_batch_shard": _handle_user_batch_shard,
    "tenant_shard": _handle_tenant_shard,
}


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

async def async_lambda_handler(event, context):
    start_time = time.time()
    account_id = None
    resource = "unknown"

    # Reset per-invocation state that is bound to the asyncio.run() event loop, which
    # is closed on the next warm-start call: the shared aiohttp session, and the token
    # buckets (each holds an asyncio.Lock bound to the loop that first used it — reusing
    # a stale one on a warm container raises "bound to a different event loop").
    # A fresh bucket per shard is also correct: bursts are capped and shards are
    # sequential, so the rolling-window rate stays under the CRM limit.
    _api_client._session = None
    _api_client._buckets.clear()

    body = get_event_body(event)

    # Step Functions shard-mode dispatch (Stage 1: Plan -> LocationMap -> UserMap).
    mode = body.get("mode")
    if mode:
        handler = _MODE_HANDLERS.get(mode)
        if not handler:
            return {"status": "error", "error": f"Unknown mode '{mode}'"}
        try:
            return await handler(body)
        except Exception as exc:
            logger.critical(f"[handler] mode={mode} failed for account={body.get('account_id')}: {exc}")
            raise
        finally:
            await _api_client.close_session()

    try:
        resource = body.get("resource", "customers")
        account_id = body.get("account_id")
        api_base_url = body.get("api_base_url")
        location_id = body.get("location_id")

        logger.info(f"[handler] START resource={resource}, account={account_id}, location={location_id}")

        error = validate_params(account_id, api_base_url, location_id)
        if error:
            return error

        if resource not in VALID_RESOURCES:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    f"Resource '{resource}' does not exist. "
                    f"Valid resources are: {', '.join(VALID_RESOURCES)}"
                ),
            }

        account_config = get_account_config(account_id)
        if not account_config:
            return {"statusCode": 400, "body": json.dumps(f"Account '{account_id}' not found in database.")}

        db_integration_id = account_config.get("integration_id")
        db_crm_endpoint = account_config.get("crm_api_end_point")

        if not db_integration_id or str(db_integration_id) != str(location_id):
            return {
                "statusCode": 400,
                "body": json.dumps(
                    f"Account details don't match: account '{account_id}' has integration_id='{db_integration_id}', "
                    f"but location_id='{location_id}' was provided."
                ),
            }
        if not db_crm_endpoint or db_crm_endpoint != api_base_url:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    f"Account details don't match: account '{account_id}' has crm_api_end_point='{db_crm_endpoint}', "
                    f"but api_base_url='{api_base_url}' was provided."
                ),
            }

        logger.info(f"[handler] account={account_id} integration_id={db_integration_id} crm_endpoint={db_crm_endpoint}")

        # Clear stale parquet from a previous run for this account+resource before
        # writing fresh data, so the S3->staging step never re-ingests orphaned files.
        # Only the parent invocation cleans; child batch lambdas carry user_ids and
        # must NOT delete, or they would wipe each other's output.
        if not body.get("user_ids"):
            from utils.s3_writer import delete_account_prefix
            s3_prefix = settings.S3_PREFIXES.get(resource)
            if s3_prefix:
                logger.info(f"[handler] clearing old S3 records for resource={resource}, account={account_id}")
                await delete_account_prefix(account_id, s3_prefix)

        # Route to correct processor
        if resource in ("customers", "orders", "order_lines", "class_sessions", "reservations"):
            try:
                result = await _process_location_resource(resource, location_id, account_id, api_base_url)
                results = [result]
            except Exception as e:
                logger.error(f"[handler] {resource} failed for location={location_id}: {e}")
                results = [{"location": location_id, "status": "failed", "error": str(e)}]
            entity_type = "location"
            entity_ids = [location_id]

        elif resource in ("credit_transactions", "membership_instances", "membership_transactions"):
            results = await _process_user_batch_resource(resource, body, context, account_id, api_base_url, location_id)
            entity_type = "user"
            entity_ids = body.get("user_ids") or []

        # Aggregate counts
        total_records = sum(r.get("records", 0) for r in results if r.get("status") == "success")
        total_expected = sum(r.get("expected", r.get("records", 0)) for r in results if r.get("status") == "success")
        missing = total_expected - total_records

        if missing > 0:
            logger.error(f"[handler] {resource} data inconsistency account={account_id}: expected={total_expected}, written={total_records}, missing={missing}")
            raise Exception(f"{resource} data inconsistency: expected {total_expected}, got {total_records}, missing {missing}")

        failed_results = [r for r in results if r.get("status") == "failed"]
        status = "error" if (failed_results or missing > 0) else "success"

        elapsed = time.time() - start_time
        logger.info(f"[handler] DONE resource={resource}, account={account_id}, {len(entity_ids)} {entity_type}s, "
                    f"records={total_records}, expected={total_expected}, elapsed={elapsed:.2f}s, status={status}")

        # Teams notification
        if settings.TEAMS_ENABLED:
            try:
                from notifiers.onboard_notifier import Stage1Notifier
                await Stage1Notifier().notify({
                    "resource":        resource,
                    "account_id":      account_id,
                    "status":          status,
                    "total_records":   total_records,
                    "total_expected":  total_expected,
                    "missing_records": missing,
                    "elapsed":         f"{elapsed:.2f}s",
                })
            except Exception as notify_exc:
                logger.error(f"[handler] Stage1Notifier failed: {notify_exc}")

        return {
            "status": status,
            "statusCode": 200,
            "body": {
                "status": status,
                "message": f"{resource} processing completed in {elapsed:.2f} seconds.",
                f"total_{entity_type}s": len(entity_ids),
                "total_records": total_records,
                "total_expected": total_expected,
                "missing_records": missing,
                "results": results,
            },
        }

    except Exception as exc:
        logger.critical(f"[handler] Lambda failed for account={account_id}: {exc}")
        if settings.TEAMS_ENABLED:
            try:
                from notifiers.onboard_notifier import Stage1Notifier
                await Stage1Notifier().notify({
                    "resource":   resource,
                    "account_id": account_id,
                    "status":     "error",
                })
            except Exception:
                pass
        return {
            "status": "error",
            "statusCode": 500,
            "body": json.dumps(f"Lambda failed: {exc}"),
        }
    finally:
        await _api_client.close_session()


def lambda_handler(event, context):
    return asyncio.run(async_lambda_handler(event, context))
