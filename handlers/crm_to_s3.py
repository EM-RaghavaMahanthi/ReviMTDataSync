import json
import asyncio
import time
import re
import logging
from typing import List
from core.config import settings
from core.logger import setup_logging

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
# Main handler
# ---------------------------------------------------------------------------

async def async_lambda_handler(event, context):
    start_time = time.time()
    account_id = None
    resource = "unknown"

    try:
        body = get_event_body(event)
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


def lambda_handler(event, context):
    return asyncio.run(async_lambda_handler(event, context))
