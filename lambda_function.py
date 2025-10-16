import json
import asyncio
import itertools
from typing import List
from db.session import SessionLocal
from models.account import Account, Users
from core.config import settings

from core.logger import setup_logging
setup_logging()

import logging
logger = logging.getLogger(__name__)

def get_location(account_id: str, api_base_url: str) -> List[str]:
    session = SessionLocal()
    try:
        results = session.query(Account.integration_id).filter(
            Account.id == account_id,
            Account.crm_api_end_point == api_base_url,
            Account.crm_config.isnot(None)
        ).all()
        locations = [str(r.integration_id) for r in results if r.integration_id]
        logger.info(f"[get_location] account_id={account_id}, locations={locations}")
        if len(locations) != 1:
            logger.error(f"Expected 1 location for account_id={account_id}, got {len(locations)}. Locations: {locations}")
        else:
            logger.info(f"Found location for account_id={account_id}: {locations[0]}")
        return locations
    finally:
        session.close()

def get_all_user_ids():
    session = SessionLocal()
    try:
        return [str(row.id) for row in session.query(Users.id).all() if row.id]
    finally:
        session.close()

def get_active_locations() -> List[str]:
    session = SessionLocal()
    try:
        results = session.query(Account.integration_id).filter(
            Account.status == 'ACTIVE',
            Account.crm_config.isnot(None),
            Account.crm_api_end_point.isnot(None)
        ).all()
        locations = [str(row.integration_id) for row in results if row.integration_id]
        logger.info(f"[get_active_locations] Found {len(locations)} active locations")
        return locations
    finally:
        session.close()

def get_active_accounts() -> List[str]:
    session = SessionLocal()
    try:
        results = session.query(Account.id).filter(
            Account.status == 'ACTIVE',
            Account.crm_config.isnot(None),
            Account.crm_api_end_point.isnot(None)
        ).all()
        accounts = [str(row.id) for row in results if row.id]
        logger.info(f"[get_active_accounts] Found {len(accounts)} active accounts")
        return accounts
    finally:
        session.close()

import time

async def get_all_user_ids_from_customers(account_id: str, api_base_url, location_id):
    from services.customers_service import process_customers_for_location
    #active_locations = get_location(account_id, api_base_url)
    active_locations = [location_id]
    logger.info(f"[get_all_user_ids_from_customers] account_id={account_id}, active_locations={active_locations}")
    async def fetch_user_ids(loc):
        try:
            _, _, user_ids = await process_customers_for_location(loc, account_id, api_base_url, save_to_s3=False)
            logger.info(f"[get_all_user_ids_from_customers] account_id={account_id}, location={loc}, user_ids_count={len(user_ids)}")
            return user_ids
        except Exception as e:
            logger.error(f"[get_all_user_ids_from_customers] account_id={account_id}, location={loc}, error={e}")
            return []
    tasks = [fetch_user_ids(loc) for loc in active_locations]
    results = await asyncio.gather(*tasks)
    all_user_ids = [uid for sublist in results for uid in sublist]
    logger.info(f"[get_all_user_ids_from_customers] account_id={account_id}, total_user_ids={len(all_user_ids)}")
    return all_user_ids


def get_event_body(event):
    """
    Extracts the actual JSON payload from the event.
    Works for:
      1. Postman / Function URL requests (event['body'] is a string)
      2. AWS console test events (event is already dict)
    """
    # If event is not a dict, return empty dict
    if not isinstance(event, dict):
        return {}
    # Check if 'body' exists and is a string
    if isinstance(event.get("body"), str):
        try:
            return json.loads(event["body"])
        except json.JSONDecodeError:
            # If body is not valid JSON, return {}
            return {}
    # Otherwise, assume event is already the payload
    return event

async def async_lambda_handler(event, context):
    start_time = time.time()
    account_id = None
    try:

        # return {
        #     "status": "error",
        #     "statusCode": 500,
        #     "body": json.dumps(f"Lambda failed with error test")
        # }

        event_body = get_event_body(event)
        resource = event_body.get("resource", "customers")
        account_id = event_body.get("account_id")
        api_base_url = event_body.get("api_base_url")
        location_id = event_body.get("location_id")

        logger.info(f"[lambda_handler] Starting processing for resource={resource}, account_id={account_id}, api_base_url={api_base_url}, location_id={location_id}")

        # Validate required parameters
        def validate_lambda_params(account_id, api_base_url, location_id):
            import re
            
            if not account_id:
                logger.error("[lambda_handler] Missing account_id in event.")
                return {
                    "statusCode": 400,
                    "body": json.dumps("Missing account_id in event.")
                }

            if not api_base_url:
                logger.error("[lambda_handler] Missing api_base_url in event.")
                return {
                    "statusCode": 400,
                    "body": json.dumps("Missing api_base_url in event.")
                }

            if not location_id:
                logger.error("[lambda_handler] Missing location_id in event.")
                return {
                    "statusCode": 400,
                    "body": json.dumps("Missing location_id in event.")
                }

            url_pattern = re.compile(
                r'^https?://'  # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
                r'localhost|'  # localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
                r'(?::\d+)?'  # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
            
            if not url_pattern.match(api_base_url):
                logger.error(f"[lambda_handler] Invalid api_base_url format: {api_base_url}")
                return {
                    "statusCode": 400,
                    "body": json.dumps(f"Invalid api_base_url format: {api_base_url}. Must be a valid HTTP/HTTPS URL.")
                }

            if not str(location_id).strip():
                logger.error(f"[lambda_handler] Invalid location_id: {location_id}")
                return {
                    "statusCode": 400,
                    "body": json.dumps("Invalid location_id: cannot be empty.")
                }
            
            return None

        validation_error = validate_lambda_params(account_id, api_base_url, location_id)
        if validation_error:
            return validation_error

        #active_locations = get_location(account_id, api_base_url)
        active_locations = [location_id]
        if not active_locations:
            logger.error(f"[lambda_handler] No locations found for account_id={account_id}")
            return {
                "statusCode": 200,
                "body": json.dumps("No locations found for account_id.")
            }

        
        async def process_reservations(event):
            filter_type = event.get("type", "location")
            if filter_type == "location":
               # ids  = get_location(account_id, api_base_url)
                ids = [location_id]
            elif filter_type == "account":
                ids = get_active_accounts()
            else:
                logger.error(f"[process_reservations] Unknown filter type: {filter_type}")
                return {
                    "statusCode": 400,
                    "body": json.dumps(f"Unknown filter type: {filter_type}")
                }, None, None, None
            summary_msg = f"Reservations data processing completed for {filter_type}s"
            from services.reservations_service import process_reservations_for_entity
            async def process_id(id_):
                try:
                    logger.info(f"[process_reservations] Processing {filter_type}={id_} for account_id={account_id}")
                    processed, expected = await process_reservations_for_entity(id_, account_id, api_base_url, filter_type)
                    logger.info(f"[process_reservations] Finished {filter_type}={id_} for account_id={account_id}, records={processed}, expected={expected}")
                    return {filter_type: id_, "status": "success", "records": processed, "expected": expected}
                except Exception as e:
                    logger.error(f"[process_reservations] Processing {filter_type}={id_} for account_id={account_id} failed: {e}")
                    return {filter_type: id_, "status": "failed", "error": str(e)}
            tasks = [process_id(id_) for id_ in ids]
            results = await asyncio.gather(*tasks)
            return results, summary_msg, filter_type, ids

        async def process_credit_transactions(event, context):
            from services.credit_transactions_batch_service import process_credit_transactions_batch
            import boto3
            lambda_client = boto3.client("lambda")
            summary_msg = "Credit transactions data processing completed for users"
            def chunked(iterable, size):
                it = iter(iterable)
                for first in it:
                    yield [first] + list(itertools.islice(it, size - 1))
            batch_size = getattr(settings, "API_BATCH_SIZE", 1000)
            user_ids = event.get("user_ids")
            batch_id = event.get("batch_id", 1)
            account_id = event.get("account_id")  
            api_base_url = event.get("api_base_url")
            
            if user_ids:
                try:
                    logger.info(f"[process_credit_transactions] Processing batch_id={batch_id} for account_id={account_id}, users_count={len(user_ids)}")
                    processed, expected = await process_credit_transactions_batch(user_ids, batch_id, account_id, api_base_url, location_id)
                    logger.info(f"[process_credit_transactions] Finished batch_id={batch_id} for account_id={account_id}, records={processed}, expected={expected}")
                    return [{
                        "batch_id": batch_id,
                        "users_count": len(user_ids),
                        "status": "success",
                        "records": processed,
                        "expected": expected
                    }], summary_msg, "user", user_ids
                except Exception as e:
                    logger.error(f"[process_credit_transactions] Processing batch_id={batch_id} for account_id={account_id} failed: {e}")
                    return [{
                        "batch_id": batch_id,
                        "users_count": len(user_ids),
                        "status": "failed",
                        "error": str(e)
                    }], summary_msg, "user", user_ids
            else:
                logger.info(f"[process_credit_transactions] Getting user ids from customers data for account_id={account_id}")
                all_user_ids = await get_all_user_ids_from_customers(account_id, api_base_url, location_id)
                logger.info(f"[process_credit_transactions] account_id={account_id}, total_user_ids={len(all_user_ids)}")

                async def invoke_child_lambda(batch, batch_number):
                    logger.info(f"[process_credit_transactions] Invoking child lambda for batch #{batch_number} for account_id={account_id}, users_count={len(batch)}")
                    payload = json.dumps({
                        "resource": "credit_transactions", 
                        "account_id": account_id,
                        "api_base_url": api_base_url,
                        "location_id": location_id,
                        "user_ids": batch,
                        "batch_id": batch_number
                    })
                    try:
                        # Run the synchronous boto3 invoke in a thread to avoid blocking the event loop
                        response = await asyncio.to_thread(
                            lambda_client.invoke,
                            FunctionName=context.function_name,
                            InvocationType="RequestResponse",  # synchronous invocation
                            Payload=payload
                        )
                        response_payload = json.loads(response['Payload'].read())
                        # Check if the invocation succeeded
                        if response_payload.get("statusCode") == 200:
                            child_body = json.loads(response_payload["body"])
                            if 'results' in child_body:
                                return child_body['results']
                            else:
                                return [{
                                    "batch_id": batch_number,
                                    "users_count": len(batch),
                                    "status": "success",
                                    "message": "Batch processed successfully"
                                }]
                        else:
                            return [{
                                "batch_id": batch_number,
                                "users_count": len(batch),
                                "status": "failed",
                                "error": f"Lambda invocation failed with status {response_payload.get('statusCode')}"
                            }]
                    except Exception as e:
                        return [{
                            "batch_id": batch_number,
                            "users_count": len(batch),
                            "status": "failed",
                            "error": str(e)
                        }]

                tasks = []
                batch_number = 1
                for batch in chunked(all_user_ids, batch_size):
                    tasks.append(invoke_child_lambda(batch, batch_number))
                    batch_number += 1

                batch_results = await asyncio.gather(*tasks)
                # Flatten results from batches
                results = [item for sublist in batch_results for item in sublist]

                logger.info(f"[process_credit_transactions] Collected results from {len(batch_results)} batches for account_id={account_id}")
                return results, summary_msg, "user", all_user_ids

        async def process_membership_instances(event, context):
            from services.membership_instances_batch_service import process_membership_instances_batch
            import boto3
            lambda_client = boto3.client("lambda")
            summary_msg = "Membership instances data processing completed for users"

            def chunked(iterable, size):
                it = iter(iterable)
                for first in it:
                    yield [first] + list(itertools.islice(it, size - 1))

            batch_size = getattr(settings, "API_BATCH_SIZE", 1000)
            user_ids = event.get("user_ids")
            batch_id = event.get("batch_id", 1)
            account_id = event.get("account_id")  # Make sure account_id is in event or context
            api_base_url = event.get("api_base_url")

            if user_ids:
                try:
                    logger.info(f"[process_membership_instances] Processing batch_id={batch_id} for account_id={account_id}, users_count={len(user_ids)}")
                    processed, expected = await process_membership_instances_batch(user_ids, batch_id, account_id, api_base_url, location_id)
                    logger.info(f"[process_membership_instances] Finished batch_id={batch_id} for account_id={account_id}, records={processed}, expected={expected}")
                    return [{
                        "batch_id": batch_id,
                        "users_count": len(user_ids),
                        "status": "success",
                        "records": processed,
                        "expected": expected
                    }], summary_msg, "user", user_ids
                except Exception as e:
                    logger.error(f"[process_membership_instances] Processing batch_id={batch_id} for account_id={account_id} failed: {e}")
                    return [{
                        "batch_id": batch_id,
                        "users_count": len(user_ids),
                        "status": "failed",
                        "error": str(e)
                    }], summary_msg, "user", user_ids
            else:
                logger.info(f"[process_membership_instances] Getting user ids from customers data for account_id={account_id}")
                all_user_ids = await get_all_user_ids_from_customers(account_id, api_base_url, location_id)
                logger.info(f"[process_membership_instances] account_id={account_id}, total_user_ids={len(all_user_ids)}")

                async def invoke_child_lambda(batch, batch_number):
                    logger.info(f"[process_membership_instances] Invoking child lambda for batch #{batch_number} for account_id={account_id}, users_count={len(batch)}")
                    payload = json.dumps({
                        "resource": "membership_instances",
                        "account_id": account_id,
                        "api_base_url": api_base_url,
                        "location_id": location_id,
                        "user_ids": batch,
                        "batch_id": batch_number
                    })
                    try:
                        # Run the synchronous boto3 invoke in a thread to avoid blocking the event loop
                        response = await asyncio.to_thread(
                            lambda_client.invoke,
                            FunctionName=context.function_name,
                            InvocationType="RequestResponse",  # synchronous invocation
                            Payload=payload
                        )
                        response_payload = json.loads(response['Payload'].read())
                        # Check if the invocation succeeded
                        if response_payload.get("statusCode") == 200:
                            child_body = json.loads(response_payload["body"])
                            if 'results' in child_body:
                                return child_body['results']
                            else:
                                return [{
                                    "batch_id": batch_number,
                                    "users_count": len(batch),
                                    "status": "success",
                                    "message": "Batch processed successfully"
                                }]
                        else:
                            return [{
                                "batch_id": batch_number,
                                "users_count": len(batch),
                                "status": "failed",
                                "error": f"Lambda invocation failed with status {response_payload.get('statusCode')}"
                            }]
                    except Exception as e:
                        return [{
                            "batch_id": batch_number,
                            "users_count": len(batch),
                            "status": "failed",
                            "error": str(e)
                        }]

                tasks = []
                batch_number = 1
                for batch in chunked(all_user_ids, batch_size):
                    tasks.append(invoke_child_lambda(batch, batch_number))
                    batch_number += 1

                batch_results = await asyncio.gather(*tasks)
                # Flatten results from batches
                results = [item for sublist in batch_results for item in sublist]

                logger.info(f"[process_membership_instances] Collected results from {len(batch_results)} batches for account_id={account_id}")
                return results, summary_msg, "user", all_user_ids

        async def process_membership_transactions(event, context):
            from services.membership_transactions_batch_service import process_membership_transactions_batch
            import boto3

            lambda_client = boto3.client("lambda")
            summary_msg = "Membership transactions data processing completed for users"

            def chunked(iterable, size):
                it = iter(iterable)
                for first in it:
                    yield [first] + list(itertools.islice(it, size - 1))

            batch_size = getattr(settings, "API_BATCH_SIZE", 1000)
            user_ids = event.get("user_ids")
            batch_id = event.get("batch_id", 1)
            account_id = event.get("account_id")  # assuming account_id is in event
            api_base_url = event.get("api_base_url")

            if user_ids:
                try:
                    logger.info(f"[process_membership_transactions] Processing batch_id={batch_id} "
                                f"for account_id={account_id}, users_count={len(user_ids)}")
                    processed, expected = await process_membership_transactions_batch(user_ids, batch_id, account_id, api_base_url, location_id)
                    logger.info(f"[process_membership_transactions] Finished batch_id={batch_id} "
                                f"for account_id={account_id}, records={processed}, expected={expected}")
                    return [{
                        "batch_id": batch_id,
                        "users_count": len(user_ids),
                        "status": "success",
                        "records": processed,
                        "expected": expected
                    }], summary_msg, "user", user_ids
                except Exception as e:
                    logger.error(f"[process_membership_transactions] Processing batch_id={batch_id} "
                                f"for account_id={account_id} failed: {e}")
                    return [{
                        "batch_id": batch_id,
                        "users_count": len(user_ids),
                        "status": "failed",
                        "error": str(e)
                    }], summary_msg, "user", user_ids
            else:
                logger.info(f"[process_membership_transactions] Getting user ids from customers data for account_id={account_id}")
                all_user_ids = await get_all_user_ids_from_customers(account_id, api_base_url, location_id)
                logger.info(f"[process_membership_transactions] account_id={account_id}, total_user_ids={len(all_user_ids)}")

                async def invoke_batch(batch, batch_number):
                    logger.info(f"[process_membership_transactions] Invoking child lambda for batch #{batch_number} "
                                f"for account_id={account_id}, users_count={len(batch)}")
                    payload = json.dumps({
                        "resource": "membership_transactions",
                        "account_id": account_id,
                        "api_base_url": api_base_url,
                        "location_id": location_id,
                        "user_ids": batch,
                        "batch_id": batch_number
                    })
                    try:
                        response = await asyncio.to_thread(
                            lambda_client.invoke,
                            FunctionName=context.function_name,
                            InvocationType="RequestResponse",
                            Payload=payload
                        )
                        response_payload = json.loads(response['Payload'].read())
                        if response_payload.get('statusCode') == 200:
                            child_body = json.loads(response_payload['body'])
                            if 'results' in child_body:
                                return child_body['results']
                            else:
                                return [{
                                    "batch_id": batch_number,
                                    "users_count": len(batch),
                                    "status": "success",
                                    "message": "Batch processed successfully"
                                }]
                        else:
                            return [{
                                "batch_id": batch_number,
                                "users_count": len(batch),
                                "status": "failed",
                                "error": f"Lambda invocation failed with status {response_payload.get('statusCode')}"
                            }]
                    except Exception as e:
                        return [{
                            "batch_id": batch_number,
                            "users_count": len(batch),
                            "status": "failed",
                            "error": str(e)
                        }]

                tasks = []
                batch_number = 1
                for batch in chunked(all_user_ids, batch_size):
                    tasks.append(invoke_batch(batch, batch_number))
                    batch_number += 1

                batch_results = await asyncio.gather(*tasks)
                results = [item for sublist in batch_results for item in sublist]
                logger.info(f"[process_membership_transactions] Collected results from {len(batch_results)} batches "
                            f"for account_id={account_id}")
                return results, summary_msg, "user", all_user_ids

        # Main resource dispatch
        if resource == "reservations":
            logger.info(f"[lambda_handler] Processing reservations for account_id={account_id}")
            results, summary_msg, filter_type, ids = await process_reservations(event)
        elif resource == "credit_transactions":
            logger.info(f"[lambda_handler] Processing credit_transactions for account_id={account_id}")
            results, summary_msg, entity_type, user_ids = await process_credit_transactions(event, context)
        elif resource == "membership_instances":
            logger.info(f"[lambda_handler] Processing membership_instances for account_id={account_id}")
            results, summary_msg, entity_type, user_ids = await process_membership_instances(event, context)
        elif resource == "membership_transactions":
            logger.info(f"[lambda_handler] Processing membership_transactions for account_id={account_id}")
            results, summary_msg, entity_type, user_ids = await process_membership_transactions(event, context)
        elif resource == "customers":
            from services.customers_service import process_customers_for_location
            process_func = process_customers_for_location
            summary_msg = "Customer data processing completed"
            async def process_location(loc):
                try:
                    logger.info(f"[customers] Processing location={loc} for account_id={account_id}")
                    processed, expected, _ = await process_func(loc, account_id, api_base_url)
                    logger.info(f"[customers] Finished location={loc} for account_id={account_id}, records={processed}, expected={expected}")
                    return {"location": loc, "status": "success", "records": processed, "expected": expected}
                except Exception as e:
                    logger.error(f"[customers] Processing location={loc} for account_id={account_id} failed: {e}")
                    return {"location": loc, "status": "failed", "error": str(e)}
            #active_locations = get_location(account_id, api_base_url)
            active_locations = [location_id]
            tasks = [process_location(loc) for loc in active_locations]
            results = await asyncio.gather(*tasks)
            entity_type = "location"
            ids = active_locations
        elif resource == "orders":
            from services.orders_service import process_orders_for_location
            process_func = process_orders_for_location
            summary_msg = "Order data processing completed"
            async def process_location(loc):
                try:
                    logger.info(f"[orders] Processing location={loc} for account_id={account_id}")
                    processed, expected = await process_func(loc, account_id, api_base_url)
                    logger.info(f"[orders] Finished location={loc} for account_id={account_id}, records={processed}, expected={expected}")
                    return {"location": loc, "status": "success", "records": processed, "expected": expected}
                except Exception as e:
                    logger.error(f"[orders] Processing location={loc} for account_id={account_id} failed: {e}")
                    return {"location": loc, "status": "failed", "error": str(e)}
            active_locations = [location_id]
            tasks = [process_location(loc) for loc in active_locations]
            results = await asyncio.gather(*tasks)
            entity_type = "location"
            ids = active_locations
        elif resource == "order_lines":
            from services.order_lines_service import process_order_lines_for_location
            process_func = process_order_lines_for_location
            summary_msg = "Order lines data processing completed"
            async def process_location(loc):
                try:
                    logger.info(f"[order_lines] Processing location={loc} for account_id={account_id}")
                    processed, expected = await process_func(loc, account_id, api_base_url)
                    logger.info(f"[order_lines] Finished location={loc} for account_id={account_id}, records={processed}, expected={expected}")
                    return {"location": loc, "status": "success", "records": processed, "expected": expected}
                except Exception as e:
                    logger.error(f"[order_lines] Processing location={loc} for account_id={account_id} failed: {e}")
                    return {"location": loc, "status": "failed", "error": str(e)}
            #active_locations = get_location(account_id, api_base_url)
            active_locations = [location_id]
            tasks = [process_location(loc) for loc in active_locations]
            results = await asyncio.gather(*tasks)
            entity_type = "location"
            ids = active_locations
        elif resource == "class_sessions":
            from services.class_sessions_service import process_class_sessions_for_location
            process_func = process_class_sessions_for_location
            summary_msg = "Class sessions data processing completed"
            async def process_location(loc):
                try:
                    logger.info(f"[class_sessions] Processing location={loc} for account_id={account_id}")
                    processed, expected = await process_func(loc, account_id, api_base_url)
                    logger.info(f"[class_sessions] Finished location={loc} for account_id={account_id}, records={processed}, expected={expected}")
                    return {"location": loc, "status": "success", "records": processed, "expected": expected}
                except Exception as e:
                    logger.error(f"[class_sessions] Processing location={loc} for account_id={account_id} failed: {e}")
                    return {"location": loc, "status": "failed", "error": str(e)}
            #active_locations = get_location(account_id, api_base_url)
            active_locations = [location_id]
            tasks = [process_location(loc) for loc in active_locations]
            results = await asyncio.gather(*tasks)
            entity_type = "location"
            ids = active_locations
        else:
            logger.error(f"[lambda_handler] Unknown resource: {resource}")
            return {
                "statusCode": 400,
                "body": json.dumps(f"Unknown resource: {resource}")
            }
        total_records = sum(r.get("records", 0) for r in results if r.get("status") == "success")
        total_expected = sum(r.get("expected", r.get("records", 0)) for r in results if r.get("status") == "success")

        if total_expected is not None and total_records < total_expected:
            missing_records = total_expected - total_records
            error_msg = f"{resource.title()} data inconsistency for account_id={account_id}: Expected {total_expected} records, but only {total_records} written to S3. Missing: {missing_records}"
            logger.error(f"[lambda_handler] {error_msg}")
            raise Exception(error_msg)
        else:
            logger.info(f"[lambda_handler] {resource.title()} FULL SUCCESS for account_id={account_id}: All expected records processed successfully. Total: {total_records}")


        elapsed = time.time() - start_time
        if resource == "reservations":
            entity_type = filter_type
            entity_count = len(ids)
        elif resource == "credit_transactions":
            entity_type = "user"
            entity_count = len(user_ids)
        elif resource == "membership_instances":
            entity_type = "user"
            entity_count = len(user_ids)
        elif resource == "membership_transactions":
            entity_type = "user"
            entity_count = len(user_ids)
        else:
            entity_type = "location"
            entity_count = len(active_locations)
        logger.info(f"[lambda_handler] Processed {entity_count} {entity_type}s for account_id={account_id} in {elapsed:.2f} seconds. Total records: {total_records}, Expected: {total_expected}")
        
        # Determine status based on data validation
        missing_records_count = total_expected - total_records
        if total_records == 0 or missing_records_count > 0:
            status = "error"
            logger.error(f"[lambda_handler] Setting status=error for account_id={account_id}: total_records={total_records}, missing_records={missing_records_count}")
        else:
            status = "success"
            logger.info(f"[lambda_handler] Setting status=success for account_id={account_id}: all records processed successfully")
        
        response_body = {
            "status": status,
            "message": f"{summary_msg} in {elapsed:.2f} seconds.",
            f"total_{entity_type}s": entity_count,
            "total_records": total_records,
            "total_expected": total_expected,
            "missing_records": missing_records_count,
            "results": results
        }
        logger.info(f"[lambda_handler] Finished processing for resource={resource}, account_id={account_id}")
        return {
            "status": status,
            "statusCode": 200,
            "body": json.dumps(response_body)
        }
    except Exception as exc:
        logger.critical(f"[lambda_handler] Lambda execution failed for account_id={account_id}: {exc}")
        return {
            "status": "error",
            "statusCode": 500,
            "body": json.dumps(f"Lambda failed with error: {exc}")
        }

def lambda_handler(event, context):
    return asyncio.run(async_lambda_handler(event, context))
