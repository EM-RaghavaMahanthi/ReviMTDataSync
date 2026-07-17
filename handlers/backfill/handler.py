"""
Backfill Lambda — event_type dispatch, mirroring handlers/crm_to_s3.py's mode dispatch.

Each backfill job is its own event_type + module (see handlers/backfill/notes_and_tags.py);
adding a new backfill job later means adding a new module and one _EVENT_HANDLERS entry here,
not a new Lambda / new deploy script.

Reads DATABASE_URL straight from the environment rather than core.config/core.stg_db_config —
both of those pydantic Settings classes eagerly validate a bunch of CRM/Auth0/S3 fields this
Lambda has no use for (it only ever needs a Postgres connection string), so importing either
would force us to configure unrelated env vars just to satisfy validation at import time.

Required env var: DATABASE_URL

Event payload:
  { "event_type": "backfill_notes_and_tags", "account_id": 4809 }
"""

import os
import asyncio
import logging
from sqlalchemy import create_engine

from core.logger import setup_logging
from handlers.backfill import notes_and_tags

setup_logging()
logger = logging.getLogger(__name__)

_EVENT_HANDLERS = {
    "backfill_notes_and_tags": notes_and_tags.run,
}


async def async_lambda_handler(event, context=None):
    event_type = event.get("event_type")
    account_id = event.get("account_id")

    if not event_type:
        return {"status": "error", "error": "event_type is required"}
    if account_id is None:
        return {"status": "error", "error": "account_id is required"}

    handler = _EVENT_HANDLERS.get(event_type)
    if handler is None:
        return {
            "status": "error",
            "error": f"Unknown event_type '{event_type}'. Valid: {', '.join(_EVENT_HANDLERS)}",
        }

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return {"status": "error", "error": "DATABASE_URL environment variable is required"}

    logger.info(f"[backfill] event_type={event_type} account_id={account_id} starting")
    engine = create_engine(database_url)

    try:
        result = await handler(account_id, engine)
        logger.info(f"[backfill] event_type={event_type} account_id={account_id} SUCCESS")
        return {"status": "success", "event_type": event_type, "account_id": account_id, **result}
    except Exception as e:
        logger.error(f"[backfill] event_type={event_type} account_id={account_id} FAILED: {e}")
        return {"status": "error", "event_type": event_type, "account_id": account_id, "error": str(e)}
    finally:
        engine.dispose()


def lambda_handler(event, context=None):
    return asyncio.run(async_lambda_handler(event, context))
