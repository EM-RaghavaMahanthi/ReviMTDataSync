"""
Backfill Lambda — event_type dispatch, mirroring handlers/crm_to_s3.py's mode dispatch.

Each backfill job is its own event_type + module (see handlers/backfill/notes_and_tags.py);
adding a new backfill job later means adding a new module and one _EVENT_HANDLERS entry here,
not a new Lambda / new deploy script.

Reads DATABASE_URL straight from the environment rather than core.stg_db_config, which
eagerly validates a bunch of Auth0 fields this Lambda has no use for (it only ever needs a
Postgres connection string), so importing it would force us to configure unrelated env vars
just to satisfy validation at import time. core.config itself is safe to import now that
DATABASE_URL is its only strictly-required field (refresh_recent_notes needs it for
utils.api_client / crm_sync.user_notes).

Required env var: DATABASE_URL
Required for refresh_recent_notes only: API_KEY (MarianaTek bearer token)
Optional env var: NOTES_WINDOW_HOURS (refresh_recent_notes only - default 24, see notes_recent.py)
Optional env var: BACKFILL_WRITE_ENABLED (default FALSE - see _resolve_write below)

Event payload:
  { "event_type": "backfill_notes_and_tags", "account_id": 4809 }
  { "event_type": "refresh_recent_notes", "account_id": 4809 }
  optional on either: "write": true   (overrides BACKFILL_WRITE_ENABLED for this call)
"""

import os
import asyncio
import logging
from sqlalchemy import create_engine

from core.logger import setup_logging
import utils.api_client as _api_client
from handlers.backfill import notes_and_tags, notes_recent

setup_logging()
logger = logging.getLogger(__name__)

def _resolve_write(event) -> bool:
    """
    Whether this invocation may INSERT into the main tables. Event field wins over the env
    var, matching the update/BULK_UPDATE precedence in the bulk pipeline: the deployed env
    var is the safe default, a caller who means it says so per-call.

    Defaults to FALSE. Every count still runs when disabled, so a dry invocation reports
    exactly what it would have written — which is the point: run it, read the numbers,
    then re-run with "write": true.
    """
    if "write" in event:
        return bool(event["write"])
    return os.environ.get("BACKFILL_WRITE_ENABLED", "false").strip().lower() in ("1", "true", "yes")


_EVENT_HANDLERS = {
    "backfill_notes_and_tags": notes_and_tags.run,
    "refresh_recent_notes": notes_recent.run,
}


async def async_lambda_handler(event, context=None):
    # Reset per-invocation state that is bound to the asyncio.run() event loop, which is
    # closed on the next warm-start call: the shared aiohttp session, and the token buckets
    # (each holds an asyncio.Lock bound to the loop that first used it — reusing a stale one
    # on a warm container raises "Event loop is closed"). Same fix as handlers/crm_to_s3.py.
    # Only refresh_recent_notes uses utils.api_client today, but this is harmless for jobs
    # that don't touch it.
    _api_client._session = None
    _api_client._buckets.clear()

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

    write = _resolve_write(event)
    logger.info(
        f"[backfill] event_type={event_type} account_id={account_id} write={write} starting"
    )
    if not write:
        logger.warning(
            "[backfill] WRITE DISABLED — counting only, no rows will be inserted into "
            "customer_notes / customer_tags_default / customer_tag_assignments. "
            'Set BACKFILL_WRITE_ENABLED=true or pass "write": true to enable.'
        )
    engine = create_engine(database_url)

    try:
        result = await handler(account_id, engine, write=write)
        logger.info(f"[backfill] event_type={event_type} account_id={account_id} SUCCESS")
        return {"status": "success", "event_type": event_type, "account_id": account_id,
                "write_enabled": write, **result}
    except Exception as e:
        logger.error(f"[backfill] event_type={event_type} account_id={account_id} FAILED: {e}")
        return {"status": "error", "event_type": event_type, "account_id": account_id, "error": str(e)}
    finally:
        engine.dispose()
        await _api_client.close_session()


def lambda_handler(event, context=None):
    return asyncio.run(async_lambda_handler(event, context))
