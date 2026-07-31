"""
Bulk Stage 1 — Silver → staging, for every account in one time window.

The delta counterpart to handlers/s3_to_db.py. That Lambda loads one account's full CRM
pull out of S3 parquet; this one loads whatever Silver received during (start, end] for
every account at once, then runs the widened stale update.

One action per invocation; the Step Function sequences them:

  stage         (default) claim the run slot, create staging, load all 13 tables,
                run update_stale, return the promotable account_ids for the Map
  update_stale  stale update only — for one account or a list. Lets the Step Function
                move the stale pass into the Map if `stage` starts running long
  cleanup       drop the staging tables, release the run slot
  verify        assert the target tables are reachable and Athena is configured

Event:
  {"action": "stage", "start_time": "2026-07-30T00:00:00Z", "end_time": "2026-07-31T00:00:00Z"}
  {"action": "stage", "delta_minutes": 90}                  window = (now-90m, now]
  {"action": "stage", "account_ids": [1410], "run_stale": false, "force": true}
  {"action": "update_stale", "account_id": 1410, "dry_run": true}
  {"action": "cleanup"}

Environment variables — see core/bulk_config.py. The load needs DATABASE_URL, REGION,
SILVER_NAMESPACE, ATHENA_OUTPUT_LOCATION and ATHENA_WORKGROUP at minimum.
"""

import json
import logging
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

from core.bulk_config import settings
from core.logger import setup_logging
from s3_to_stg_bulk import config as cfg
from s3_to_stg_bulk import staging
from s3_to_stg_bulk.update_stale import update_stale_data, update_stale_data_for_accounts

setup_logging(log_level="INFO")
logger = logging.getLogger(__name__)


def _parse_dt(value: str) -> datetime:
    """ISO-8601 → naive UTC, matching Silver's silver_inserted_at."""
    return (
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )


def _window(event: dict) -> tuple:
    """
    (start, end] as naive UTC. Explicit start_time/end_time wins; otherwise the window is
    the last delta_minutes (event) or BULK_DELTA_MINUTES (env) up to now.
    """
    end = _parse_dt(event["end_time"]) if event.get("end_time") else \
        datetime.now(timezone.utc).replace(tzinfo=None)

    if event.get("start_time"):
        start = _parse_dt(event["start_time"])
    else:
        minutes = int(event.get("delta_minutes") or settings.BULK_DELTA_MINUTES)
        start = end - timedelta(minutes=minutes)

    if start >= end:
        raise ValueError(f"empty window: start={start.isoformat()} end={end.isoformat()}")

    return start, end


def _engine():
    # No pooling: each invocation is one short-lived Lambda doing bulk SQL, and COPY runs
    # on a raw connection taken from this engine.
    return create_engine(settings.DATABASE_URL, pool_pre_ping=True)


# ── Actions ─────────────────────────────────────────────────────────────────

def _verify(event: dict, engine) -> dict:
    """Read-only pre-flight. Writes nothing."""
    report = {}
    ok = True

    with engine.connect() as conn:
        for table in cfg.STAGING_ORDER:
            tgt = cfg.target_table(table)
            exists = conn.execute(text("""
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema = 'public' AND table_name = :t
                )
            """), {"t": tgt}).scalar()
            report[table] = {"target": tgt, "exists": bool(exists)}
            ok = ok and bool(exists)

    if not settings.ATHENA_OUTPUT_LOCATION:
        report["athena"] = "ATHENA_OUTPUT_LOCATION is not set"
        ok = False
    else:
        report["athena"] = {
            "namespace": settings.SILVER_NAMESPACE,
            "workgroup": settings.ATHENA_WORKGROUP,
            "output": settings.ATHENA_OUTPUT_LOCATION,
        }

    return {"status": "success" if ok else "error", "action": "verify", "report": report}


def _stage(event: dict, engine) -> dict:
    start, end = _window(event)
    account_ids = [int(a) for a in (event.get("account_ids") or [])]
    run_stale = event.get("run_stale", True)
    dry_run = bool(event.get("dry_run", False))
    run_id = event.get("run_id") or f"bulk-{uuid.uuid4().hex[:12]}"

    logger.info(
        f"[stage] run_id={run_id} window=({start.isoformat()}, {end.isoformat()}] "
        f"accounts={account_ids or 'all'} run_stale={run_stale} dry_run={dry_run}"
    )

    staging.claim_run_slot(engine, run_id, force=bool(event.get("force", False)))
    staging.create_staging_tables(engine)

    t0 = time.time()
    staged = staging.load_all(engine, start, end, account_ids)
    load_elapsed = round(time.time() - t0, 2)

    accounts = staging.promotable_account_ids(engine)

    stale = None
    if run_stale and accounts["account_ids"]:
        stale = update_stale_data_for_accounts(engine, accounts["account_ids"], dry_run)

    response = {
        "status": "success",
        "action": "stage",
        "run_id": run_id,
        "window": {"start_time": start.isoformat(), "end_time": end.isoformat()},
        "staged": staged,
        "staged_rows": sum(staged.values()),
        "load_elapsed_seconds": load_elapsed,
        # The Map input. Empty means the window held nothing promotable — the Step
        # Function's Map handles that as zero iterations.
        "account_ids": accounts["account_ids"],
        "skipped_inactive_accounts": accounts["skipped"],
        "stale": stale,
    }

    if stale and not stale["success"]:
        # Surface it, but do not fail the stage: the rows are staged, and promotion can
        # still run. The failed accounts are named so an operator can re-run the action.
        logger.error(f"[stage] stale update failed for accounts {stale['failed_accounts']}")
        response["status"] = "partial"

    return response


def _update_stale(event: dict, engine) -> dict:
    dry_run = bool(event.get("dry_run", False))
    account_ids = [int(a) for a in (event.get("account_ids") or [])]
    if event.get("account_id") is not None:
        account_ids.append(int(event["account_id"]))
    if not account_ids:
        account_ids = staging.promotable_account_ids(engine)["account_ids"]

    if len(account_ids) == 1:
        result = update_stale_data(engine, account_ids[0], dry_run)
        status = "success" if result["success"] else "error"
    else:
        result = update_stale_data_for_accounts(engine, account_ids, dry_run)
        status = "success" if result["success"] else "error"

    return {
        "status": status,
        "action": "update_stale",
        "account_ids": account_ids,
        "dry_run": dry_run,
        "result": result,
    }


def _cleanup(event: dict, engine) -> dict:
    dropped = staging.drop_staging_tables(engine)
    staging.release_run_slot(engine)
    return {"status": "success", "action": "cleanup", "dropped": dropped}


_ACTIONS = {
    "stage": _stage,
    "update_stale": _update_stale,
    "cleanup": _cleanup,
    "verify": _verify,
}


def lambda_handler(event, context=None):
    action = (event or {}).get("action", "stage")
    handler = _ACTIONS.get(action)
    if handler is None:
        raise ValueError(f"unknown action {action!r} — expected one of {sorted(_ACTIONS)}")

    engine = _engine()
    start = time.time()
    try:
        response = handler(event or {}, engine)
        response["elapsed_time_seconds"] = round(time.time() - start, 2)
        logger.info(f"[{action}] done in {response['elapsed_time_seconds']}s")

        if response["status"] == "error":
            # Fail the invocation so Step Functions retries, then routes to a Fail state
            # (enabling redrive).
            raise RuntimeError(f"{action} failed: {json.dumps(response, default=str)[:900]}")

        return response

    except Exception as e:
        logger.error(f"[{action}] failed after {round(time.time() - start, 2)}s: {e}")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    # ./s3_to_stg_bulk.py stage 120        → last 120 minutes
    # ./s3_to_stg_bulk.py verify
    cli_action = sys.argv[1] if len(sys.argv) > 1 else "stage"
    cli_event = {"action": cli_action}
    if len(sys.argv) > 2:
        cli_event["delta_minutes"] = int(sys.argv[2])
    print(json.dumps(lambda_handler(cli_event), indent=2, default=str))
