"""
Bulk Stage 1 — Silver → staging, for every account in one time window.

The delta counterpart to handlers/s3_to_db.py. That Lambda loads one account's full CRM
pull out of S3 parquet; this one loads whatever Silver received during (start, end] for
every account at once, then runs the widened stale update.

One action per invocation; the Step Function sequences them:

  stage         (default) resolve the account set, claim the run slot, create staging,
                load the 9 tables, run update_stale, return the account_ids for the Map
  update_stale  stale update only — for one account or a list. Lets the Step Function
                move the stale pass into the Map if `stage` starts running long
  reconcile     read-only: prove every staged row reached the target. Run after
                promotion and BEFORE cleanup, which drops the staging it compares against
  cleanup       drop the staging tables, release the run slot
  verify        assert the target tables are reachable and Athena is configured

Event:
  account_ids     which accounts to process. Omitted or empty means every active account.
                  Ids are intersected with the active set BEFORE anything is loaded, and
                  anything rejected is named with a reason in the response.
  start_datetime  ISO-8601. With end_datetime, defines the window explicitly.
  end_datetime    ISO-8601. Defaults to now.
  delta_minutes   window length when start_datetime is absent. Falls back to
                  BULK_DELTA_MINUTES (env, default 10).
  update          false counts what would change and writes nothing; true performs the
                  stale update. Falls back to BULK_UPDATE (env, default false) when the
                  field is absent — an explicit false in the event still wins over the env.

  {"action": "stage", "account_ids": [1410, 1411],
   "start_datetime": "2026-07-30T00:00:00Z", "end_datetime": "2026-07-31T00:00:00Z",
   "update": true}
  {"action": "stage"}                                    window = (now-10m, now], all active
  {"action": "stage", "delta_minutes": 90}               window = (now-90m, now]
  {"action": "stage", "account_ids": [1410], "run_stale": false, "force": true}
  {"action": "update_stale", "account_id": 1410}         dry run
  {"action": "reconcile"}                                everything staged
  {"action": "reconcile", "account_ids": [1410]}         scope it
  {"action": "cleanup"}

Environment variables — see core/bulk_config.py. The load needs DATABASE_URL, REGION,
SILVER_NAMESPACE, ATHENA_CATALOG, ATHENA_WORKGROUP and ATHENA_OUTPUT_BUCKET at minimum.
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
from s3_to_stg_bulk import athena
from s3_to_stg_bulk import config as cfg
from s3_to_stg_bulk import staging
from s3_to_stg_bulk.update_stale import update_stale_data

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
    (start, end] as naive UTC. Explicit start_datetime/end_datetime wins; otherwise the
    window is the last delta_minutes (event) or BULK_DELTA_MINUTES (env, default 10) up
    to now.
    """
    end = _parse_dt(event["end_datetime"]) if event.get("end_datetime") else \
        datetime.now(timezone.utc).replace(tzinfo=None)

    if event.get("start_datetime"):
        start = _parse_dt(event["start_datetime"])
    else:
        minutes = int(event.get("delta_minutes") or settings.BULK_DELTA_MINUTES)
        start = end - timedelta(minutes=minutes)

    if start >= end:
        raise ValueError(f"empty window: start={start.isoformat()} end={end.isoformat()}")

    return start, end


def _update_flag(event: dict) -> bool:
    """
    Whether the stale pass writes. Event field wins, else BULK_UPDATE (env, default false).

    Membership is tested rather than event.get("update", …) so an explicit "update": false
    still overrides an env var set to true — the safe direction has to be reachable per
    invocation.
    """
    if "update" in event:
        return bool(event["update"])
    return bool(settings.BULK_UPDATE)


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

    report["athena"] = {
        "catalog": athena.catalog(),
        "namespace": settings.SILVER_NAMESPACE,
        "workgroup": settings.ATHENA_WORKGROUP,
        "output_requested": settings.ATHENA_OUTPUT_LOCATION,
        "example_table": athena.table_ref(cfg.silver_table(cfg.STAGING_ORDER[0])),
    }

    return {"status": "success" if ok else "error", "action": "verify", "report": report}


def _stage(event: dict, engine) -> dict:
    start, end = _window(event)
    account_ids = [int(a) for a in (event.get("account_ids") or [])]
    run_stale = event.get("run_stale", True)
    update = _update_flag(event)
    run_id = event.get("run_id") or f"bulk-{uuid.uuid4().hex[:12]}"

    logger.info(
        f"[stage] run_id={run_id} window=({start.isoformat()}, {end.isoformat()}] "
        f"accounts={account_ids or 'all'} run_stale={run_stale} update={update}"
    )

    # Resolve the account set FIRST — an invalid or inactive id must never reach the
    # Athena scan or staging.
    resolved = staging.resolve_accounts(engine, account_ids)

    # Then prove Athena is reachable and the Silver namespace resolves, BEFORE taking the
    # run slot. Anything that fails after the slot is claimed leaves it held until
    # STALE_RUN_HOURS elapses or someone passes force=true, so cheap preconditions belong
    # in front of it.
    athena.preflight(cfg.silver_table(cfg.STAGING_ORDER[0]))

    staging.claim_run_slot(engine, run_id, force=bool(event.get("force", False)))

    # Everything past the claim runs under a release-on-failure guard. Without it a run
    # that dies here holds the slot until STALE_RUN_HOURS elapses or an operator passes
    # force=true — a six-hour wedge caused by, say, a bad env var.
    #
    # Releasing is safe: the slot exists only to stop a second run dropping and recreating
    # the fixed-name stg_*_bulk tables under a live one. A stage that failed is not live,
    # and the next run recreates those tables anyway, so there is nothing left to protect.
    #
    # The staging tables are deliberately NOT dropped — same choice the Step Function's
    # failure path makes. Whatever loaded stays available to inspect, or to promote
    # without re-reading Silver.
    try:
        staging.create_staging_tables(engine)

        t0 = time.time()
        staged = staging.load_all(engine, start, end, resolved["accounts"])
        load_elapsed = round(time.time() - t0, 2)

        # Of the accounts we loaded for, the ones that actually had rows in the window.
        # Only these are worth a Map branch.
        with_rows = set(staging.staged_account_ids(engine))
        processed = [a for a in resolved["accounts"] if a in with_rows]
        no_rows = [a for a in resolved["accounts"] if a not in with_rows]
        if no_rows:
            logger.info(f"[stage] {len(no_rows)} accounts had no rows in the window: {no_rows}")

        stale = None
        if run_stale and processed:
            stale = update_stale_data(engine, processed, update)

    except Exception:
        # Best-effort: a release that itself fails must not mask the original error, which
        # is the one worth reading.
        try:
            staging.release_run_slot(engine)
            logger.error(
                f"[stage] run_id={run_id} failed — run slot released, staging left in "
                f"place. Re-run without force once the cause is fixed."
            )
        except Exception as release_error:
            logger.error(
                f"[stage] run_id={run_id} failed AND the run slot could not be released "
                f"({release_error}). The next run needs {{\"force\": true}}."
            )
        raise

    response = {
        "status": "success",
        "action": "stage",
        "run_id": run_id,
        "window": {"start_datetime": start.isoformat(), "end_datetime": end.isoformat()},
        "staged": staged,
        "staged_rows": sum(staged.values()),
        "load_elapsed_seconds": load_elapsed,
        # The Map input. Empty means the window held nothing — the Step Function's Map
        # handles that as zero iterations.
        "account_ids": processed,
        "accounts": {
            "requested": account_ids or "all_active",
            "resolved": resolved["accounts"],
            "processed": processed,
            "no_rows_in_window": no_rows,
            "rejected": resolved["rejected"],
        },
        "stale": stale,
    }

    if stale and not stale["success"]:
        # Surface it, but do not fail the stage: the rows are staged, and promotion can
        # still run. The failed accounts are named so an operator can re-run the action.
        logger.error(f"[stage] stale update failed for accounts {stale['failed_accounts']}")
        response["status"] = "partial"

    return response


def _update_stale(event: dict, engine) -> dict:
    update = _update_flag(event)
    requested = [int(a) for a in (event.get("account_ids") or [])]
    if event.get("account_id") is not None:
        requested.append(int(event["account_id"]))

    # Same validation path as `stage`: an id supplied here gets checked against the active
    # set too, rather than being trusted because it came from an operator.
    resolved = staging.resolve_accounts(engine, requested)

    # The update joins staging, so an account with nothing staged has no work. Skipping it
    # here just avoids the round trips.
    with_rows = set(staging.staged_account_ids(engine))
    account_ids = [a for a in resolved["accounts"] if a in with_rows]

    if not account_ids:
        return {
            "status": "success",
            "action": "update_stale",
            "account_ids": [],
            "rejected": resolved["rejected"],
            "update": update,
            "result": {"success": True, "accounts": 0, "note": "nothing staged"},
        }

    result = update_stale_data(engine, account_ids, update)

    return {
        "status": "success" if result["success"] else "error",
        "action": "update_stale",
        "account_ids": account_ids,
        "rejected": resolved["rejected"],
        "update": update,
        "result": result,
    }


def _reconcile(event: dict, engine) -> dict:
    """
    Did every row Silver held for the window reach the target?

    Same inputs as `stage` minus end_datetime — the window is always (start, now], because
    an upper bound on a "did everything land" check could only hide rows that did not.

    Read-only against production, and self-contained: it loads its own copy of the window
    into rec_*_bulk rather than reading whatever `stage` left in staging, so it can run
    before promotion, after cleanup, or on its own.
    """
    start, _ = _window({**event, "end_datetime": None})
    resolved = staging.resolve_accounts(
        engine, [int(a) for a in (event.get("account_ids") or [])]
        + ([int(event["account_id"])] if event.get("account_id") is not None else [])
    )
    account_ids = resolved["accounts"]

    result = staging.reconcile(engine, account_ids, start)

    # A shortfall is reported, not raised: the operator decides whether it is explained
    # (a promote that has not run yet) or a real miss. Raising here would also make the
    # Step Function retry a read-only check.
    return {
        "status": "success" if result["complete"] else "incomplete",
        "action": "reconcile",
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
    "reconcile": _reconcile,
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
