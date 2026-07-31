"""
Stale data updater for the bulk pipeline — generalised from db_services/update_stale.py.

Differences from that module, all deliberate:

1. **Column coverage is complete.** The onboarding version hand-picks a few columns per
   table (orders: date_placed + status; order_lines: title + transaction_type; …), so a
   change to any other column is never propagated. Here the SET list is derived from the
   spec — every mutable column with a Silver source — so nothing is silently skipped.
   See config.mutable_columns.
2. **Staging names are stg_<table>_bulk**, and the four credit/membership tables each
   read their OWN staging table. The onboarding version shares one staging table per pair
   and splits it with isin_reservation / isin_order_line; Silver already ships them split,
   so those columns do not exist and are not needed.
3. **NOT NULL target columns use COALESCE(staging, target)**, so a NULL from Silver reads
   as "no information" and can never wipe a live value or violate a constraint. The
   difference test for those columns is guarded the same way.
4. **`location` is gone from the join conditions.** Staging is unique on
   (account_id, business key), so a delta spanning several locations needs no
   per-location loop.
5. **ref_id repair is spec-driven** (config.REF_SPECS) instead of hand-written per table,
   which is how the columns the onboarding version misses — order_lines.order_ref_id,
   reservations.class_session_ref_id, customer_notes.customer_ref_id — get repaired at
   all. Ref columns the backend marks @unique are guarded so the repair cannot raise a
   unique violation.
6. **deleted_at / deleted_by are included** in the mutable set, which is how soft deletes
   propagate now that the is_valid / child_orders placeholder detection is unavailable
   (Silver carries neither column).

The `updated_at` recency gate is reported on, not just applied — see _count.
"""

import logging
import time
import traceback

from sqlalchemy import text

from core.bulk_config import settings
from s3_to_stg_bulk import config as cfg

logger = logging.getLogger(__name__)


# ── Column resolution ───────────────────────────────────────────────────────
# The spec is written against the backend schema as documented, but this Lambda cannot
# import Prisma to check. Intersecting the spec with information_schema once per cold
# start turns "the spec names a column the target dropped" from a failed UPDATE (which
# would fail the whole account) into a logged warning.

_mutable_cache: dict = {}


def _mutable(engine, table: str) -> list:
    if table in _mutable_cache:
        return _mutable_cache[table]

    wanted = cfg.mutable_columns(table)
    tgt = cfg.target_table(table)

    with engine.connect() as conn:
        present = {
            r[0] for r in conn.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :t
            """), {"t": tgt}).fetchall()
        }

    usable = [c for c in wanted if c in present]
    missing = [c for c in wanted if c not in present]
    if missing:
        logger.warning(
            f"[{table}] {tgt} has no column(s) {missing} — excluded from the stale "
            f"update. Fix the spec in s3_to_stg_bulk/config.py if this is unexpected."
        )
    if not usable:
        raise RuntimeError(f"[{table}] no updatable columns remain against {tgt}")

    _mutable_cache[table] = usable
    return usable


# ── Predicate / clause builders ─────────────────────────────────────────────

def _col_diff(table: str, col: str) -> str:
    """Per-column 'staging differs from target' test."""
    if col in cfg.ts_columns(table):
        # timestamp(3) rounding reports a difference on every run under
        # IS DISTINCT FROM, so timestamps get the onboarding pipeline's 0.1s tolerance.
        cmp_ = (
            f"((t.{col} IS NULL) <> (s.{col} IS NULL)"
            f" OR (t.{col} IS NOT NULL AND s.{col} IS NOT NULL"
            f" AND abs(extract(epoch from (t.{col} - s.{col}))) >= 0.1))"
        )
    else:
        cmp_ = f"(t.{col} IS DISTINCT FROM s.{col})"

    if col in cfg.never_null_columns(table):
        # NULL in staging means "Silver has no opinion" — never a difference, so it can
        # neither trigger an update nor be written over a live value.
        return f"(s.{col} IS NOT NULL AND {cmp_})"
    return cmp_


def _diff_predicate(table: str, cols: list) -> str:
    return "(\n       " + "\n    OR ".join(_col_diff(table, c) for c in cols) + "\n  )"


def _set_clause(table: str, cols: list) -> str:
    never_null = cfg.never_null_columns(table)
    assignments = [
        f"{col} = COALESCE(s.{col}, t.{col})" if col in never_null else f"{col} = s.{col}"
        for col in cols
    ]
    assignments += [f"updated_by = {cfg.ACTOR_ID}", "updated_at = now()"]
    return ",\n      ".join(assignments)


def _recency_gate(table: str) -> str:
    """
    The 'staging is newer than the target row' condition, or TRUE when disabled.

    Silver's user_notes carries no updated_at, so for tables whose updated_at has no
    Silver source the gate falls back to silver_inserted_at — "Silver saw this row after
    we last wrote it".
    """
    if not settings.REQUIRE_NEWER_UPDATED_AT:
        return "TRUE"
    clock = "updated_at" if cfg.source_of(table, "updated_at") else "silver_inserted_at"
    return f"s.{clock} > t.updated_at"


def _join_on(table: str) -> str:
    """Staging ↔ target join: account_id plus the business key."""
    keys = ["account_id", *cfg.key_columns(table)]
    return "\n     AND ".join(f"s.{k} = t.{k}" for k in keys)


# ── Column updates ──────────────────────────────────────────────────────────

def _count(engine, table: str, cols: list, account_id) -> tuple:
    """
    (differing, suppressed) for one table.

    `differing` respects the recency gate — it is what the UPDATE will touch.
    `suppressed` is the count that differs in value but is held back by the gate, logged
    so a dry run shows how much the gate is hiding.
    """
    stg = cfg.staging_name(table)
    tgt = cfg.target_table(table)
    gate = _recency_gate(table)
    diff = _diff_predicate(table, cols)
    join = _join_on(table)

    sql = f"""
        SELECT
          COUNT(*) FILTER (WHERE {gate})     AS differing,
          COUNT(*) FILTER (WHERE NOT ({gate})) AS suppressed
        FROM {tgt} t
        JOIN "{stg}" s
          ON {join}
        WHERE t.account_id = :account_id
          AND {diff}
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql), {"account_id": account_id}).fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def _update(engine, table: str, cols: list, account_id) -> int:
    stg = cfg.staging_name(table)
    tgt = cfg.target_table(table)

    sql = f"""
        UPDATE {tgt} t
        SET
          {_set_clause(table, cols)}
        FROM "{stg}" s
        WHERE {_join_on(table)}
          AND t.account_id = :account_id
          AND {_recency_gate(table)}
          AND {_diff_predicate(table, cols)}
    """
    with engine.begin() as conn:
        return conn.execute(text(sql), {"account_id": account_id}).rowcount


# ── ref_id repair ───────────────────────────────────────────────────────────

def _repair_ref(engine, table: str, ref, account_id, dry_run: bool) -> dict:
    """
    Point one ref column at the parent row its business key names.

    The parent lookup goes through a LATERAL … LIMIT 1 so the resolved id is
    deterministic even where the parent table has no unique index on
    (account_id, business key) yet.
    """
    stg = cfg.staging_name(table)
    tgt = cfg.target_table(table)
    parent = cfg.target_table(ref.parent)

    lookup = f"""
        LEFT JOIN LATERAL (
          SELECT p.id
          FROM {parent} p
          WHERE p.account_id = s.account_id
            AND p.{ref.parent_key} = s.{ref.child_key}
          ORDER BY p.id
          LIMIT 1
        ) par ON TRUE
    """

    # A @unique ref column may be held by only one child row. Skip any row whose target
    # id is already taken by a different row rather than raising mid-update.
    unique_guard = ""
    if (table, ref.col) in cfg.UNIQUE_REF_COLS:
        unique_guard = f"""
          AND NOT EXISTS (
            SELECT 1 FROM {tgt} other
            WHERE other.{ref.col} = par.id
              AND other.id <> t.id
          )"""

    where = f"""
        WHERE {_join_on(table)}
          AND t.account_id = :account_id
          AND s.{ref.child_key} IS NOT NULL
          AND par.id IS NOT NULL
          AND t.{ref.col} IS DISTINCT FROM par.id{unique_guard}
    """

    # The count states the staging↔target match in its JOIN clause, so its WHERE repeats
    # only the row filters. The UPDATE has to keep the match in WHERE — staging is in its
    # FROM list, and UPDATE has no JOIN clause.
    count_sql = f"""
        SELECT COUNT(*)
        FROM {tgt} t
        JOIN "{stg}" s ON {_join_on(table)}
        {lookup}
        WHERE t.account_id = :account_id
          AND s.{ref.child_key} IS NOT NULL
          AND par.id IS NOT NULL
          AND t.{ref.col} IS DISTINCT FROM par.id{unique_guard}
    """

    with engine.connect() as conn:
        expected = conn.execute(text(count_sql), {"account_id": account_id}).scalar() or 0

    if expected == 0 or dry_run:
        return {"column": ref.col, "expected": expected, "updated": 0}

    update_sql = f"""
        UPDATE {tgt} t
        SET {ref.col} = par.id,
            updated_by = {cfg.ACTOR_ID},
            updated_at = now()
        FROM "{stg}" s
        {lookup}
        {where}
    """
    with engine.begin() as conn:
        updated = conn.execute(text(update_sql), {"account_id": account_id}).rowcount

    logger.info(f"  [{table}] {ref.col} → {parent}.id: {updated} rows repaired")
    return {"column": ref.col, "expected": expected, "updated": updated}


# ── Per-table entry point ───────────────────────────────────────────────────

def update_table(engine, table: str, account_id, dry_run: bool = False) -> dict:
    """Update every mutable column, then repair every ref_id, for one table."""
    logger.info(f"[{table}] checking stale records...")
    t0 = time.time()
    try:
        cols = _mutable(engine, table)
        differing, suppressed = _count(engine, table, cols, account_id)
        logger.info(
            f"[{table}] {differing} stale records found"
            + (f" ({suppressed} more differ but are older than the target row)"
               if suppressed else "")
        )

        updated = 0
        if differing and not dry_run:
            updated = _update(engine, table, cols, account_id)
            if updated != differing:
                # Concurrent writes can move the boundary between COUNT and UPDATE. Worth
                # seeing, not worth failing the account over.
                logger.warning(
                    f"[{table}] updated {updated} rows, expected {differing}"
                )

        ref_results = [
            _repair_ref(engine, table, ref, account_id, dry_run)
            for ref in cfg.refs(table)
        ]

        return {
            "expected": differing,
            "updated": updated,
            "suppressed_by_updated_at": suppressed,
            "refs": ref_results,
            "columns_covered": len(cols),
            "success": True,
            "elapsed_time_seconds": round(time.time() - t0, 2),
        }

    except Exception as e:
        logger.error(f"[{table}] failed: {e}\n{traceback.format_exc()}")
        return {
            "expected": 0,
            "updated": 0,
            "suppressed_by_updated_at": 0,
            "refs": [],
            "columns_covered": 0,
            "success": False,
            "error": str(e),
            "elapsed_time_seconds": round(time.time() - t0, 2),
        }


# ── Orchestrator ────────────────────────────────────────────────────────────

def update_stale_data(engine, account_id, dry_run: bool = False) -> dict:
    """
    Run the stale update for one account across every maintained table, in dependency
    order. Stops at the first failure — a later table may resolve a ref_id from an
    earlier one, so continuing past a failure would write ids from a half-updated parent.
    """
    results = {
        "success": True, "successful_tables": [], "failed_table": None,
        "remaining_tables": [], "elapsed_time_seconds": 0.0, "tables": {},
    }
    t0 = time.time()
    logger.info(
        f"Stale {'check' if dry_run else 'update'} starting for account={account_id} "
        f"({len(cfg.STALE_TABLES)} tables, "
        f"require_newer_updated_at={settings.REQUIRE_NEWER_UPDATED_AT})"
    )

    for i, table in enumerate(cfg.STALE_TABLES):
        result = update_table(engine, table, account_id, dry_run)
        results["tables"][table] = result
        logger.info(
            f"[{table}] completed in {result['elapsed_time_seconds']}s — "
            f"expected={result['expected']}, updated={result['updated']}, "
            f"success={result['success']}"
        )

        if not result["success"]:
            results["success"] = False
            results["failed_table"] = table
            results["remaining_tables"] = cfg.STALE_TABLES[i + 1:]
            logger.error(
                f"Stopping at [{table}] (strict mode). "
                f"Remaining: {results['remaining_tables']}"
            )
            break

        results["successful_tables"].append(table)

    results["elapsed_time_seconds"] = round(time.time() - t0, 2)
    logger.info(
        f"Stale update done for account={account_id}: "
        f"{len(results['successful_tables'])}/{len(cfg.STALE_TABLES)} tables "
        f"in {results['elapsed_time_seconds']}s"
    )
    return results


def update_stale_data_for_accounts(engine, account_ids: list, dry_run: bool = False) -> dict:
    """Run the stale update for several accounts. One account's failure does not stop the
    others — each is independent, and the caller decides what a partial run means."""
    per_account = {}
    failed = []

    for account_id in account_ids:
        result = update_stale_data(engine, account_id, dry_run)
        per_account[account_id] = result
        if not result["success"]:
            failed.append(account_id)

    return {
        "success": not failed,
        "accounts": len(account_ids),
        "failed_accounts": failed,
        "per_account": per_account,
    }
