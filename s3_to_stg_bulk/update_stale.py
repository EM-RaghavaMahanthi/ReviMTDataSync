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
4. **`location` is gone from the join conditions, and so is the per-account loop.**
   Staging is unique on (account_id, business key), so a delta spanning several locations
   needs no per-location loop; and every statement is scoped with
   `account_id = ANY(:account_ids)` rather than run once per account. One table is two
   statements for the whole run, not two per account. Per-account numbers survive via a
   GROUP BY in _count, and per-account failure isolation via _with_fallback.
5. **ref_id repair is spec-driven** (config.REF_SPECS) instead of hand-written per table,
   but covers the same four columns the onboarding version repairs and no more. It is not
   a general FK repair pass: a ref_id is only re-pointed when this update moved the
   business key it mirrors. Every other key is frozen via `no_update` instead, so it
   cannot drift in the first place — see the REF_SPECS comment for the invariant. Ref
   columns the backend marks @unique are guarded so the repair cannot raise a unique
   violation.
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


# ── Bulk execution with per-account fallback ────────────────────────────────

def _with_fallback(label: str, run, account_ids: list) -> tuple:
    """
    Run `run(account_ids)` as a single statement; on failure, retry one account at a time.

    (rows_affected, failures) where failures is [{"account_id": …, "error": …}].

    The fast path is one statement for the whole set — that is the point of the bulk
    pipeline. The slow path only runs when the fast one raised, and exists so a single bad
    account is named and quarantined instead of taking the table down for everyone.

    On a healthy run the fallback never fires and costs nothing. It fires rarely by
    construction: staging columns are all NULLable and typed exactly like the target, so
    COPY already rejected anything malformed; NOT NULL columns are COALESCE-guarded; and
    the unique-violation risk is confined to the two guarded ref columns. What is left —
    deadlock, timeout, lost connection — is not account-specific, so the retry will usually
    fail the same way and simply report which accounts were affected.
    """
    try:
        return run(account_ids), []
    except Exception as e:
        logger.warning(
            f"[{label}] bulk statement failed over {len(account_ids)} accounts ({e}) — "
            f"retrying per account to isolate the cause"
        )

    rows, failures = 0, []
    for account_id in account_ids:
        try:
            rows += run([account_id])
        except Exception as e:
            logger.error(f"[{label}] account={account_id} failed: {e}")
            failures.append({"account_id": account_id, "error": str(e)})
    return rows, failures


# ── Column updates ──────────────────────────────────────────────────────────

def _count(engine, table: str, cols: list, account_ids: list) -> dict:
    """
    Row counts for one table across every account, in a single grouped query.

    {"differing": n, "suppressed": n, "per_account": {account_id: differing}}

    `differing` respects the recency gate — it is what the UPDATE will touch.
    `suppressed` differs in value but is held back by the gate, reported separately so a
    dry run shows how much the gate is hiding.

    Grouping by account_id is what keeps per-account reporting after the switch to bulk
    statements: one query, not one query per account.
    """
    stg = cfg.staging_name(table)
    tgt = cfg.target_table(table)
    gate = _recency_gate(table)

    sql = f"""
        SELECT
          t.account_id,
          COUNT(*) FILTER (WHERE {gate})       AS differing,
          COUNT(*) FILTER (WHERE NOT ({gate})) AS suppressed
        FROM {tgt} t
        JOIN "{stg}" s
          ON {_join_on(table)}
        WHERE t.account_id = ANY(:account_ids)
          AND {_diff_predicate(table, cols)}
        GROUP BY t.account_id
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"account_ids": account_ids}).fetchall()

    return {
        "differing": sum(int(r[1] or 0) for r in rows),
        "suppressed": sum(int(r[2] or 0) for r in rows),
        "per_account": {int(r[0]): int(r[1] or 0) for r in rows if r[1]},
    }


def _update(engine, table: str, cols: list, account_ids: list) -> int:
    stg = cfg.staging_name(table)
    tgt = cfg.target_table(table)

    sql = f"""
        UPDATE {tgt} t
        SET
          {_set_clause(table, cols)}
        FROM "{stg}" s
        WHERE {_join_on(table)}
          AND t.account_id = ANY(:account_ids)
          AND {_recency_gate(table)}
          AND {_diff_predicate(table, cols)}
    """
    with engine.begin() as conn:
        return conn.execute(text(sql), {"account_ids": account_ids}).rowcount


# ── ref_id repair ───────────────────────────────────────────────────────────

def _repair_ref(engine, table: str, ref, account_ids: list, update: bool) -> dict:
    """
    Re-point one ref column after this run moved the business key it mirrors.

    Only the refs whose child key is mutable reach here — see config.REF_SPECS.

    Two things about the shape, both taken from db_services/update_stale.update_reservations:

    * **The parent is resolved from the TARGET's business key, not staging's.** Resolving
      from staging would decouple the two: the main UPDATE is gated on recency and on the
      value actually differing, so when it declines to fire, the target keeps the old key
      while the ref would jump to the new parent — a row pointing at a parent its own key
      does not name. Reading t.<child_key> makes the repair idempotent and correct whether
      or not the main update fired.
    * **Staging is still joined, purely to scope the repair to this delta window.** Without
      it the statement would sweep every row in the account and stamp updated_at on rows
      the window never touched.

    A plain equi-join onto the parent, no LATERAL: the parents involved
    (credit_transactions, membership_transactions, membership_instances) are keyed by their
    own transaction ids, so the duplicate-parent hazard that applies to `customers` does not
    arise here.
    """
    stg = cfg.staging_name(table)
    tgt = cfg.target_table(table)
    parent = cfg.target_table(ref.parent)

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

    # s = scope to the staged delta; par = the parent t's own key names.
    filters = f"""
          AND par.account_id = t.account_id
          AND par.{ref.parent_key} = t.{ref.child_key}
          AND t.account_id = ANY(:account_ids)
          AND t.{ref.col} IS DISTINCT FROM par.id{unique_guard}
    """

    count_sql = f"""
        SELECT COUNT(*)
        FROM {tgt} t
        JOIN "{stg}" s ON {_join_on(table)}
        JOIN {parent} par ON TRUE
        WHERE TRUE
          {filters}
    """

    with engine.connect() as conn:
        expected = conn.execute(
            text(count_sql), {"account_ids": account_ids}
        ).scalar() or 0

    if expected == 0 or not update:
        return {"column": ref.col, "expected": expected, "updated": 0, "failures": []}

    update_sql = f"""
        UPDATE {tgt} t
        SET {ref.col} = par.id,
            updated_by = {cfg.ACTOR_ID},
            updated_at = now()
        FROM "{stg}" s, {parent} par
        WHERE {_join_on(table)}
          {filters}
    """

    def _run(ids):
        with engine.begin() as conn:
            return conn.execute(text(update_sql), {"account_ids": ids}).rowcount

    updated, failures = _with_fallback(f"{table}.{ref.col}", _run, account_ids)

    logger.info(f"  [{table}] {ref.col} → {parent}.id: {updated} rows repaired")
    return {
        "column": ref.col, "expected": expected, "updated": updated, "failures": failures,
    }


# ── Per-table entry point ───────────────────────────────────────────────────

def update_table(engine, table: str, account_ids: list, update: bool = False) -> dict:
    """
    Update every mutable column, then repair every ref_id, for one table across every
    account — two statements, not two per account.
    """
    logger.info(f"[{table}] checking stale records across {len(account_ids)} accounts...")
    t0 = time.time()
    try:
        cols = _mutable(engine, table)
        counts = _count(engine, table, cols, account_ids)
        differing, suppressed = counts["differing"], counts["suppressed"]
        logger.info(
            f"[{table}] {differing} stale records found"
            + (f" ({suppressed} more differ but are older than the target row)"
               if suppressed else "")
        )

        updated, failures = 0, []
        if differing and update:
            updated, failures = _with_fallback(
                table,
                lambda ids: _update(engine, table, cols, ids),
                account_ids,
            )
            if not failures and updated != differing:
                # Concurrent writes can move the boundary between COUNT and UPDATE. Worth
                # seeing, not worth failing the table over.
                logger.warning(f"[{table}] updated {updated} rows, expected {differing}")

        ref_results = [
            _repair_ref(engine, table, ref, account_ids, update)
            for ref in cfg.refs(table)
        ]
        for r in ref_results:
            failures.extend(r["failures"])

        return {
            "expected": differing,
            "updated": updated,
            "suppressed_by_updated_at": suppressed,
            "per_account_expected": counts["per_account"],
            "refs": ref_results,
            "columns_covered": len(cols),
            "failed_accounts": sorted({f["account_id"] for f in failures}),
            "failures": failures,
            "success": not failures,
            "elapsed_time_seconds": round(time.time() - t0, 2),
        }

    except Exception as e:
        # Reaching here means the table failed for reasons no per-account retry can
        # attribute — a bad column list, staging missing, the count query itself failing.
        logger.error(f"[{table}] failed: {e}\n{traceback.format_exc()}")
        return {
            "expected": 0,
            "updated": 0,
            "suppressed_by_updated_at": 0,
            "per_account_expected": {},
            "refs": [],
            "columns_covered": 0,
            "failed_accounts": list(account_ids),
            "failures": [{"account_id": None, "error": str(e)}],
            "success": False,
            "error": str(e),
            "elapsed_time_seconds": round(time.time() - t0, 2),
        }


# ── Orchestrator ────────────────────────────────────────────────────────────

def update_stale_data(engine, account_ids, update: bool = False) -> dict:
    """
    Run the stale update across every maintained table for every account at once.

    `update` defaults to False — the default call is a dry run that counts what WOULD
    change and writes nothing. Pass update=True to actually write. This matches
    db_services/update_stale.update_stale_data, and it is the safe default for a pass that
    can touch six figures of production rows.

    Failure handling is per table, then per account: a table whose bulk statement raises
    retries account by account (see _with_fallback), so one bad account is quarantined
    rather than losing the table for everyone. A table that fails outright does NOT stop
    the run — unlike the onboarding version, which had to stop because a later table could
    resolve a ref_id through an earlier one's freshly written values. That coupling is gone:
    every repair resolves against the parent's `id`, which no update in this pass ever
    changes. So the remaining tables are independent and worth attempting.
    """
    if isinstance(account_ids, int):
        account_ids = [account_ids]
    account_ids = [int(a) for a in account_ids]

    results = {
        "success": True, "successful_tables": [], "failed_tables": [],
        "accounts": len(account_ids), "failed_accounts": [],
        "elapsed_time_seconds": 0.0, "tables": {},
    }
    t0 = time.time()
    logger.info(
        f"Stale {'update' if update else 'check (dry run)'} starting for "
        f"{len(account_ids)} accounts across {len(cfg.STALE_TABLES)} tables "
        f"(require_newer_updated_at={settings.REQUIRE_NEWER_UPDATED_AT})"
    )

    failed_accounts = set()
    for table in cfg.STALE_TABLES:
        result = update_table(engine, table, account_ids, update)
        results["tables"][table] = result
        logger.info(
            f"[{table}] completed in {result['elapsed_time_seconds']}s — "
            f"expected={result['expected']}, updated={result['updated']}, "
            f"success={result['success']}"
        )

        if result["success"]:
            results["successful_tables"].append(table)
        else:
            results["success"] = False
            results["failed_tables"].append(table)
            failed_accounts.update(a for a in result["failed_accounts"] if a is not None)

    results["failed_accounts"] = sorted(failed_accounts)
    results["elapsed_time_seconds"] = round(time.time() - t0, 2)
    logger.info(
        f"Stale {'update' if update else 'check'} done: "
        f"{len(results['successful_tables'])}/{len(cfg.STALE_TABLES)} tables, "
        f"{len(account_ids)} accounts, in {results['elapsed_time_seconds']}s"
        + (f" — failed tables: {results['failed_tables']}"
           if results["failed_tables"] else "")
    )
    return results
