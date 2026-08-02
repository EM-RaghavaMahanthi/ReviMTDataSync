"""
Staging step — run slot, staging DDL, and the Silver → Postgres load.

The load streams the Athena result CSV out of S3 straight into COPY, so nothing larger
than a socket buffer is ever resident. A day-wide delta across every account is far too
large to build as a Python list first: db_services/main_bulk_insert.py's own comments
record credit_transactions at ~500k rows for a single account.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import text

from core.bulk_config import settings
from s3_to_stg_bulk import athena
from s3_to_stg_bulk import config as cfg
from s3_to_stg_bulk import update_stale

logger = logging.getLogger(__name__)


# ── Run slot ────────────────────────────────────────────────────────────────
# pg_advisory_lock is not usable here: it is scoped to the connection that took it, and
# every Step Function state is a separate Lambda invocation with its own connection, so a
# lock taken by `stage` would be released before the Map ran. A row in a control table is
# durable across connections.

def ensure_run_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{cfg.RUN_TABLE}" (
              singleton  boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
              run_id     text        NOT NULL,
              started_at timestamptz NOT NULL DEFAULT now()
            )
        """))


def claim_run_slot(engine, run_id: str, force: bool = False) -> None:
    """
    Take the single run slot, or raise if a live run holds it.

    A slot older than STALE_RUN_HOURS is assumed dead and taken over, so a run that dies
    before reaching `cleanup` cannot wedge the pipeline permanently. force=True takes it
    over regardless — for an operator who knows the previous run is gone.
    """
    ensure_run_table(engine)

    where = (
        "TRUE" if force else
        f'"{cfg.RUN_TABLE}".started_at < now() - interval \'{settings.STALE_RUN_HOURS} hours\''
    )

    with engine.begin() as conn:
        affected = conn.execute(text(f"""
            INSERT INTO "{cfg.RUN_TABLE}" (singleton, run_id, started_at)
            VALUES (true, :run_id, now())
            ON CONFLICT (singleton) DO UPDATE
              SET run_id = EXCLUDED.run_id, started_at = now()
            WHERE {where}
        """), {"run_id": run_id}).rowcount

        if affected == 0:
            held = conn.execute(text(f'SELECT run_id, started_at FROM "{cfg.RUN_TABLE}"')).fetchone()
            raise RuntimeError(
                f"another bulk run holds the slot: run_id={held[0]} started_at={held[1]}. "
                f'Wait for it, or re-invoke with {{"force": true}} if you know it is dead.'
            )

    logger.info(f"[run_slot] claimed by run_id={run_id} (force={force})")


def release_run_slot(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'DELETE FROM "{cfg.RUN_TABLE}"'))
    logger.info("[run_slot] released")


# ── Staging tables ──────────────────────────────────────────────────────────

def create_staging_tables(engine) -> list:
    created = []
    with engine.begin() as conn:
        for table in cfg.STAGING_ORDER:
            conn.execute(text(cfg.staging_ddl(table)))
            created.append(cfg.staging_name(table))
    logger.info(f"[staging] created {len(created)} tables")
    return created


def drop_staging_tables(engine) -> list:
    # ALL_TABLES, not STAGING_ORDER: a table dropped from the active set by a config change
    # still has a staging table left over from the run before, and cleanup has to remove it.
    dropped = []
    with engine.begin() as conn:
        for table in cfg.ALL_TABLES:
            stg = cfg.staging_name(table)
            conn.execute(text(f'DROP TABLE IF EXISTS "{stg}"'))
            dropped.append(stg)
    logger.info(f"[staging] dropped {len(dropped)} tables")
    return dropped


# ── Athena → S3 → COPY ──────────────────────────────────────────────────────

def _select_sql(table: str, t0, t1, account_ids: list) -> str:
    """
    The Silver delta for one table: every business key that changed in (t0, t1], carrying
    that key's CURRENT value.

    Two steps on purpose. `changed` uses the window to decide WHICH keys are in scope;
    the outer query then picks each key's newest row with no upper bound.

    Deduplicating inside the window instead — the obvious one-step version — is only
    correct when t1 is now. For any historical window (which is what a rollover catch-up
    runs) a key can have a newer row after t1, and the one-step form would stage the
    superseded value. That is not merely stale: writing it stamps the target's updated_at
    with now(), and the recency gate in update_stale then blocks the real value from ever
    landing. The window says what changed; it should not say what the value is.

    The outer scan keeps the LOWER bound. A key that changed inside the window has all its
    rows at or after t0, so its newest row is > t0 too — dropping only the upper bound
    keeps Iceberg's partition pruning on everything before the window.
    """
    fqn = athena.table_ref(cfg.silver_table(table))
    select = ",\n         ".join(cfg.select_list(table))
    out_cols = ", ".join(cfg.columns(table))
    keys = cfg.key_sources(table)
    partition = ", ".join(["account_id", *keys])

    # A NULL business key cannot be joined on, so it can never be matched or usefully
    # inserted. Dropping it here also keeps the staging unique index meaningful —
    # Postgres permits unlimited NULLs in a unique index.
    common = ["account_id IS NOT NULL"] + [f"{src} IS NOT NULL" for src in keys]
    if account_ids:
        ids = ", ".join(str(int(a)) for a in account_ids)
        common.append(f"account_id IN ({ids})")

    changed_where = [
        f"silver_inserted_at >  TIMESTAMP '{athena.ts_literal(t0)}'",
        f"silver_inserted_at <= TIMESTAMP '{athena.ts_literal(t1)}'",
        *common,
    ]
    latest_where = [
        f"silver_inserted_at > TIMESTAMP '{athena.ts_literal(t0)}'",
        *common,
    ]

    join = " AND ".join(
        [f"chg.account_id = src.account_id"] + [f"chg.{k} = src.{k}" for k in keys]
    )
    sep = chr(10) + "    AND "

    return (
        f"WITH changed AS (\n"
        f"  SELECT DISTINCT account_id, {', '.join(keys)}\n"
        f"  FROM {fqn}\n"
        f"  WHERE {sep.join(changed_where)}\n"
        f"),\n"
        f"latest AS (\n"
        f"  SELECT {select},\n"
        f"         ROW_NUMBER() OVER (\n"
        f"           PARTITION BY {partition}\n"
        f"           ORDER BY silver_inserted_at DESC\n"
        f"         ) AS _rn\n"
        f"  FROM {fqn} src\n"
        f"  WHERE {sep.join(latest_where)}\n"
        f"    AND EXISTS (SELECT 1 FROM changed chg WHERE {join})\n"
        f")\n"
        f"SELECT {out_cols} FROM latest WHERE _rn = 1"
    )


class _SkipHeader:
    """
    File-like wrapper that drops the first CSV line, then passes bytes straight through.
    Lets COPY consume the Athena result stream without the whole CSV being materialised.

    Only the header is scanned for a newline, and column names never contain one, so no
    CSV quoting awareness is needed.
    """

    def __init__(self, raw):
        self._raw = raw
        self._buf = b""
        self._header_done = False

    def _drop_header(self) -> None:
        while b"\n" not in self._buf:
            chunk = self._raw.read(65536)
            if not chunk:
                # Header-only or empty result — nothing left to yield.
                self._buf = b""
                self._header_done = True
                return
            self._buf += chunk
        self._buf = self._buf[self._buf.index(b"\n") + 1:]
        self._header_done = True

    def read(self, size=-1):
        if not self._header_done:
            self._drop_header()
        if size is None or size < 0:
            out, self._buf = self._buf + self._raw.read(), b""
            return out
        if self._buf:
            out, self._buf = self._buf[:size], self._buf[size:]
            return out
        return self._raw.read(size)


def load_table(engine, table: str, t0, t1, account_ids: list = None) -> int:
    """Query one table's Silver delta and COPY it into staging. Returns the row count."""
    stg = cfg.staging_name(table)
    qid = athena.run_query(_select_sql(table, t0, t1, account_ids), label=f"stage:{table}")
    body = athena.result_body(qid)

    col_list = ", ".join(cfg.columns(table))
    raw = engine.raw_connection()
    try:
        cursor = raw.cursor()
        try:
            cursor.copy_expert(
                f'COPY "{stg}" ({col_list}) FROM STDIN WITH (FORMAT {cfg.COPY_FORMAT})',
                _SkipHeader(body),
            )
            raw.commit()
        finally:
            cursor.close()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()

    with engine.connect() as conn:
        staged = conn.execute(text(f'SELECT COUNT(*) FROM "{stg}"')).scalar()

    logger.info(f"[stage] {table} → {stg}: {staged} rows (qid={qid})")
    return staged


def load_all(engine, t0, t1, account_ids: list = None) -> dict:
    """Load every staging table. Returns {table: row_count}."""
    counts = {}
    for table in cfg.STAGING_ORDER:
        counts[table] = load_table(engine, table, t0, t1, account_ids)
    logger.info(f"[stage] total {sum(counts.values())} rows across {len(counts)} tables")
    return counts


# ── Account discovery ───────────────────────────────────────────────────────

def staged_account_ids(engine) -> list:
    """Distinct account_ids present across every staging table."""
    parts = [
        f'SELECT DISTINCT account_id FROM "{cfg.staging_name(t)}"'
        for t in cfg.STAGING_ORDER
    ]
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT account_id FROM (" + " UNION ".join(parts) + ") u "
            "WHERE account_id IS NOT NULL ORDER BY account_id"
        )).fetchall()
    return [int(r[0]) for r in rows]


def active_account_ids(engine) -> set:
    """
    Accounts the pipeline is allowed to write to.

    Silver holds data for accounts that are no longer active (or were never onboarded on
    this environment); promoting those would create rows nothing reads.

    Deliberately NOT the same predicate as
    clients/db_client.DatabaseManager.get_active_accounts, which also requires
    crm_config IS NOT NULL. The bulk path reads Silver rather than the CRM API, so it does
    not need the API credentials crm_config holds — only a tenant that exists upstream,
    which crm_api_end_point establishes. Do not "restore" the crm_config check.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id FROM accounts
            WHERE status = 'ACTIVE'
              AND crm_api_end_point IS NOT NULL
        """)).fetchall()
    return {int(r[0]) for r in rows}


def resolve_accounts(engine, requested: list = None) -> dict:
    """
    Decide which accounts this run will touch, BEFORE anything is loaded.

    {"accounts": [...], "rejected": [{"account_id": …, "reason": …}, …]}

    An empty/absent `requested` means "every active account". Otherwise the requested list
    is intersected with the active set, and every id that falls out is named with why —
    the caller asked for it, so silently dropping it would hide a typo'd account id behind
    a successful run.

    Resolving here rather than after the load is what keeps an invalid id out of the
    Athena scan and out of staging entirely.
    """
    active = active_account_ids(engine)

    if not requested:
        accounts = sorted(active)
        logger.info(f"[accounts] no account_ids supplied — using all {len(accounts)} active")
        return {"accounts": accounts, "rejected": []}

    requested = sorted(dict.fromkeys(int(a) for a in requested))
    accounts = [a for a in requested if a in active]
    missing = [a for a in requested if a not in active]

    rejected = []
    if missing:
        # One query for the reason, so the report distinguishes "no such account" from
        # "exists but is not eligible" instead of lumping both into "skipped".
        with engine.connect() as conn:
            rows = {
                int(r[0]): (r[1], bool(r[2]))
                for r in conn.execute(text("""
                    SELECT id, status, (crm_api_end_point IS NOT NULL)
                    FROM accounts WHERE id = ANY(:ids)
                """), {"ids": missing}).fetchall()
            }
        for account_id in missing:
            if account_id not in rows:
                reason = "no such account"
            elif rows[account_id][0] != "ACTIVE":
                reason = f"status={rows[account_id][0]!r}, expected 'ACTIVE'"
            elif not rows[account_id][1]:
                reason = "crm_api_end_point IS NULL"
            else:
                reason = "not in the active set"
            rejected.append({"account_id": account_id, "reason": reason})

        logger.warning(
            f"[accounts] {len(rejected)} of {len(requested)} requested accounts rejected: "
            f"{rejected}"
        )

    if not accounts:
        raise ValueError(
            f"none of the requested accounts are active: {rejected}"
        )

    logger.info(f"[accounts] {len(accounts)} to process: {accounts}")
    return {"accounts": accounts, "rejected": rejected}


# ── Reconciliation ──────────────────────────────────────────────────────────

def _reconcile_join(table: str) -> tuple:
    """(match condition, absent test) for one table. Every reconciled target is keyed by
    (account_id, business key) — the three that are not are all in cfg.RDS_OWNED."""
    keys = cfg.key_columns(table)
    on = " AND ".join(["t.account_id = s.account_id"] + [f"t.{k} = s.{k}" for k in keys])
    return on, f"t.{keys[0]} IS NULL"


def reconcile(engine, account_ids: list, start, sample: int = 5) -> dict:
    """
    Is RDS caught up with Silver — what is missing, and what is stale?

    Two different shortfalls, and counting only the first is misleading. A row can be
    present in RDS and still be wrong:

      missing    in Silver, no matching row in the target  -> needs an INSERT (stage 2)
      differing  present but the values do not match       -> needs the stale UPDATE
      suppressed present, values differ, but the recency gate holds the update back

    `present` therefore means "a row exists", NOT "the row matches". `complete` requires
    both missing and differing to be zero.

    Same load as `stage` — same nine tables, same two-step query, same COPY into the same
    stg_*_bulk tables — but the window has no upper bound: (start, now]. It then compares
    what Silver holds against the target and counts what did not make it.

    **Staging is left populated on purpose.** The rows it just loaded are exactly the rows
    stage 2 needs to promote, so dropping them would mean loading the same window twice.
    Run `cleanup` when you are done with them, not before — and note that this REPLACES
    whatever a previous `stage` left in staging.

    Read-only against the production tables; the only writes are to staging.

    `deleted` is broken out separately: a row Silver has marked deleted that was never
    inserted is usually correct, not a miss, so counting it would create noise every run.
    """
    report = {}
    total_expected = total_missing = total_dupes = 0
    total_differing = total_suppressed = 0
    end = datetime.now(timezone.utc).replace(tzinfo=None)

    logger.info(
        f"[reconcile] window=({start.isoformat()}, {end.isoformat()}] "
        f"accounts={len(account_ids)} tables={len(cfg.STAGING_ORDER)}"
    )

    create_staging_tables(engine)
    loaded = load_all(engine, start, end, account_ids)

    with engine.connect() as conn:
        for table in cfg.STAGING_ORDER:
            stg = cfg.staging_name(table)
            tgt = cfg.target_table(table)
            keys = cfg.key_columns(table)
            on, _ = _reconcile_join(table)
            deleted = (
                "s.deleted_at IS NOT NULL" if cfg.source_of(table, "deleted_at") else "FALSE"
            )
            # One LEFT JOIN collapsed back to one row per staged row by grouping on
            # ctid. Two things this avoids: a bare LEFT JOIN multiplies the staged row by
            # however many target rows share its business key, silently inflating both the
            # total and `present`; and a correlated `(SELECT COUNT(*) …) > 1` per row to
            # detect that is quadratic — it ran fine for one account and timed out across
            # sixty. Grouping gives the match count per staged row in a single pass, which
            # is both the duplicate signal and the present/missing test.
            match = f"EXISTS (SELECT 1 FROM {tgt} t WHERE {on})"

            row = conn.execute(text(f"""
                SELECT COUNT(*)                                        AS in_silver,
                       COUNT(*) FILTER (WHERE n > 0)                   AS present,
                       COUNT(*) FILTER (WHERE n = 0 AND NOT deleted)   AS missing,
                       COUNT(*) FILTER (WHERE n = 0 AND deleted)       AS missing_deleted,
                       COUNT(*) FILTER (WHERE n > 1)                   AS duplicated
                FROM (
                    SELECT s.ctid AS rid,
                           ({deleted}) AS deleted,
                           COUNT(t.{keys[0]}) AS n
                    FROM "{stg}" s
                    LEFT JOIN {tgt} t ON {on}
                    WHERE s.account_id = ANY(:ids)
                    GROUP BY s.ctid, ({deleted})
                ) x
            """), {"ids": account_ids}).fetchone()

            in_silver, present, missing, missing_deleted, duplicated = (
                int(v or 0) for v in row
            )

            missing_keys = []
            if missing:
                key_sel = ", ".join(f"s.{k}" for k in keys)
                missing_keys = [
                    dict(zip(["account_id", *keys], m))
                    for m in conn.execute(text(f"""
                        SELECT s.account_id, {key_sel}
                        FROM "{stg}" s
                        WHERE s.account_id = ANY(:ids)
                          AND NOT {match} AND NOT ({deleted})
                        LIMIT :lim
                    """), {"ids": account_ids, "lim": sample}).fetchall()
                ]

            # Which accounts the shortfall actually falls on. A total says 1% is missing;
            # this says whether that is spread evenly or concentrated in a few accounts,
            # which is usually the difference between a systemic gap and a local one.
            per_account = {}
            if missing:
                per_account = {
                    int(r[0]): int(r[1])
                    for r in conn.execute(text(f"""
                        SELECT s.account_id, COUNT(*)
                        FROM "{stg}" s
                        WHERE s.account_id = ANY(:ids)
                          AND NOT {match} AND NOT ({deleted})
                        GROUP BY s.account_id
                        ORDER BY COUNT(*) DESC
                    """), {"ids": account_ids}).fetchall()
                }

            # What differs among the rows that ARE present. Same dry-run count the stale
            # pass uses, so reconcile and `stage` cannot disagree about what is stale.
            stale = update_stale.update_table(engine, table, account_ids, update=False)
            differing, suppressed = stale["expected"], stale["suppressed_by_updated_at"]

            report[table] = {
                "target": tgt,
                "in_silver": in_silver,
                "present": present,
                "differing": differing,
                "suppressed_by_updated_at": suppressed,
                "missing": missing,
                "missing_but_deleted": missing_deleted,
                "duplicated_in_target": duplicated,
                "missing_by_account": per_account,
                "sample_missing": missing_keys,
            }
            total_expected += in_silver
            total_missing += missing
            total_dupes += duplicated
            total_differing += differing
            total_suppressed += suppressed

            # A shortfall is a finding, not a failure — this whole action is read-only
            # against production. WARNING so it stands out without reading as a crash.
            level = (
                logger.warning if (missing or differing or duplicated) else logger.info
            )
            level(
                f"[reconcile] {table}: silver={in_silver} present={present} "
                f"missing={missing} differing={differing}"
                + (f" (+{suppressed} differ but gated)" if suppressed else "")
                + (f" (+{missing_deleted} deleted, not counted)" if missing_deleted else "")
                + (f" duplicated_in_target={duplicated}" if duplicated else "")
            )

    return {
        # Both shortfalls, not just the absent rows. Suppressed rows differ too, but the
        # recency gate is a deliberate policy choice rather than something the pipeline
        # failed to do, so it is reported beside `complete` rather than folded into it.
        "complete": total_missing == 0 and total_differing == 0,
        "accounts": account_ids,
        "window": {"start_datetime": start.isoformat(), "end_datetime": end.isoformat()},
        "loaded": loaded,
        "total_in_silver": total_expected,
        "total_missing": total_missing,
        "total_differing": total_differing,
        "total_suppressed_by_updated_at": total_suppressed,
        "total_duplicated_in_target": total_dupes,
        "tables": report,
        # Named, not omitted: "complete: true" must not be read as "everything was
        # checked". These are RDS-owned after the migration — see cfg.RDS_OWNED.
        "not_checked": sorted(cfg.RDS_OWNED),
        "staging": "left populated for promotion — run cleanup when done",
    }


def staged_counts_for_account(engine, account_id: int) -> dict:
    with engine.connect() as conn:
        return {
            t: conn.execute(
                text(f'SELECT COUNT(*) FROM "{cfg.staging_name(t)}" WHERE account_id = :a'),
                {"a": account_id},
            ).scalar()
            for t in cfg.STAGING_ORDER
        }
