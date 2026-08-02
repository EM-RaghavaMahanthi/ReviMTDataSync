"""
Staging step — run slot, staging DDL, and the Silver → Postgres load.

The load streams the Athena result CSV out of S3 straight into COPY, so nothing larger
than a socket buffer is ever resident. A day-wide delta across every account is far too
large to build as a Python list first: db_services/main_bulk_insert.py's own comments
record credit_transactions at ~500k rows for a single account.
"""

import logging

from sqlalchemy import text

from core.bulk_config import settings
from s3_to_stg_bulk import athena
from s3_to_stg_bulk import config as cfg

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
    dropped = []
    with engine.begin() as conn:
        for table in cfg.STAGING_ORDER:
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
# How a staged row is matched to its target row. Ten of the thirteen targets are keyed by
# (account_id, business key) and use the default below. The notes/tags targets are not,
# so they get an explicit spec that mirrors the matching stg_to_main_bulk processor —
# assuming the default for them silently reports every row as missing.
#
#   extra   additional FROM entries, joined before the target
#   on      the staging -> target match
#   absent  true when no target row matched
#
# NOTE: both entries below are for tables in cfg.RECONCILE_SKIP, so neither is reached
# today. They are kept because the default (account_id, business key) join is actively
# wrong for these targets — customer_tags_default has no account_id column at all — and
# whoever removes a table from RECONCILE_SKIP will need them.

_RECONCILE_SPECS = {
    # customer_tags_default is shared per TENANT, not per account: no account_id column,
    # keyed by (tenant_name, crm_tag_id). Mirrors
    # stg_to_main_bulk/customer_tags/default.py step_3.
    "user_tags": {
        "extra": "",
        "on": "t.tenant_name = s.tenant_name AND t.crm_tag_id = s.tag_id",
        "absent": "t.crm_tag_id IS NULL",
    },
    # customer_tag_assignments matches on the RESOLVED default_tag_id, which comes from
    # customer_tags_default via (tenant_name, crm_tag_id) — and tenant_name is not staged
    # on customer_tags, so it has to come from the user_tags staging table. Mirrors
    # stg_to_main_bulk/customer_tags/assignments.py step_count_ready exactly, INNER joins
    # included.
    #
    # The INNER joins matter for how `staged` reads here: a staged tag with no matching
    # user_tags row, or whose definition is not in customer_tags_default, drops out of the
    # count entirely rather than being reported missing. That is deliberate — it makes the
    # denominator "rows stage 2 would attempt", so `missing` means stage 2 tried and did
    # not land it, not "this row was never insertable". For customer_tags alone, therefore,
    # `staged` is not the staging table's row count.
    "customer_tags": {
        "extra": (
            ' INNER JOIN "stg_user_tags_bulk" ut'
            "   ON ut.tag_id = s.tag_id AND ut.account_id = s.account_id"
            "  INNER JOIN customer_tags_default def"
            "   ON def.tenant_name = ut.tenant_name AND def.crm_tag_id = ut.tag_id"
        ),
        "on": (
            "t.account_id = s.account_id AND t.customer_id = s.customer_id"
            " AND t.default_tag_id = def.id"
        ),
        "absent": "t.default_tag_id IS NULL",
    },
}


def _reconcile_spec(table: str) -> dict:
    """The join spec for one table — the explicit one if it has it, else the default."""
    if table in _RECONCILE_SPECS:
        return _RECONCILE_SPECS[table]
    keys = cfg.key_columns(table)
    return {
        "extra": "",
        "on": " AND ".join(
            ["t.account_id = s.account_id"] + [f"t.{k} = s.{k}" for k in keys]
        ),
        "absent": f"t.{keys[0]} IS NULL",
    }


def reconcile(engine, account_ids: list, sample: int = 5) -> dict:
    """
    Prove every row Silver had in the window now exists in the target.

    For a rollover this is the difference between "we think it worked" and a number: an
    overlapping window makes a missed event unlikely, it does not make it verifiable.

    Runs against the staging tables, so it must be called after `stage` (and after
    promotion) but BEFORE `cleanup` — staging is the record of what Silver held for the
    window. Read-only.

    Reports per table: staged, present in target, missing, and up to `sample` missing
    business keys so a shortfall can be chased without another query.

    `deleted` is broken out separately: a staged row carrying deleted_at that never got
    inserted is usually correct behaviour, not a miss, so counting it against the total
    would create noise on every run.
    """
    report, total_staged, total_missing = {}, 0, 0

    with engine.connect() as conn:
        for table in cfg.RECONCILE_TABLES:
            stg = cfg.staging_name(table)
            tgt = cfg.target_table(table)
            keys = cfg.key_columns(table)
            spec = _reconcile_spec(table)
            join, absent, extra = spec["on"], spec["absent"], spec["extra"]
            # Only the CRM tables carry deleted_at; the notes/tags staging tables stage it
            # as NULL or not at all.
            has_deleted = "deleted_at" in cfg.columns(table)
            deleted_expr = "s.deleted_at IS NOT NULL" if has_deleted else "FALSE"
            source = f'"{stg}" s{extra} LEFT JOIN {tgt} t ON {join}'

            row = conn.execute(text(f"""
                SELECT COUNT(*)                                                  AS staged,
                       COUNT(*) FILTER (WHERE NOT ({absent}))                    AS present,
                       COUNT(*) FILTER (WHERE {absent} AND NOT ({deleted_expr})) AS missing,
                       COUNT(*) FILTER (WHERE {absent} AND ({deleted_expr}))     AS missing_deleted
                FROM {source}
                WHERE s.account_id = ANY(:ids)
            """), {"ids": account_ids}).fetchone()

            staged, present, missing, missing_deleted = (int(v or 0) for v in row)

            missing_keys = []
            if missing:
                key_sel = ", ".join(f"s.{k}" for k in keys)
                missing_keys = [
                    dict(zip(["account_id", *keys], r))
                    for r in conn.execute(text(f"""
                        SELECT s.account_id, {key_sel}
                        FROM {source}
                        WHERE s.account_id = ANY(:ids)
                          AND {absent} AND NOT ({deleted_expr})
                        LIMIT :lim
                    """), {"ids": account_ids, "lim": sample}).fetchall()
                ]

            report[table] = {
                "target": tgt,
                "staged": staged,
                "present": present,
                "missing": missing,
                "missing_but_deleted": missing_deleted,
                "sample_missing": missing_keys,
            }
            total_staged += staged
            total_missing += missing

            level = logger.error if missing else logger.info
            level(
                f"[reconcile] {table}: staged={staged} present={present} "
                f"missing={missing}"
                + (f" (+{missing_deleted} deleted, not counted)" if missing_deleted else "")
            )

    return {
        "complete": total_missing == 0,
        "accounts": account_ids,
        "total_staged": total_staged,
        "total_missing": total_missing,
        "tables": report,
        # Named, not omitted: "complete: true" must not be read as "everything was
        # checked". These are RDS-owned after the migration — see cfg.RECONCILE_SKIP.
        "not_checked": sorted(cfg.RECONCILE_SKIP),
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
