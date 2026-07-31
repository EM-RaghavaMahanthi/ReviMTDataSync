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
    The Silver delta for one table, deduplicated to the newest row per business key.

    Deduplicating inside the window is correct because Silver is append-only: if a key
    was written during the window, its newest row is also in the window.
    """
    fqn = athena.fqn(cfg.silver_table(table))
    select = ",\n         ".join(cfg.select_list(table))
    out_cols = ", ".join(cfg.columns(table))
    partition = ", ".join(["account_id", *cfg.key_sources(table)])

    where = [
        f"silver_inserted_at >  TIMESTAMP '{athena.ts_literal(t0)}'",
        f"silver_inserted_at <= TIMESTAMP '{athena.ts_literal(t1)}'",
        "account_id IS NOT NULL",
    ]
    # A NULL business key cannot be joined on, so it can never be matched or usefully
    # inserted. Dropping it here also keeps the staging unique index meaningful —
    # Postgres permits unlimited NULLs in a unique index.
    where += [f"{src} IS NOT NULL" for src in cfg.key_sources(table)]

    if account_ids:
        ids = ", ".join(str(int(a)) for a in account_ids)
        where.append(f"account_id IN ({ids})")

    return (
        f"WITH latest AS (\n"
        f"  SELECT {select},\n"
        f"         ROW_NUMBER() OVER (\n"
        f"           PARTITION BY {partition}\n"
        f"           ORDER BY silver_inserted_at DESC\n"
        f"         ) AS _rn\n"
        f"  FROM {fqn}\n"
        f"  WHERE {(chr(10) + '    AND ').join(where)}\n"
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
    Accounts the pipeline is allowed to write to — the same definition
    clients/db_client.DatabaseManager.get_active_accounts uses.

    Silver holds data for accounts that are no longer active (or were never onboarded on
    this environment); promoting those would create rows nothing reads.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id FROM accounts
            WHERE status = 'ACTIVE'
              AND crm_config IS NOT NULL
              AND crm_api_end_point IS NOT NULL
        """)).fetchall()
    return {int(r[0]) for r in rows}


def promotable_account_ids(engine) -> dict:
    """
    {"account_ids": [...], "skipped": [...]} — staged accounts split by whether they are
    active. The skipped list is returned (and logged) rather than silently dropped.
    """
    staged = staged_account_ids(engine)
    active = active_account_ids(engine)
    promotable = [a for a in staged if a in active]
    skipped = [a for a in staged if a not in active]

    if skipped:
        logger.warning(
            f"[stage] {len(skipped)} staged accounts are not active and will not be "
            f"promoted: {skipped}"
        )
    logger.info(f"[stage] {len(promotable)} promotable accounts: {promotable}")
    return {"account_ids": promotable, "skipped": skipped}


def staged_counts_for_account(engine, account_id: int) -> dict:
    with engine.connect() as conn:
        return {
            t: conn.execute(
                text(f'SELECT COUNT(*) FROM "{cfg.staging_name(t)}" WHERE account_id = :a'),
                {"a": account_id},
            ).scalar()
            for t in cfg.STAGING_ORDER
        }
