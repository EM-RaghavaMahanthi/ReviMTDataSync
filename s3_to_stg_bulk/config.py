"""
Table specifications for the bulk Silver → staging load — the single source of truth.

Everything else in this package derives from TABLE_SPECS: the staging DDL, the Athena
SELECT list, the COPY column order and the stale-update column set. Nothing is written
twice, so the steps cannot drift apart.

Column facts (business keys, NOT NULL columns, DB defaults, timestamp tolerances) were
reconciled against the backend Prisma models and the Silver Iceberg schemas
(revi-dlk-bronze/src/init_tables/silver_schemas.py) by the RDS backfill tool
(revi-dlk-bronze/src/rds_backfill/backfill_config.py) and are reused here rather than
re-derived — re-deriving them would only risk drift.

Deliberate exclusions, applied uniformly:
  id            — the target owns its own sequence; the copied stage-3 SQL never reads it
  *_ref_id      — Silver's values index Silver, not the backend. Stage 3 resolves every
                  ref_id by joining the target tables, so they are not staged at all
  child_orders  — no Silver equivalent (see is_valid note in stg_to_main_bulk)
Silver-only columns omitted because no target column exists:
  home_location (all tables), currency (orders), is_subscribed_to_email /
  email_unsubscribe_hash (customers — UI-managed)
"""

# Staging table names are fixed (not run-scoped) so every SQL statement stays static.
# Concurrent runs are prevented by the run-slot control table instead — see
# staging.claim_run_slot. They are distinct from the onboarding pipeline's
# mt_*_details_dlk tables, which that pipeline drops and recreates per run.
STAGING_PREFIX = "stg_"
STAGING_SUFFIX = "_bulk"

# Control table holding the single run slot. Outlives any one run; never dropped.
RUN_TABLE = "stg_bulk_run"

# Every UPDATE stamps updated_by with this — matches the onboarding pipeline, so bulk
# rows stay indistinguishable from rows it wrote and distinguishable from user edits.
ACTOR_ID = 1

# Athena writes NULL as an unquoted empty field, which Postgres COPY ... CSV reads back
# as NULL. A genuine empty string therefore also lands as NULL — acceptable for every
# column staged here, none of which distinguishes '' from NULL.
COPY_FORMAT = "csv"


def staging_name(table: str) -> str:
    return f"{STAGING_PREFIX}{table}{STAGING_SUFFIX}"


# ── Column type vocabulary ───────────────────────────────────────────────────
# Postgres types for the staging tables, matching sql_cmds/create_staging_tables.sql so
# the copied stage-3 SQL sees the types it was written against. Every staging column is
# NULLable even where the target is NOT NULL: a bad row must fail the targeted INSERT,
# where stage 3 filters and counts it, never the bulk COPY that would abort the table.

_TS = "timestamp(3) without time zone"
_INT = "integer"
_BOOL = "boolean"
_TEXT = "text"
_DBL = "double precision"


def _vc(n: int) -> str:
    return f"character varying({n})"


# Athena cast used when a staging column has no Silver source — an explicit cast keeps
# the column typed inside the dedup CTE instead of leaving it `unknown`.
_ATHENA_CAST = {
    _TS: "timestamp",
    _INT: "bigint",
    _BOOL: "boolean",
    _TEXT: "varchar",
    _DBL: "double",
}


def _athena_cast(pg_type: str) -> str:
    return _ATHENA_CAST.get(pg_type, "varchar")


# Audit columns shared by the ten CRM tables, in a fixed order. Silver carries all six.
_AUDIT = [
    ("created_at", _TS, "created_at"),
    ("created_by", _INT, "created_by"),
    ("updated_at", _TS, "updated_at"),
    ("updated_by", _INT, "updated_by"),
    ("deleted_at", _TS, "deleted_at"),
    ("deleted_by", _INT, "deleted_by"),
]

# The notes/tags Silver tables carry no audit columns at all — staged as NULL so the
# staging shape stays uniform and stage 3's INSERT can stamp its own values.
_AUDIT_NULL = [(name, typ, None) for name, typ, _ in _AUDIT]

# Appended to every staging table. Not a target column — it is the dedup ordering key
# and the window predicate.
_SILVER_TS = [("silver_inserted_at", _TS, "silver_inserted_at")]

# Set to now() / ACTOR_ID by the stale update rather than copied from staging, so they
# always reflect when the bulk run touched the row.
STAMPED = ("created_at", "created_by", "updated_at", "updated_by")


# ── Per-table specs ─────────────────────────────────────────────────────────
# columns:  (staging_column, pg_type, silver_expression | None) in staging / Athena
#           SELECT / COPY order. None stages a typed NULL.
# key:      business key — str, or tuple for a composite. Staging gets
#           UNIQUE (account_id, *key).
# ts_cols:  timestamp columns the stale update compares with an epoch delta rather than
#           IS DISTINCT FROM. timestamp(3) rounding otherwise reports a difference on
#           every run (keeps the onboarding pipeline's 0.1s tolerance).
# required: columns that must be non-NULL for the row to be usable — account_id, the
#           business key, and any genuinely NOT NULL target column with no default.
# defaults: target columns that are NOT NULL *with* a DB default. A NULL from Silver
#           means "no information", so the stale update leaves the live value alone.
# no_update: staged (so a future INSERT path can read them) but never overwritten:
#           staging-only columns the target does not have (order_lines.is_valid /
#           child_orders, orders.location_id / parent_order, customers.tags), and
#           columns the backend owns (customers.state_id — an FK into `states` assigned
#           backend-side; last_class_date / next_class_date — recomputed from the
#           promoted reservations by stg_to_main_bulk, so overwriting them from Silver
#           would just make the two passes fight).
# stale:    whether update_stale maintains this table (see STALE_TABLES below).

_CUSTOMERS = {
    "silver": "customers",
    "key": "customer_id",
    "columns": [
        ("customer_id", _vc(255), "customer_id"),
        ("location_id", _INT, "location_id"),
        ("account_id", _INT, "account_id"),
        ("first_name", _vc(64), "first_name"),
        ("last_name", _vc(64), "last_name"),
        ("email", _vc(255), "email"),
        ("full_name", _vc(255), "full_name"),
        ("birth_date", _TS, "birth_date"),
        ("birth_month", _INT, "birth_month"),
        ("birth_day", _INT, "birth_day"),
        ("phone_number", _vc(64), "phone_number"),
        ("address_line1", _TEXT, "address_line1"),
        ("address_line2", _TEXT, "address_line2"),
        ("address_line3", _TEXT, "address_line3"),
        ("city", _vc(255), "city"),
        ("country", _vc(255), "country"),
        ("state_province", _vc(255), "state_province"),
        ("customer_state", _vc(64), "customer_state"),
        ("postal_code", _vc(64), "postal_code"),
        ("gender", _vc(64), "gender"),
        ("date_joined", _TS, "date_joined"),
        ("is_opted_in_to_sms", _BOOL, "is_opted_in_to_sms"),
        ("completed_class_count", _INT, "completed_class_count"),
        # FK → states.id, mirrored from the backend's own state assignment. Any value
        # here must already exist in `states`.
        ("state_id", _INT, "state_id"),
        ("is_external_user", _BOOL, "is_external_user"),
        ("last_class_date", _TS, "last_class_date"),
        ("next_class_date", _TS, "next_class_date"),
        # No Silver source. Kept because the onboarding staging table has it and the
        # copied stage-3 SQL may reference it.
        ("tags", _TEXT, None),
        *_AUDIT,
        *_SILVER_TS,
    ],
    "ts_cols": ("birth_date", "date_joined", "last_class_date", "next_class_date"),
    "required": ("customer_id", "location_id", "account_id", "first_name", "last_name"),
    "defaults": {"is_opted_in_to_sms": "false"},
    "no_update": ("state_id", "last_class_date", "next_class_date", "tags"),
    "stale": True,
}

_CLASS_SESSIONS = {
    "silver": "class_sessions",
    "key": "class_session_id",
    "columns": [
        ("class_session_id", _TEXT, "class_session_id"),
        ("start_datetime", _TS, "start_datetime"),
        ("start_date", _TEXT, "start_date"),
        ("location", _TEXT, "location"),
        ("end_datetime", _TS, "end_datetime"),
        ("cancellation_datetime", _TS, "cancellation_datetime"),
        ("class_name", _vc(255), "class_name"),
        ("class_type_name", _vc(255), "class_type_name"),
        ("capacity", _INT, "capacity"),
        ("account_id", _INT, "account_id"),
        *_AUDIT,
        *_SILVER_TS,
    ],
    "ts_cols": ("start_datetime", "end_datetime", "cancellation_datetime"),
    "required": ("class_session_id", "account_id"),
    "defaults": {},
    "no_update": (),
    "stale": True,
}

_MEMBERSHIP_INSTANCES = {
    "silver": "membership_instances",
    "key": "membership_instances_id",
    "columns": [
        ("membership_instances_id", _INT, "membership_instances_id"),
        ("purchase_date", _TS, "purchase_date"),
        ("membership_name", _vc(128), "membership_name"),
        ("renewal_rate_incl_tax", _vc(16), "renewal_rate_incl_tax"),
        ("status", _vc(128), "status"),
        ("location", _INT, "location"),
        ("renewal_count", _INT, "renewal_count"),
        ("next_charge_date", _TS, "next_charge_date"),
        ("account_id", _INT, "account_id"),
        *_AUDIT,
        *_SILVER_TS,
    ],
    "ts_cols": ("purchase_date", "next_charge_date"),
    "required": ("membership_instances_id", "account_id"),
    "defaults": {},
    "no_update": (),
    "stale": True,
}

_ORDERS = {
    "silver": "orders",
    "key": "order_id",
    "columns": [
        ("order_id", _TEXT, "order_id"),
        ("date_placed", _TS, "date_placed"),
        ("location", _TEXT, "location"),
        # Silver orders has no location_id or parent_order column. Both are staged as
        # NULL: the copied stage-3 orders SQL reads neither from staging.
        ("location_id", _INT, None),
        ("payment_sources_labels", _vc(255), "payment_sources_labels"),
        ("status", _vc(255), "status"),
        ("order_lines_id", _vc(64), "order_lines_id"),
        ("customer_id", _vc(255), "customer_id"),
        ("parent_order", _vc(255), None),
        ("account_id", _INT, "account_id"),
        *_AUDIT,
        *_SILVER_TS,
    ],
    "ts_cols": ("date_placed",),
    "required": ("order_id", "account_id"),
    "defaults": {},
    # customer_id is frozen: orders.customer_ref_id mirrors it, and this pipeline does not
    # repair customer_ref_id (see REF_SPECS). Freezing the key keeps the pair consistent.
    "no_update": ("location_id", "parent_order", "customer_id"),
    "stale": True,
}

# credit_transactions and credit_transactions_orders are separate Silver tables, so each
# gets its own staging table. The onboarding pipeline shares one staging table and splits
# it with isin_reservation / isin_order_line; those columns do not exist in Silver and
# are not needed — see stg_to_main_bulk/README notes.
_CREDIT_COMMON = [
    ("credit_transactions_id", _INT, "credit_transactions_id"),
    ("transaction_date", _TS, "transaction_date"),
    ("credit_name", _vc(128), "credit_name"),
    ("is_expired", _BOOL, "is_expired"),
    ("is_intro_offer", _BOOL, "is_intro_offer"),
    ("parent_credit_transaction_type", _vc(64), "parent_credit_transaction_type"),
    ("parent_credit_transaction_id", _INT, "parent_credit_transaction_id"),
    ("customer_id", _vc(255), "customer_id"),
    ("location", _INT, "location"),
    ("account_id", _INT, "account_id"),
]

_CREDIT_TRANSACTIONS = {
    "silver": "credit_transactions",
    "key": "credit_transactions_id",
    # No remaining_credits_cache — the target credit_transactions has no such column
    # (only credit_transactions_orders does), and Silver matches.
    "columns": [*_CREDIT_COMMON, *_AUDIT, *_SILVER_TS],
    "ts_cols": ("transaction_date",),
    "required": ("credit_transactions_id", "account_id"),
    "defaults": {},
    # Frozen: customer_ref_id mirrors customer_id and is not repaired here.
    "no_update": ("customer_id",),
    "stale": True,
}

_CREDIT_TRANSACTIONS_ORDERS = {
    "silver": "credit_transactions_orders",
    "key": "credit_transactions_id",
    "columns": [
        *_CREDIT_COMMON,
        ("remaining_credits_cache", _INT, "remaining_credits_cache"),
        *_AUDIT,
        *_SILVER_TS,
    ],
    "ts_cols": ("transaction_date",),
    "required": ("credit_transactions_id", "account_id"),
    "defaults": {},
    # Frozen: customer_ref_id mirrors customer_id and is not repaired here.
    "no_update": ("customer_id",),
    "stale": True,
}

_MEMBERSHIP_TX_COMMON = [
    ("membership_transactions_id", _INT, "membership_transactions_id"),
    ("transaction_date", _TS, "transaction_date"),
    ("membership_name", _vc(128), "membership_name"),
    ("parent_membership_transaction_id", _INT, "parent_membership_transaction_id"),
    ("membership_instances_id", _INT, "membership_instances_id"),
    ("customer_id", _vc(255), "customer_id"),
    ("location", _INT, "location"),
    ("account_id", _INT, "account_id"),
]

_MEMBERSHIP_TRANSACTIONS = {
    "silver": "membership_transactions",
    "key": "membership_transactions_id",
    "columns": [*_MEMBERSHIP_TX_COMMON, *_AUDIT, *_SILVER_TS],
    "ts_cols": ("transaction_date",),
    "required": ("membership_transactions_id", "account_id"),
    "defaults": {},
    # customer_id frozen (customer_ref_id is not repaired here). membership_instances_id
    # stays mutable — membership_instances_ref_id IS repaired, matching the onboarding
    # pipeline's update_membership_transactions.
    "no_update": ("customer_id",),
    "stale": True,
}

_MEMBERSHIP_TRANSACTIONS_ORDERS = {
    "silver": "membership_transactions_orders",
    "key": "membership_transactions_id",
    # payment_interval_end_date is unique to this table.
    "columns": [
        *_MEMBERSHIP_TX_COMMON,
        ("payment_interval_end_date", _TS, "payment_interval_end_date"),
        *_AUDIT,
        *_SILVER_TS,
    ],
    "ts_cols": ("transaction_date", "payment_interval_end_date"),
    "required": ("membership_transactions_id", "account_id"),
    "defaults": {},
    # customer_id is NOT frozen here: the backend's membership_transactions_orders has no
    # customer_ref_id column, so there is no mirrored ref to keep in step.
    # membership_instances_id stays mutable and its ref is repaired.
    "no_update": (),
    "stale": True,
}

_ORDER_LINES = {
    "silver": "order_lines",
    "key": "order_line_id",
    # Silver has neither child_orders nor is_valid, so the deferred-payment placeholder
    # detection (db_services/main_bulk_insert.validate_order_lines) cannot run here.
    # is_valid is staged as a constant TRUE so the copied stage-3 filters still hold, and
    # invalidation instead arrives as deleted_at IS NOT NULL via the stale update.
    "columns": [
        ("order_line_id", _vc(64), "order_line_id"),
        ("order_id", _TEXT, "order_id"),
        ("transaction_type", _vc(255), "transaction_type"),
        ("location", _vc(255), "location"),
        ("credit_transactions_id", _INT, "credit_transactions_id"),
        ("membership_transactions_id", _INT, "membership_transactions_id"),
        ("title", _vc(255), "title"),
        ("line_total", _DBL, "line_total"),
        ("processed_by", _BOOL, "processed_by"),
        ("child_orders", _TEXT, None),
        ("is_valid", _BOOL, "true"),
        ("account_id", _INT, "account_id"),
        *_AUDIT,
        *_SILVER_TS,
    ],
    "ts_cols": (),
    "required": ("order_line_id", "account_id"),
    "defaults": {"processed_by": "false"},
    # order_id / credit_transactions_id / membership_transactions_id are frozen: each is
    # mirrored by a *_ref_id this pipeline does not repair. The onboarding pipeline
    # (update_order_lines) updates title + transaction_type only, for the same reason.
    "no_update": (
        "is_valid", "child_orders",
        "order_id", "credit_transactions_id", "membership_transactions_id",
    ),
    "stale": True,
}

_RESERVATIONS = {
    "silver": "reservations",
    "key": "reservations_id",
    "columns": [
        ("reservations_id", _INT, "reservations_id"),
        ("cancel_date", _TS, "cancel_date"),
        ("check_in_date", _TS, "check_in_date"),
        ("creation_date", _TS, "creation_date"),
        ("status", _vc(16), "status"),
        ("credit_transactions_type", _vc(64), "credit_transactions_type"),
        ("credit_transactions_id", _INT, "credit_transactions_id"),
        ("membership_transactions_type", _vc(64), "membership_transactions_type"),
        ("membership_transactions_id", _INT, "membership_transactions_id"),
        ("guest", _BOOL, "guest"),
        ("customer_id", _vc(255), "customer_id"),
        ("class_session_id", _vc(255), "class_session_id"),
        ("first_timer", _BOOL, "first_timer"),
        ("reservation_type", _vc(64), "reservation_type"),
        ("prev_reservation_type", _vc(64), "prev_reservation_type"),
        ("transaction_type", _vc(64), "transaction_type"),
        ("location", _INT, "location"),
        ("account_id", _INT, "account_id"),
        *_AUDIT,
        *_SILVER_TS,
    ],
    "ts_cols": ("cancel_date", "check_in_date", "creation_date"),
    "required": ("reservations_id", "account_id"),
    # guest and first_timer are NOT NULL DEFAULT false in the target. Silver computes
    # first_timer, so NULL there means "unknown" — the live value is left alone.
    "defaults": {"guest": "false", "first_timer": "false"},
    # customer_id / class_session_id frozen — their ref_ids are not repaired here.
    # credit_transactions_id / membership_transactions_id stay mutable and their ref_ids
    # ARE repaired, matching the onboarding pipeline's update_reservations.
    "no_update": ("customer_id", "class_session_id"),
    "stale": True,
}

_USER_NOTES = {
    "silver": "user_notes",
    # Composite, matching customer_notes' idempotency key (account_id, customer_id,
    # note_id) — see stg_to_main_bulk/customer_notes.py.
    "key": ("customer_id", "note_id"),
    "columns": [
        ("note_id", _vc(64), "note_id"),
        ("account_id", _INT, "account_id"),
        ("customer_id", _vc(255), "customer_id"),
        ("note", _TEXT, "note"),
        ("note_datetime", _TS, "note_datetime"),
        ("is_pinned", _BOOL, "is_pinned"),
        ("author_id", _vc(255), "author_id"),
        # Silver user_notes carries no location or audit columns.
        ("location", _INT, None),
        *_AUDIT_NULL,
        *_SILVER_TS,
    ],
    "ts_cols": ("note_datetime",),
    "required": ("note_id", "customer_id", "account_id"),
    "defaults": {},
    # customer_notes has no is_pinned / author_id column — staged (a future schema change
    # would make them usable) but never written.
    "no_update": ("is_pinned", "author_id"),
    # The note body is editable in the CRM, so notes are worth keeping fresh. Silver has
    # no updated_at here, so the stale update falls back to silver_inserted_at for the
    # recency gate — see update_stale._recency_gate.
    "stale": True,
}

_USER_TAGS = {
    "silver": "user_tags",
    "key": "tag_id",
    "columns": [
        # Silver renamed tag_id → crm_tag_id to line up with
        # customer_tags_default.crm_tag_id; the staging column keeps the name stage 3
        # already joins on.
        ("tag_id", _vc(64), "crm_tag_id"),
        ("account_id", _INT, "account_id"),
        ("tenant_name", _vc(100), "tenant_name"),
        ("name", _vc(255), "name"),
        ("slug", _vc(255), "slug"),
        ("user_tag_type", _vc(64), "user_tag_type"),
        ("description", _TEXT, "description"),
        ("tag_type", _vc(64), "tag_type"),
        ("weight", _INT, "weight"),
        ("location", _INT, None),
        *_AUDIT_NULL,
        *_SILVER_TS,
    ],
    "ts_cols": (),
    "required": ("tag_id", "account_id"),
    "defaults": {},
    # Insert-only: customer_tags_default has no updated_at and stage 3 treats tag
    # definitions as immutable once created.
    "no_update": (),
    "stale": False,
}

_CUSTOMER_TAGS = {
    "silver": "customer_tags",
    # A customer↔tag link has no single-column business key.
    "key": ("customer_id", "tag_id"),
    "columns": [
        ("customer_id", _vc(255), "customer_id"),
        ("tag_id", _vc(64), "crm_tag_id"),
        ("account_id", _INT, "account_id"),
        # Silver's default_tag_id indexes Silver's own user_tags.id, NOT the backend's
        # customer_tags_default.id — staged as NULL and resolved DB-side by stage 3's
        # join through (tenant_name, crm_tag_id). custom_tag_id is always NULL for
        # CRM-sourced tags. tenant_name comes from the user_tags staging table.
        ("default_tag_id", _INT, None),
        ("custom_tag_id", _INT, None),
        ("tenant_name", _vc(100), None),
        ("location", _INT, None),
        ("created_at", _TS, None),
        ("created_by", _INT, None),
        ("deleted_at", _TS, "deleted_at"),
        *_SILVER_TS,
    ],
    "ts_cols": (),
    "required": ("customer_id", "tag_id", "account_id"),
    "defaults": {},
    # Insert-only: customer_tag_assignments has no updated_at. Silver's deleted_at
    # tombstone is staged but not yet propagated — see stg_to_main_bulk docs.
    "no_update": (),
    "stale": False,
}


# Load order. Roots first, then each table only after every table stage 3 resolves a
# ref_id from. Mirrors stg_to_db.PROCESSING_ORDER with customers promoted to first (the
# most-referenced root).
TABLE_SPECS: dict = {
    "customers": _CUSTOMERS,
    "class_sessions": _CLASS_SESSIONS,
    "membership_instances": _MEMBERSHIP_INSTANCES,
    "orders": _ORDERS,
    "credit_transactions": _CREDIT_TRANSACTIONS,
    "credit_transactions_orders": _CREDIT_TRANSACTIONS_ORDERS,
    "membership_transactions": _MEMBERSHIP_TRANSACTIONS,
    "membership_transactions_orders": _MEMBERSHIP_TRANSACTIONS_ORDERS,
    "order_lines": _ORDER_LINES,
    "reservations": _RESERVATIONS,
    "user_notes": _USER_NOTES,
    "user_tags": _USER_TAGS,
    "customer_tags": _CUSTOMER_TAGS,
}

STAGING_ORDER: list = list(TABLE_SPECS)

# Tables update_stale maintains, in dependency order (parents before children, so a
# ref_id repair always sees a populated parent).
STALE_TABLES: list = [t for t in STAGING_ORDER if TABLE_SPECS[t]["stale"]]


# ── Target tables ───────────────────────────────────────────────────────────
# Logical (Silver) name → production table. Identity for the ten CRM tables; the
# notes/tags trio is named differently on the backend side.

TARGET_TABLES: dict = {
    "customers": "customers",
    "class_sessions": "class_sessions",
    "membership_instances": "membership_instances",
    "orders": "orders",
    "credit_transactions": "credit_transactions",
    "credit_transactions_orders": "credit_transactions_orders",
    "membership_transactions": "membership_transactions",
    "membership_transactions_orders": "membership_transactions_orders",
    "order_lines": "order_lines",
    "reservations": "reservations",
    "user_notes": "customer_notes",
    "user_tags": "customer_tags_default",
    "customer_tags": "customer_tag_assignments",
}


def target_table(table: str) -> str:
    return TARGET_TABLES[table]


# ── ref_id resolution ───────────────────────────────────────────────────────
# (ref column on the child, parent logical table, parent business key, child column
# holding the parent's business key).
#
# This pipeline does NOT run a general-purpose FK repair pass. Silver's own *_ref_id
# values index Silver rather than the backend, so they are never staged; a row's ref_id is
# resolved once, by stg_to_main_bulk, at INSERT time. For a row whose parent pointer never
# moves there is nothing left to resolve.
#
# The one case that still needs repair is when THIS update changes the business key the
# ref mirrors: set reservations.credit_transactions_id from 100 to 200 and
# credit_transactions_ref_id still points at the row for 100.
#
# So the invariant, ported verbatim from db_services/update_stale.py, is:
#
#     repair a ref_id if and only if the business key it mirrors is mutable.
#
# Everything else is kept in step by freezing the key instead — see each spec's
# `no_update`. That is why customer_ref_id, class_session_ref_id and order_ref_id appear
# nowhere below: their keys are frozen, so they cannot drift. Keep the two sides in sync —
# making a frozen key mutable without adding its Ref here silently breaks the FK.
#
# The split between the plain and _orders variants is asymmetric, inherited from the
# onboarding pipeline:
#   order_lines  → credit_transactions_ORDERS / membership_transactions_ORDERS
#   reservations → credit_transactions        / membership_transactions

class Ref:
    __slots__ = ("col", "parent", "parent_key", "child_key")

    def __init__(self, col: str, parent: str, parent_key: str, child_key: str):
        self.col = col
        self.parent = parent
        self.parent_key = parent_key
        self.child_key = child_key


_MI_REF = ("membership_instances", "membership_instances_id", "membership_instances_id")

REF_SPECS: dict = {
    # membership_instances_id is mutable on both → its ref is repaired on both.
    "membership_transactions": [Ref("membership_instances_ref_id", *_MI_REF)],
    "membership_transactions_orders": [Ref("membership_instances_ref_id", *_MI_REF)],
    # credit_transactions_id / membership_transactions_id are mutable → both repaired.
    "reservations": [
        Ref("credit_transactions_ref_id", "credit_transactions",
            "credit_transactions_id", "credit_transactions_id"),
        Ref("membership_transactions_ref_id", "membership_transactions",
            "membership_transactions_id", "membership_transactions_id"),
    ],
}

# The backend marks these @unique, so at most one child row may hold a given parent id.
# The repair has to respect that or it raises a unique violation and takes the whole
# table's update down with it. The onboarding pipeline omits this guard; it is cheap, and
# bulk runs over far more rows at once, so the exposure is not comparable.
UNIQUE_REF_COLS: set = {
    ("reservations", "credit_transactions_ref_id"),
    ("reservations", "membership_transactions_ref_id"),
}


def refs(table: str) -> list:
    return REF_SPECS.get(table, [])


# ── Derived accessors ───────────────────────────────────────────────────────

def columns(table: str) -> list:
    """Staging column names, in Athena SELECT / COPY order."""
    return [name for name, _, _ in TABLE_SPECS[table]["columns"]]


def select_list(table: str) -> list:
    """Athena SELECT expressions, aliased to the staging column names."""
    out = []
    for name, pg_type, source in TABLE_SPECS[table]["columns"]:
        if source is None:
            out.append(f"CAST(NULL AS {_athena_cast(pg_type)}) AS {name}")
        elif source == name:
            out.append(name)
        else:
            out.append(f"{source} AS {name}")
    return out


def source_of(table: str, column: str):
    """
    The Silver-side expression behind a staging column, or None if it has no source.

    Needed wherever SQL must name the Silver column rather than the staging alias:
    Trino does not let a window function's PARTITION BY reference an output alias from
    the same SELECT, and neither does the WHERE clause.
    """
    for name, _, source in TABLE_SPECS[table]["columns"]:
        if name == column:
            return source
    raise KeyError(f"{table} has no staging column {column!r}")


def key_sources(table: str) -> list:
    """Silver expressions for the business key columns."""
    out = []
    for col in key_columns(table):
        source = source_of(table, col)
        if source is None:
            raise ValueError(f"{table} key column {col!r} has no Silver source")
        out.append(source)
    return out


def key_columns(table: str) -> list:
    """Business key as a list — a composite key has more than one entry."""
    key = TABLE_SPECS[table]["key"]
    return list(key) if isinstance(key, tuple) else [key]


def key(table: str) -> str:
    """Single-column business key. Raises for composite keys — callers that support
    them use key_columns() instead."""
    cols = key_columns(table)
    if len(cols) != 1:
        raise ValueError(f"{table} has a composite key {cols}; use key_columns()")
    return cols[0]


def ts_columns(table: str) -> tuple:
    return TABLE_SPECS[table]["ts_cols"]


def required_columns(table: str) -> tuple:
    return TABLE_SPECS[table]["required"]


def silver_table(table: str) -> str:
    return TABLE_SPECS[table]["silver"]


def mutable_columns(table: str) -> list:
    """
    Columns the stale update may overwrite.

    Excludes the join keys (account_id + business key), created_at/created_by (never
    overwritten on an existing row), updated_at/updated_by (stamped, not copied),
    silver_inserted_at (not a target column), everything in `no_update`, and the columns
    with no Silver source — staging holds NULL for those, so an update could only ever
    wipe a live value.

    update_stale intersects this with the target's real columns at run time, so a spec
    that names a column the target does not have degrades to a logged warning rather than
    a failed UPDATE.
    """
    spec = TABLE_SPECS[table]
    frozen = {
        "account_id", "silver_inserted_at",
        *key_columns(table), *STAMPED, *spec["no_update"],
    }
    return [
        name for name, _, source in spec["columns"]
        if name not in frozen and source is not None
    ]


def never_null_columns(table: str) -> set:
    """
    Columns that must never be set to NULL — `required` plus `defaults`.

    The stale update guards these with `staging IS NOT NULL`, so a NULL from Silver reads
    as "no information" rather than wiping a live value or violating a constraint.
    """
    spec = TABLE_SPECS[table]
    return set(spec["required"]) | set(spec["defaults"])


def staging_ddl(table: str) -> str:
    """
    CREATE UNLOGGED TABLE + UNIQUE INDEX for one staging table.

    UNLOGGED skips WAL for a table that is dropped at the end of the run anyway. The
    unique index on (account_id, *key) is what lets every downstream statement join
    staging without a DISTINCT ON over it.
    """
    stg = staging_name(table)
    cols = ",\n  ".join(
        f"{name} {typ}" for name, typ, _ in TABLE_SPECS[table]["columns"]
    )
    key_list = ", ".join(key_columns(table))
    return (
        f'DROP TABLE IF EXISTS "{stg}";\n'
        f'CREATE UNLOGGED TABLE "{stg}" (\n  {cols}\n);\n'
        f'CREATE UNIQUE INDEX "{stg}_key" ON "{stg}" (account_id, {key_list});'
    )
