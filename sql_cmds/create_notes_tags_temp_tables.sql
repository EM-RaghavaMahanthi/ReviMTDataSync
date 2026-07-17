-- TEMP tables for validating Stage 3 writes (customer_notes / customer_tags_default /
-- customer_tag_assignments) before pointing stg_db_services/customer_notes.py and
-- stg_db_services/customer_tags/{default,assignments}.py at the real tables.
--
-- customer_tags_default now holds ALL tag definitions (both MT tag_type='system' and
-- tag_type='manual') — customer_tags_custom is no longer populated by this sync, so its temp
-- table isn't created here either. custom_tag_id stays NULL on every assignments row.
--
-- No FK relations, no indexes — just columns + the unique keys we defined:
--   customer_notes_temp:            (account_id, customer_id, note_id)
--   customer_tags_default_temp:     (tenant_name, crm_tag_id) — shared per tenant, NOT per account
--   customer_tag_assignments_temp:  (account_id, customer_id, custom_tag_id)
--                                   (account_id, customer_id, default_tag_id)
--
-- Run directly on Aurora Postgres, e.g.: psql "$DATABASE_URL" -f sql_cmds/create_notes_tags_temp_tables.sql

DROP TABLE IF EXISTS customer_tag_assignments_temp;
DROP TABLE IF EXISTS customer_tags_default_temp;
DROP TABLE IF EXISTS customer_notes_temp;

-- ─── customer_notes_temp ───────────────────────────────────────────────────────
CREATE TABLE customer_notes_temp (
  id              SERIAL PRIMARY KEY,
  account_id      INTEGER NOT NULL,
  customer_id     VARCHAR(255) NOT NULL,
  customer_ref_id INTEGER NOT NULL,
  note_id         VARCHAR(64),
  note            TEXT NOT NULL,
  note_datetime   TIMESTAMP(3) NOT NULL DEFAULT NOW(),
  created_at      TIMESTAMP(3) NOT NULL DEFAULT NOW(),
  created_by      INTEGER NOT NULL,
  updated_at      TIMESTAMP(3) DEFAULT NOW(),
  updated_by      INTEGER,
  deleted_at      TIMESTAMP(3),
  deleted_by      INTEGER,

  CONSTRAINT customer_notes_temp_unique UNIQUE (account_id, customer_id, note_id)
);

-- ─── customer_tags_default_temp ────────────────────────────────────────────────
-- System (tag_type='system') tag definitions, shared per tenant — no account_id column.
CREATE TABLE customer_tags_default_temp (
  id                  SERIAL PRIMARY KEY,
  name                VARCHAR(100) NOT NULL,
  tenant_name         VARCHAR(100),
  crm_tag_id          VARCHAR(100),
  crm_tag_description VARCHAR(255),
  created_at          TIMESTAMP(3) NOT NULL DEFAULT NOW(),
  created_by          INTEGER NOT NULL,
  deleted_at          TIMESTAMP(3),
  deleted_by          INTEGER,

  CONSTRAINT customer_tags_default_temp_unique UNIQUE (tenant_name, crm_tag_id)
);

-- ─── customer_tag_assignments_temp ─────────────────────────────────────────────
CREATE TABLE customer_tag_assignments_temp (
  id              SERIAL PRIMARY KEY,
  account_id      INTEGER NOT NULL,
  customer_ref_id INTEGER NOT NULL,
  customer_id     VARCHAR(255),
  custom_tag_id   INTEGER,
  default_tag_id  INTEGER,
  created_at      TIMESTAMP(3) NOT NULL DEFAULT NOW(),
  created_by      INTEGER NOT NULL,

  CONSTRAINT customer_tag_assignments_temp_custom_unique  UNIQUE (account_id, customer_id, custom_tag_id),
  CONSTRAINT customer_tag_assignments_temp_default_unique UNIQUE (account_id, customer_id, default_tag_id)
);
