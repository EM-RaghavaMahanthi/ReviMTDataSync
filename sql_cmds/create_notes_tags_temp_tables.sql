-- TEMP tables for validating Stage 3 writes (customer_notes / customer_tags_custom /
-- customer_tag_assignments) before pointing stg_db_services/customer_notes.py and
-- stg_db_services/customer_tags/{custom,assignments}.py at the real tables.
--
-- No FK relations — just columns + the unique keys we defined:
--   customer_notes_temp:            (account_id, customer_id, note_id)
--   customer_tags_custom_temp:      (account_id, customer_id, name)
--   customer_tag_assignments_temp:  (account_id, customer_id, custom_tag_id)
--                                   (account_id, customer_id, default_tag_id)
--
-- Run directly on Aurora Postgres, e.g.: psql "$DATABASE_URL" -f sql_cmds/create_notes_tags_temp_tables.sql

DROP TABLE IF EXISTS customer_tag_assignments_temp;
DROP TABLE IF EXISTS customer_tags_custom_temp;
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

CREATE INDEX idx_customer_notes_temp_account_id  ON customer_notes_temp (account_id);
CREATE INDEX idx_customer_notes_temp_customer_id ON customer_notes_temp (customer_id);

-- ─── customer_tags_custom_temp ─────────────────────────────────────────────────
CREATE TABLE customer_tags_custom_temp (
  id              SERIAL PRIMARY KEY,
  account_id      INTEGER NOT NULL,
  customer_ref_id INTEGER NOT NULL,
  customer_id     VARCHAR(255),
  name            VARCHAR(100) NOT NULL,
  created_at      TIMESTAMP(3) NOT NULL DEFAULT NOW(),
  created_by      INTEGER NOT NULL,

  CONSTRAINT customer_tags_custom_temp_unique UNIQUE (account_id, customer_id, name)
);

CREATE INDEX idx_customer_tags_custom_temp_account_id      ON customer_tags_custom_temp (account_id);
CREATE INDEX idx_customer_tags_custom_temp_customer_ref_id ON customer_tags_custom_temp (customer_ref_id);

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

CREATE INDEX idx_customer_tag_assignments_temp_account_id      ON customer_tag_assignments_temp (account_id);
CREATE INDEX idx_customer_tag_assignments_temp_customer_ref_id ON customer_tag_assignments_temp (customer_ref_id);
CREATE INDEX idx_customer_tag_assignments_temp_custom_tag_id  ON customer_tag_assignments_temp (custom_tag_id);
CREATE INDEX idx_customer_tag_assignments_temp_default_tag_id ON customer_tag_assignments_temp (default_tag_id);
CREATE INDEX idx_customer_tag_assignments_temp_acct_custom    ON customer_tag_assignments_temp (account_id, custom_tag_id);
CREATE INDEX idx_customer_tag_assignments_temp_acct_default   ON customer_tag_assignments_temp (account_id, default_tag_id);
