# ReviSync — CRM Data Sync Pipeline

ReviSync is a three-stage AWS Lambda ETL pipeline that syncs data from a CRM system into a PostgreSQL production database. Each stage is an independent Lambda function triggered in sequence.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              REVISYNC ETL PIPELINE                                   │
└──────────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────────────────┐
  │    CRM API      │       │    Amazon S3     │       │   PostgreSQL (Main DB)      │
  │   (External)    │       │   (Data Lake)    │       │   (Production Tables)       │
  └────────┬────────┘       └────────┬─────────┘       └──────────────┬──────────────┘
           │                         │                                 │
           │  Stage 1                │  Stage 2                        │  Stage 3
           ▼                         ▼                                 ▼
  ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────────────────┐
  │   crm_to_s3     │──────▶│   s3_to_db      │──────▶│        stg_to_db            │
  │                 │       │                 │       │                             │
  │ Fetches data    │       │ Reads parquet   │       │ Promotes staging rows into  │
  │ from CRM API    │       │ from S3 →       │       │ final production tables     │
  │ → saves as      │       │ bulk inserts    │       │ (dedup + insert new only)   │
  │ parquet on S3   │       │ into staging    │       │                             │
  │                 │       │ tables          │       │                             │
  └─────────────────┘       └─────────────────┘       └─────────────────────────────┘

  Lambda:  revi-syncdata-test    revi-crm-db-sync        revi-data-sync-stg-to-db
  Handler: crm_to_s3             s3_to_db                stg_to_db
           .lambda_handler       .lambda_handler         .lambda_handler
```

---

## The Three Stages

### Stage 1 — CRM → S3 (`revi-syncdata-test`)

Fetches raw data from the CRM REST API and saves it as Parquet files in S3.

- **Handler:** `handlers.crm_to_s3.lambda_handler`
- **Trigger:** Manual / scheduled invocation with `account_id`, `api_base_url`, `location_id`, `resource`
- **Resources supported:** `customers`, `orders`, `order_lines`, `class_sessions`, `reservations`, `credit_transactions`, `membership_instances`, `membership_transactions`
- **Fan-out pattern:** User-keyed resources (`credit_transactions`, `membership_instances`, `membership_transactions`) fan out into child Lambda invocations — one per batch of users — for parallel processing
- **Output:** Parquet files written to `s3://<bucket>/<account_id>/<resource>/`

### Stage 2 — S3 → Staging DB (`revi-crm-db-sync`)

Reads Parquet files from S3 and bulk-inserts them into PostgreSQL staging tables.

- **Handler:** `handlers.s3_to_db.lambda_handler`
- **Trigger:** After Stage 1 completes; receives `account_id`, `location_id`
- **Mechanism:** PostgreSQL `COPY` via `psycopg2.copy_expert` for high-throughput bulk loads
- **Tables touched:** 10 staging tables (prefixed `mt_*`)
- **Stale check:** Optional post-load stale-data reconciliation pass (`CHECK_STALE_DATA` setting)

### Stage 3 — Staging → Main DB (`revi-data-sync-stg-to-db`)

Promotes data from staging tables into the final production tables with full deduplication.

- **Handler:** `handlers.stg_to_db.lambda_handler`
- **Trigger:** After Stage 2 completes; receives `account_id`, `location_id`
- **Processing order (strict, stops on first failure):**

  | # | Table |
  |---|-------|
  | 01 | customers |
  | 02 | class_sessions |
  | 03 | membership_instances |
  | 04 | credit_transactions |
  | 04a | credit_transactions_orders |
  | 05 | membership_transactions |
  | 05a | membership_transactions_orders |
  | 06 | orders |
  | 07 | order_lines |
  | 08 | reservations |

- **Each table follows a 6-step process:** dedup staging → count → validate → insert new records only
- **Post-processing:** Optional API call to sync downstream systems (`IS_POST_PROCESS` setting)

---

## The Bulk Delta Pipeline (all accounts, one time window)

The three stages above onboard **one account, one location** at a time from a full CRM
pull. The bulk pipeline is the delta counterpart: it reads the **Silver Iceberg namespace**
instead of the CRM, covers **every account in one run**, and is scoped by a **time window**
rather than an account id.

```
┌──────────────────────────────┐      ┌───────────────────────────┐      ┌──────────────────────┐
│  Silver (Iceberg, Athena)    │      │  stg_*_bulk staging       │      │  Production tables   │
│  silver_inserted_at ∈ (t0,t1]│      │  (13 tables, UNLOGGED)    │      │                      │
└──────────────┬───────────────┘      └─────────────┬─────────────┘      └──────────┬───────────┘
               │  s3_to_stg_bulk                    │  stg_to_main_bulk             │
               ▼                                    ▼                               ▼
  ┌─────────────────────────────┐        ┌────────────────────────────┐   Step Functions Map,
  │ Athena SELECT → result CSV  │        │ the same twelve processors  │   one branch per account
  │ streamed into COPY,         │───────▶│ as Stage 3, account-scoped  │
  │ then the widened            │        │ only (no location), then    │
  │ update_stale for every      │        │ customer class dates        │
  │ account in the window       │        │                             │
  └─────────────────────────────┘        └────────────────────────────┘

  Lambda:  revi-bulk-s3-to-stg                  revi-bulk-stg-to-main
  Handler: handlers.s3_to_stg_bulk              handlers.stg_to_main_bulk
           .lambda_handler                      .lambda_handler
```

### Stage A — Silver → staging (`s3_to_stg_bulk`)

Action-routed; the Step Function sequences the actions.

| Action | Does |
|---|---|
| `stage` (default) | Claim the run slot, create the 13 `stg_*_bulk` tables, load each one from Silver for the window, run `update_stale` per account, return the promotable `account_ids` |
| `update_stale` | The stale pass on its own — for one account or a list |
| `cleanup` | Drop the staging tables, release the run slot |
| `verify` | Assert every target table exists and Athena is configured. Writes nothing |

- **Window**: `{"start_datetime": …, "end_datetime": …}`, or `{"delta_minutes": 90}`, else the last
  `BULK_DELTA_MINUTES`. Half-open: `silver_inserted_at > t0 AND <= t1`.
- **Dedup**: Silver is append-only, so each table is reduced to the newest row per
  `(account_id, business key)` inside the window before loading.
- **Streaming**: the Athena result CSV is piped from S3 into `COPY` — a day-wide delta over
  `credit_transactions` is far too large to materialise in the Lambda.
- **Global accounts**: the account list is *discovered* — the distinct `account_id`s present
  in staging, intersected with `accounts` where `status = 'ACTIVE'` and the CRM columns are
  set (the same definition `DatabaseManager.get_active_accounts` uses). Staged accounts
  that are not active are reported in `skipped_inactive_accounts`, never silently dropped.
- **Concurrency**: a single run slot in the `stg_bulk_run` control table. A slot older than
  `STALE_RUN_HOURS` is assumed dead and taken over; `{"force": true}` takes it regardless.

### `update_stale`, widened

`db_services/update_stale.py` (onboarding) hand-picks a few columns per table — `orders`
gets `date_placed` + `status`, `order_lines` gets `title` + `transaction_type` — so a change
to any other column was never propagated. The bulk version derives its column list from the
table spec instead: **119 columns across 11 tables**, up from 34 across 9.

Also different, deliberately:

- `deleted_at` / `deleted_by` are included, which is how soft deletes propagate.
- NOT NULL target columns use `COALESCE(staging, target)`, so a NULL from Silver reads as
  "no information" and can never wipe a live value or violate a constraint.
- Timestamps keep the 0.1s epoch tolerance — `IS DISTINCT FROM` on `timestamp(3)` reports a
  difference on every run.
- `ref_id` repair is spec-driven (`REF_SPECS`), so the ones the onboarding version never
  repaired — `order_lines.order_ref_id`, `reservations.class_session_ref_id`,
  `customer_notes.customer_ref_id` — are covered. Columns the backend marks `@unique` are
  guarded so a repair cannot raise a unique violation.
- Columns the backend owns are explicitly **not** written: `customers.state_id` (an FK into
  `states` assigned backend-side) and `last_class_date` / `next_class_date` (recomputed from
  the promoted reservations by Stage B, so overwriting them from Silver would make the two
  passes fight).
- `REQUIRE_NEWER_UPDATED_AT` (default true) keeps the onboarding gate
  `staging.updated_at > target.updated_at`. Be aware it suppresses most real updates — the
  target's clock is a backend *write* clock while Silver's comes from MarianaTek. The
  suppressed count is logged next to the differing count so a dry run shows the gap.

### Stage B — staging → production (`stg_to_main_bulk`)

The twelve Stage 3 processors, copied and generalized: `(account_id, location_id, engine)`
becomes `(account_id, engine)` and ~190 `location` / `location_id` predicates are gone,
because the bulk staging tables are unique on `(account_id, business key)` and one pass
covers all of an account's locations.

| Action | Does |
|---|---|
| `promote` (default) | Duplicate-parent pre-flight, then the twelve processors for one account, then that account's customer class dates |
| `vacuum` | One `VACUUM ANALYZE` pass over the target tables, before the Map fans out |
| `verify` | The duplicate-parent pre-flight alone. Writes nothing |

Two consequences of dropping location scoping and of Silver's shape, both worth knowing:

- **Duplicate parents are refused, not silently fanned out.** The onboarding pipeline scopes
  its "already exists" checks per location, so it can create two `customers` rows for one
  `customer_id` under two locations of an account. Location-free `INNER JOIN`s onto such a
  pair would insert the child row twice, so `promote` checks the eight parent business keys
  first and raises with the offending table and count. Re-invoke with
  `{"allow_duplicate_parents": true}` to proceed anyway. Dropping the location predicate
  also means the pipeline can no longer *create* such duplicates.
- **`is_valid` / `child_orders` cannot be computed.** Silver carries neither column, so the
  deferred-payment placeholder detection in `db_services/main_bulk_insert.validate_order_lines`
  has no input here. `is_valid` is staged as a constant `TRUE`, and invalidation instead
  arrives as `deleted_at IS NOT NULL` through the stale update.
- **The credit / membership pairs read their own staging tables.** Onboarding stages one
  shared table per pair and splits it with `isin_order_line` / `isin_reservation`; Silver
  ships the two sides as separate tables, so those filters are gone and each processor reads
  the side that belongs to it (`order_lines` → the `_orders` variants, `reservations` → the
  plain ones).

### Running it

```bash
# whole pipeline (Step Functions) — see step_functions/README.md
aws stepfunctions start-execution --state-machine-arn <arn> --input '{"delta_minutes": 90}'

# one stage, directly
aws lambda invoke --function-name revi-bulk-s3-to-stg \
  --payload '{"action":"verify"}' /tmp/out.json

aws lambda invoke --function-name revi-bulk-s3-to-stg \
  --payload '{"action":"stage","start_datetime":"2026-07-30T00:00:00Z","end_datetime":"2026-07-31T00:00:00Z"}' /tmp/out.json

aws lambda invoke --function-name revi-bulk-stg-to-main \
  --payload '{"action":"promote","account_id":1410}' /tmp/out.json
```

A dry run changes nothing and reports what it would change:
`{"action":"stage","delta_minutes":90}` (dry run; add `"update":true` to write).

---

## Repository Layout

```
ReviSync/
├── handlers/               # Lambda entry points (one per stage)
│   ├── crm_to_s3.py        # Stage 1 handler
│   ├── s3_to_db.py         # Stage 2 handler
│   ├── stg_to_db.py        # Stage 3 handler
│   ├── s3_to_stg_bulk.py   # Bulk Stage A handler (action-routed)
│   └── stg_to_main_bulk.py # Bulk Stage B handler (action-routed)
│
├── crm_sync/               # Stage 1: CRM API fetchers (one module per resource)
├── db_services/            # Stage 2: S3 → staging bulk insert logic
├── stg_db_services/        # Stage 3: Staging → production table processors
│   ├── _base/              # Shared dedup utility
│   ├── order_lines/        # order_lines sub-pipeline (credit / membership / other)
│   └── reservations/       # reservations sub-pipeline (credit / membership / no-tx)
│
├── s3_to_stg_bulk/         # Bulk Stage A: Silver → staging + widened update_stale
│   ├── config.py           # TABLE_SPECS — the single source of truth for the bulk load
│   ├── athena.py           # Athena query execution + result location
│   ├── staging.py          # run slot, staging DDL, Athena → COPY, account discovery
│   └── update_stale.py     # all-column stale update + spec-driven ref_id repair
├── stg_to_main_bulk/       # Bulk Stage B: same processors, account-scoped only
├── step_functions/         # Bulk pipeline state machine (ASL)
│
├── core/                   # Config, logging, settings
├── clients/                # DB client, API client, auth clients
├── schemas/                # Pydantic data models
├── sql_cmds/               # SQL files (DDL for staging tables)
├── utils/                  # Shared utilities
│
└── deployments/            # Deploy scripts — one per Lambda
    ├── deploy_crm_to_s3.sh
    ├── deploy_s3_to_db.sh
    └── deploy_stg_to_main.sh
```

---

## Deployment

Each Lambda has its own deploy script in `deployments/`. See [deployments/README.md](deployments/README.md) for full instructions.

Quick deploy from repo root:

```bash
# Stage 1
./deployments/deploy_crm_to_s3.sh

# Stage 2
./deployments/deploy_s3_to_db.sh

# Stage 3
./deployments/deploy_stg_to_main.sh

# Bulk delta pipeline
ATHENA_OUTPUT_LOCATION=s3://my-athena-results/bulk/ ./deployments/deploy_s3_to_stg_bulk.sh
./deployments/deploy_stg_to_main_bulk.sh
```

Override the Lambda name for a one-off deploy:

```bash
LAMBDA_NAME=my-other-function ./deployments/deploy_crm_to_s3.sh
```

---

## Environment Variables

All config lives in Lambda environment variables managed in the AWS Console (not committed to this repo).

| Variable | Used by | Description |
|---|---|---|
| `DATABASE_URL` | Stage 2, 3 | PostgreSQL connection string |
| `S3_BUCKET` | Stage 2 | Source S3 bucket for parquet files |
| `CHECK_STALE_DATA` | Stage 2 | Enable stale-data reconciliation pass |
| `IS_POST_PROCESS` | Stage 3 | Enable downstream API sync after insert |
| `TOKEN_SERVICE_LAMBDA_NAME` | Stage 3 | Lambda name for Auth0 token retrieval |
| `teams_enabled` | Stage 1 | Enable Microsoft Teams notifications |
| `API_BATCH_SIZE` | Stage 1 | User batch size for fan-out invocations |
| `REGION` | Bulk A | AWS region for Athena / S3 (default `us-east-1`) |
| `SILVER_NAMESPACE` | Bulk A | Iceberg namespace holding the Silver tables (default `silver`) |
| `ATHENA_CATALOG` | Bulk A | Athena catalog (default `awsdatacatalog`) |
| `ATHENA_WORKGROUP` | Bulk A | Athena workgroup (default `primary`) |
| `ATHENA_OUTPUT_LOCATION` | Bulk A | `s3://…` for query results — **required**, no default |
| `ATHENA_TIMEOUT_SECONDS` | Bulk A | Per-query poll timeout (default 600) |
| `BULK_DELTA_MINUTES` | Bulk A | Default window length when the event gives none (default 60) |
| `REQUIRE_NEWER_UPDATED_AT` | Bulk A | Gate the stale update on `updated_at` (default true) |
| `STALE_RUN_HOURS` | Bulk A | Age at which a run slot may be taken over (default 6) |
| `IS_POST_PROCESS` | Bulk B | Downstream EZTexting sync — **default false** for bulk |

---

## AWS Profile

Export your AWS profile before running any deploy script:

```bash
export AWS_PROFILE=your-profile
```
