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

## Repository Layout

```
ReviSync/
├── handlers/               # Lambda entry points (one per stage)
│   ├── crm_to_s3.py        # Stage 1 handler
│   ├── s3_to_db.py         # Stage 2 handler
│   └── stg_to_db.py        # Stage 3 handler
│
├── crm_sync/               # Stage 1: CRM API fetchers (one module per resource)
├── db_services/            # Stage 2: S3 → staging bulk insert logic
├── stg_db_services/        # Stage 3: Staging → production table processors
│   ├── _base/              # Shared dedup utility
│   ├── order_lines/        # order_lines sub-pipeline (credit / membership / other)
│   └── reservations/       # reservations sub-pipeline (credit / membership / no-tx)
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

---

## AWS Profile

Export your AWS profile before running any deploy script:

```bash
export AWS_PROFILE=your-profile
```
