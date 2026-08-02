# Deployments

Deploy scripts for the ReviSync Lambda functions. Each script packages the relevant source directories into a zip and uploads it to AWS Lambda via `aws lambda update-function-code`.

> **For the three onboarding scripts, environment variables, Lambda layers, memory/timeout, and the handler string are all managed manually in the AWS Console — not by these scripts.**
>
> **`deploy_s3_to_stg_bulk.sh` is different**: it creates the function if it is missing and manages layers, VPC, timeout, memory and environment itself, cloning runtime/layers/VPC from a reference function so nothing has to be set up by hand. See [Bulk pipeline](#bulk-pipeline) below.

---

## Scripts

| Script | Lambda Function | Handler | Manages config? |
|---|---|---|---|
| `deploy_crm_to_s3.sh` | `revi-syncdata-test` | `handlers.crm_to_s3.lambda_handler` | no — code only |
| `deploy_s3_to_db.sh` | `revi-crm-db-sync` | `handlers.s3_to_db.lambda_handler` | env only |
| `deploy_stg_to_main.sh` | `revi-data-sync-stg-to-db` | `handlers.stg_to_db.lambda_handler` | no — code only |
| `deploy_s3_to_stg_bulk.sh` | `revi-bulk-s3-to-stg` | `handlers.s3_to_stg_bulk.lambda_handler` | **yes — creates + full config** |
| `deploy_stg_to_main_bulk.sh` | `revi-bulk-stg-to-main` | `handlers.stg_to_main_bulk.lambda_handler` | not yet updated |

---

## Bulk pipeline

`deploy_s3_to_stg_bulk.sh` creates `revi-bulk-s3-to-stg` on first run and updates it thereafter.
Runtime, architecture, layers and VPC are read from `REFERENCE_LAMBDA` (default
`revi-crm-db-sync`) at deploy time rather than hardcoded, so the bulk function always matches the
rest of the pipeline. As of this writing all three onboarding functions share the same shape:

```
runtime  python3.12   arch  arm64   memory  10240 MB   ephemeral /tmp  10240 MB
layers   AWSLambdaPowertoolsPythonV3-python312-arm64:19
         psycopg2-layer-binary:3
         sql-alchemy-redis:1
         AWSSDKPandas-Python312-Arm64:20
         helper2:1
vpc      subnet-0540ad94d7f38f81f, subnet-0671c3476d35a5d82   sg-070aab3c40f308807
```

Timeout is set to 900 s (the Lambda maximum; the existing three run 735–900). Memory and ephemeral
storage are set to 10240 MB, matching all three.

Environment variables the script manages (merged, never replacing):

| Variable | Default | Meaning |
|---|---|---|
| `SILVER_NAMESPACE` | `silver` | Iceberg namespace the Athena reads target |
| `ATHENA_CATALOG` | `s3tablescatalog/revi-crm-data` | S3 Tables catalog. **Not** `awsdatacatalog` — `silver` does not exist there |
| `ATHENA_WORKGROUP` | `revi-dlk-gold` | Athena workgroup |
| `ATHENA_OUTPUT_BUCKET` | `revi-datalake-athena` | **Required** by the Lambda. Bare bucket name, not a URI |
| `ATHENA_OUTPUT_PREFIX` | `athena-results` | The Lambda composes `s3://<bucket>/<prefix>/` |
| `BULK_DELTA_MINUTES` | `10` | Window length when the event gives no `start_datetime` |
| `BULK_UPDATE` | `false` | Whether the stale pass writes. `false` = dry run |
| `DATABASE_URL` | seeded from reference | Only on create; export to override |

Three of these are coupled and easy to break independently:

- **The catalog is S3 Tables, not Glue.** The Silver Iceberg tables live in the
  `revi-crm-data` S3 table bucket; the default Glue catalog has no `silver` database at all.
  Because the catalog name contains a `/`, every SQL reference must be double-quoted —
  `athena.fqn()` does that, so `ATHENA_CATALOG` is stored bare.
- **The output prefix must be one the role grants.** `athena-access` allows only
  `revi-datalake-athena/{athena-results,temp,athena-ddl-results}/`.
- **`revi-dlk-gold` sets `EnforceWorkGroupConfiguration=true`**, so it overrides whatever
  output location the Lambda asks for and writes to its own,
  `s3://revi-datalake-athena/athena-results/` — which is exactly what the role grants, so
  this is harmless. `athena.result_location()` reads the real location back from Athena
  rather than computing it, so an override cannot cause a phantom 404.
  **Do not switch to `revi-dlk-workgroup`:** it also enforces, but points at
  `revi-datalake-athena/results/`, a prefix the role does *not* grant.

`BULK_UPDATE` is the default write mode, not a lock: an event's `update` field overrides it either
way, so a deliberate `{"update": true}` still writes while the deployed default stays a dry run.

First deploy:

```bash
export AWS_PROFILE=revi
./deployments/deploy_s3_to_stg_bulk.sh   # defaults are already correct for this account
```

`DATABASE_URL` is seeded from the reference function so the new Lambda points at the same RDS
instance; export `DATABASE_URL` to override. Re-deploys merge environment variables rather than
replacing them, so anything set in the console survives.

### VPC — nothing to change

The staging tables live in the same RDS instance the rest of the pipeline writes to, reached with
the same `DATABASE_URL` the script seeds from the reference function. Same VPC, same subnets, same
security group as the three existing Lambdas, so RDS connectivity is settled by construction.

Athena, Glue and S3 are public AWS APIs rather than in-VPC, so they need egress rather than VPC
membership — worth a look because this is the first Lambda here to call Athena or Glue. Both
subnets (`subnet-0540ad94d7f38f81f`, `subnet-0671c3476d35a5d82` in `vpc-0079d8fa1e8250e9d`) are
private with a `0.0.0.0/0` route through NAT `nat-0231f95c1d4f083be` (available), and
`sg-070aab3c40f308807` allows unrestricted egress. So they are reachable, and no VPC change is
needed to deploy or run.

One cost note, not a blocker: the VPC has no endpoints, so the Athena result CSVs `staging.py`
streams from S3 into `COPY` cross the NAT at ~$0.045/GB. An **S3 gateway endpoint** is free and
would take S3 off that path entirely — worth doing at some point for the existing Lambdas too, but
unrelated to getting this one running.

### IAM — no manual step

The function is created with **`revi-dlk-gold-lambda-exec`**, the data-lake execution role that
`reviDataInsightsAPI/revi-cloud-campaign` and `revi-cloud-sender` already reuse for exactly this
reason. It carries everything needed, so nothing has to be attached by hand:

| | |
|---|---|
| `AmazonS3TablesFullAccess` | read the Silver Iceberg tables |
| `athena-access` (inline) | Athena on the `primary` and `revi-dlk-gold` workgroups; `s3tablescatalog` data catalog; `lakeformation:GetDataAccess`; Glue reads; S3 on `revi-datalake-athena/{athena-results,temp,athena-ddl-results}/` |
| `s3tables-access` (inline) | S3 Tables data path |
| `AWSLambdaVPCAccessExecutionRole` | ENI management for the VPC config |
| `AWSLambdaBasicExecutionRole` | CloudWatch Logs |

RDS needs no IAM — it is reached over the VPC with a password in `DATABASE_URL`.

Override with `LAMBDA_ROLE_ARN=...` if you would rather not share a role across services. The
reference function's *own* role is deliberately not used: it has no Athena, Glue or S3 Tables
access at all.

### Overrides

```bash
LAMBDA_NAME=revi-bulk-s3-to-stg-dev        ./deployments/deploy_s3_to_stg_bulk.sh
REFERENCE_LAMBDA=revi-data-sync-stg-to-db  ./deployments/deploy_s3_to_stg_bulk.sh
TIMEOUT=600 MEMORY_SIZE=4096 EPHEMERAL_STORAGE=2048 ./deployments/deploy_s3_to_stg_bulk.sh
BULK_UPDATE=true BULK_DELTA_MINUTES=30     ./deployments/deploy_s3_to_stg_bulk.sh
LAMBDA_ROLE_ARN=arn:aws:iam::491085429701:role/my-bulk-role ./deployments/deploy_s3_to_stg_bulk.sh
```

---

## Prerequisites

- AWS CLI installed and configured
- `AWS_PROFILE` exported in your shell (see below)
- Run all commands from the **repo root**

---

## Usage

### Deploy Stage 1 — CRM → S3

```bash
./deployments/deploy_crm_to_s3.sh
```

Packages: `core  clients  handlers  crm_sync  schemas  utils`

### Deploy Stage 2 — S3 → Staging DB

```bash
./deployments/deploy_s3_to_db.sh
```

Packages: `core  clients  handlers  db_services  schemas  utils  sql_cmds`

### Deploy Stage 3 — Staging → Main DB

```bash
./deployments/deploy_stg_to_main.sh
```

Packages: `core  clients  handlers  stg_db_services`

---

## AWS Profile

Export your profile before running any script:

```bash
export AWS_PROFILE=your-profile
```

Or pass it inline for a one-off:

```bash
AWS_PROFILE=your-profile ./deployments/deploy_crm_to_s3.sh
```

## Overriding the Lambda Name

Deploy to a different function without editing the script:

```bash
LAMBDA_NAME=revi-syncdata-test-prod       ./deployments/deploy_crm_to_s3.sh
LAMBDA_NAME=revi-crm-db-sync-prod         ./deployments/deploy_s3_to_db.sh
LAMBDA_NAME=revi-data-sync-stg-to-db-prod ./deployments/deploy_stg_to_main.sh
```

---

## What Each Script Does

```
1. Removes any leftover build artifacts from a previous run
2. Creates a clean build directory
3. Copies only the required source directories into it
4. Zips the build directory (skipping __pycache__ and .pyc files)
5. Calls: aws lambda update-function-code --zip-file fileb://<zip>
6. Removes the build directory and zip on success
```

All scripts use `set -euo pipefail` — they exit immediately on any error.

---

## Troubleshooting

**`Directory 'X' not found`** — Run the script from the repo root, not from inside `deployments/`.

**`aws: command not found`** — Install the AWS CLI.

**`Unable to locate credentials`** — Ensure `AWS_PROFILE` is exported and the profile exists in `~/.aws/credentials`.

**`ResourceConflictException: The operation cannot be performed...`** — Another deploy or update is in progress. Wait a moment and retry.
