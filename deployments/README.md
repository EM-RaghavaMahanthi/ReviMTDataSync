# Deployments

Deploy scripts for the three ReviSync Lambda functions. Each script packages the relevant source directories into a zip and uploads it to AWS Lambda via `aws lambda update-function-code`.

> **Environment variables, Lambda layers, memory/timeout, and the handler string are all managed manually in the AWS Console — not by these scripts.**

---

## Scripts

| Script | Lambda Function | Handler |
|---|---|---|
| `deploy_crm_to_s3.sh` | `revi-syncdata-test` | `handlers.crm_to_s3.lambda_handler` |
| `deploy_s3_to_db.sh` | `revi-crm-db-sync` | `handlers.s3_to_db.lambda_handler` |
| `deploy_stg_to_main.sh` | `revi-data-sync-stg-to-db` | `handlers.stg_to_db.lambda_handler` |

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
