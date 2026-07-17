#!/bin/bash
# Deploy the generic backfill Lambda — packages source, uploads to AWS, sets handler.
#
# ONE Lambda, MANY jobs: handlers/backfill/handler.py dispatches on event_type, so every
# backfill job (notes/tags today, whatever comes next) lives in this same function — adding
# a new job means adding a new handlers/backfill/<job>.py module + one _EVENT_HANDLERS entry,
# NOT a new Lambda / new deploy script. Don't name the function after one job.
#   Handler: handlers.backfill.handler.lambda_handler
#
# Creates the Lambda if it doesn't exist yet (reusing the same IAM role / VPC / layers as
# revi-data-sync-stg-to-db, since this handler needs the exact same DB access — sqlalchemy +
# psycopg2 + reach into the Aurora VPC — nothing new to provision). Otherwise just updates
# code + config on the existing function, same as the other deploy_*.sh scripts.
#
# Usage (run from repo root):
#   ./deployments/deploy_backfill_handler.sh
#   LAMBDA_NAME=other-function ./deployments/deploy_backfill_handler.sh
#
# Invoke (event_type selects the job; account_id is the current jobs' only input):
#   aws lambda invoke --function-name revi-backfill \
#     --payload '{"event_type":"backfill_notes_and_tags","account_id":4809}' out.json

set -euo pipefail

LAMBDA_NAME="${LAMBDA_NAME:-revi-backfill}"
AWS_PROFILE="${AWS_PROFILE:-raghava.revi}"
ZIP_NAME="backfill_handler_deploy.zip"
BUILD_DIR="backfill_handler_build"
# Stage-3-only: no crm_sync/db_services/clients/schemas — stg_db_services has none of those
# dependencies (pure SQLAlchemy text() queries against already-staged data).
SOURCE_DIRS=("core" "handlers" "stg_db_services")

# ── First-create-only config — reused from revi-data-sync-stg-to-db (same DB/VPC access
# needs; only used if the function doesn't exist yet). Lighter memory/timeout than that
# Lambda's 10240MB/840s since these jobs do far less (no S3, no pandas, plain SQL calls).
LAMBDA_ROLE="${LAMBDA_ROLE:-arn:aws:iam::491085429701:role/service-role/revi-data-sync-stg-to-db-role-3ewg8prs}"
LAMBDA_RUNTIME="${LAMBDA_RUNTIME:-python3.12}"
LAMBDA_ARCHITECTURE="${LAMBDA_ARCHITECTURE:-arm64}"
LAMBDA_MEMORY="${LAMBDA_MEMORY:-1024}"
LAMBDA_TIMEOUT="${LAMBDA_TIMEOUT:-300}"
LAMBDA_SUBNET_IDS="${LAMBDA_SUBNET_IDS:-subnet-0540ad94d7f38f81f,subnet-0671c3476d35a5d82}"
LAMBDA_SECURITY_GROUP_IDS="${LAMBDA_SECURITY_GROUP_IDS:-sg-070aab3c40f308807}"
LAMBDA_LAYERS=(
    "arn:aws:lambda:us-east-1:017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-arm64:19"
    "arn:aws:lambda:us-east-1:491085429701:layer:psycopg2-layer-binary:3"
    "arn:aws:lambda:us-east-1:491085429701:layer:sql-alchemy-redis:1"
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312-Arm64:20"
    "arn:aws:lambda:us-east-1:491085429701:layer:helper2:1"
)

info()    { echo "[INFO]  $*"; }
success() { echo "[OK]    $*"; }
error()   { echo "[ERROR] $*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# This Lambda reads DATABASE_URL directly from the environment (see handlers/backfill/handler.py —
# deliberately avoids core.config/core.stg_db_config, which require unrelated Auth0/S3/CRM fields
# just to satisfy pydantic validation at import time). Default: pull the value out of local .env
# so you don't have to paste a connection string on the command line. Override with:
#   DATABASE_URL="postgresql://..." ./deployments/deploy_backfill_handler.sh
if [ -z "${DATABASE_URL:-}" ] && [ -f ".env" ]; then
    DATABASE_URL="$(grep -E '^DATABASE_URL\s*=' .env | head -1 | sed -E 's/^DATABASE_URL[[:space:]]*=[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/')"
fi
[ -n "${DATABASE_URL:-}" ] || error "DATABASE_URL not set and not found in .env — set it explicitly."

info "Cleaning old artifacts..."
rm -rf "$BUILD_DIR" "$ZIP_NAME"
mkdir -p "$BUILD_DIR"

info "Copying source: ${SOURCE_DIRS[*]}"
for dir in "${SOURCE_DIRS[@]}"; do
    [ -d "$dir" ] || error "Directory '$dir' not found."
    cp -R "$dir" "$BUILD_DIR/"
done

info "Zipping..."
cd "$BUILD_DIR"
zip -r9 "../$ZIP_NAME" . -x "**/__pycache__/*" -x "**/*.pyc" > /dev/null
cd ..

ZIP_SIZE=$(du -h "$ZIP_NAME" | cut -f1)
info "Zip: $ZIP_NAME ($ZIP_SIZE)"

export AWS_PROFILE="$AWS_PROFILE"

FUNCTION_EXISTS=true
aws lambda get-function --function-name "$LAMBDA_NAME" > /dev/null 2>&1 || FUNCTION_EXISTS=false

if [ "$FUNCTION_EXISTS" = false ]; then
    info "$LAMBDA_NAME does not exist — creating it (role/VPC/layers copied from revi-data-sync-stg-to-db)..."
    ENV_JSON="$(jq -nc --arg db "$DATABASE_URL" '{Variables: {DATABASE_URL: $db}}')"

    aws lambda create-function \
        --function-name "$LAMBDA_NAME" \
        --runtime "$LAMBDA_RUNTIME" \
        --architectures "$LAMBDA_ARCHITECTURE" \
        --role "$LAMBDA_ROLE" \
        --handler "handlers.backfill.handler.lambda_handler" \
        --zip-file "fileb://$ZIP_NAME" \
        --memory-size "$LAMBDA_MEMORY" \
        --timeout "$LAMBDA_TIMEOUT" \
        --vpc-config "SubnetIds=$LAMBDA_SUBNET_IDS,SecurityGroupIds=$LAMBDA_SECURITY_GROUP_IDS" \
        --layers "${LAMBDA_LAYERS[@]}" \
        --environment "$ENV_JSON" \
        --output text --query 'FunctionName' \
        || error "Create failed."

    info "Waiting for function to become active..."
    aws lambda wait function-active --function-name "$LAMBDA_NAME"
    success "Done — $LAMBDA_NAME created."
    rm -rf "$BUILD_DIR" "$ZIP_NAME"
    exit 0
fi

info "Deploying to $LAMBDA_NAME..."
aws lambda update-function-code \
    --function-name "$LAMBDA_NAME" \
    --zip-file "fileb://$ZIP_NAME" \
    --output text --query 'FunctionName' \
    || error "Deploy failed."

info "Waiting for update to complete..."
aws lambda wait function-updated --function-name "$LAMBDA_NAME"

info "Merging environment variables (preserving existing ones)..."
command -v jq >/dev/null 2>&1 || error "jq is required to merge Lambda env vars — install jq or set the vars manually."

CURRENT_ENV="$(aws lambda get-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --query 'Environment.Variables' --output json 2>/dev/null || echo '{}')"
if [ -z "$CURRENT_ENV" ] || [ "$CURRENT_ENV" = "null" ]; then
    CURRENT_ENV='{}'
fi

ENV_JSON="$(echo "$CURRENT_ENV" | jq -c \
    --arg db "$DATABASE_URL" \
    '{Variables: (. + {DATABASE_URL: $db})}')"

info "Setting handler + environment..."
aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --handler "handlers.backfill.handler.lambda_handler" \
    --environment "$ENV_JSON" \
    --output text --query 'FunctionName' \
    || error "Failed to update configuration."

info "Waiting for configuration update to complete..."
aws lambda wait function-updated --function-name "$LAMBDA_NAME" \
    && success "Done — $LAMBDA_NAME updated (handler + DATABASE_URL)."

rm -rf "$BUILD_DIR" "$ZIP_NAME"
