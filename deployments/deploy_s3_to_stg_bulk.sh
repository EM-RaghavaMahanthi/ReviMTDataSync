#!/bin/bash
# Deploy s3_to_stg_bulk Lambda — bulk Silver → staging + widened stale update.
#   Handler: handlers.s3_to_stg_bulk.lambda_handler
#
# Credentials come from the ambient AWS environment — export AWS_PROFILE (or
# AWS_ACCESS_KEY_ID/…) yourself before running. Nothing is hardcoded here.
#
# The function must already exist with the same runtime and dependency layer as
# revi-crm-db-sync (sqlalchemy, psycopg2, boto3, pydantic-settings, python-json-logger) —
# these scripts only ship source, they never install dependencies.
#
# Its execution role additionally needs, beyond the Stage 2 role:
#   athena:StartQueryExecution, athena:GetQueryExecution, athena:StopQueryExecution
#   glue:GetTable / GetDatabase on the Silver namespace
#   s3:GetObject on the Athena output location, s3:GetObject on the Silver data bucket
#
# Usage (run from repo root):
#   ./deployments/deploy_s3_to_stg_bulk.sh
#   LAMBDA_NAME=other-function ./deployments/deploy_s3_to_stg_bulk.sh

set -euo pipefail

LAMBDA_NAME="${LAMBDA_NAME:-revi-bulk-s3-to-stg}"
ZIP_NAME="s3_to_stg_bulk_deploy.zip"
BUILD_DIR="s3_to_stg_bulk_build"
SOURCE_DIRS=("core" "handlers" "s3_to_stg_bulk")

# Silver / Athena wiring. Merged into the function's existing environment, never replacing
# it — DATABASE_URL and anything else already set is preserved.
SILVER_NAMESPACE="${SILVER_NAMESPACE:-silver}"
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"
ATHENA_OUTPUT_LOCATION="${ATHENA_OUTPUT_LOCATION:-}"
BULK_DELTA_MINUTES="${BULK_DELTA_MINUTES:-60}"

info()    { echo "[INFO]  $*"; }
success() { echo "[OK]    $*"; }
error()   { echo "[ERROR] $*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

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

# ATHENA_OUTPUT_LOCATION has no safe default — only set it when the caller supplied one,
# so a re-deploy cannot blank out a value configured in the console.
NEW_VARS="$(jq -nc \
    --arg ns "$SILVER_NAMESPACE" \
    --arg wg "$ATHENA_WORKGROUP" \
    --arg dm "$BULK_DELTA_MINUTES" \
    '{SILVER_NAMESPACE: $ns, ATHENA_WORKGROUP: $wg, BULK_DELTA_MINUTES: $dm}')"
if [ -n "$ATHENA_OUTPUT_LOCATION" ]; then
    NEW_VARS="$(echo "$NEW_VARS" | jq -c --arg ol "$ATHENA_OUTPUT_LOCATION" '. + {ATHENA_OUTPUT_LOCATION: $ol}')"
else
    info "ATHENA_OUTPUT_LOCATION not supplied — leaving the existing value untouched."
fi

ENV_JSON="$(jq -nc --argjson cur "$CURRENT_ENV" --argjson new "$NEW_VARS" '{Variables: ($cur + $new)}')"

info "Setting handler + environment..."
aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --handler "handlers.s3_to_stg_bulk.lambda_handler" \
    --environment "$ENV_JSON" \
    --output text --query 'FunctionName' \
    || error "Failed to update configuration."

info "Waiting for configuration update to complete..."
aws lambda wait function-updated --function-name "$LAMBDA_NAME" \
    && success "Done — $LAMBDA_NAME updated (SILVER_NAMESPACE=$SILVER_NAMESPACE, ATHENA_WORKGROUP=$ATHENA_WORKGROUP)."

rm -rf "$BUILD_DIR" "$ZIP_NAME"
