#!/bin/bash
# Deploy crm_to_s3 Lambda — packages source, uploads to AWS, and sets handler.
#   Handler: handlers.crm_to_s3.lambda_handler
#
# Usage (run from repo root):
#   ./deployments/deploy_crm_to_s3.sh
#   LAMBDA_NAME=other-function ./deployments/deploy_crm_to_s3.sh

set -euo pipefail

LAMBDA_NAME="${LAMBDA_NAME:-revi-syncdata-test}"
AWS_PROFILE="${AWS_PROFILE:-raghava.revi}"
ZIP_NAME="crm_to_s3_deploy.zip"
BUILD_DIR="crm_to_s3_build"
SOURCE_DIRS=("core" "clients" "handlers" "crm_sync" "schemas" "utils")

# MarianaTek rate-limit / sharding env vars (merged into the Lambda config below,
# preserving all existing vars). Override any at deploy time, e.g.:
#   USER_BATCHES_PER_SHARD=10 ./deployments/deploy_crm_to_s3.sh
PAGE_SIZE="${PAGE_SIZE:-100}"                                 # MT hard-caps page_size at 100
CRM_MAX_REQUESTS_PER_MIN="${CRM_MAX_REQUESTS_PER_MIN:-100}"   # token bucket = 50% of the 200/min ceiling
PAGES_PER_SHARD="${PAGES_PER_SHARD:-200}"                     # pages per location/user page-range shard
USER_BATCHES_PER_SHARD="${USER_BATCHES_PER_SHARD:-20}"        # 100-user batches per user_batch shard (~2 min/shard)
CONCURRENCY_LIMIT="${CONCURRENCY_LIMIT:-16}"                  # in-flight cap (token bucket is the real limiter)

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

export AWS_PROFILE="$AWS_PROFILE"
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
    --arg ps  "$PAGE_SIZE" \
    --arg rpm "$CRM_MAX_REQUESTS_PER_MIN" \
    --arg pps "$PAGES_PER_SHARD" \
    --arg ubs "$USER_BATCHES_PER_SHARD" \
    --arg cl  "$CONCURRENCY_LIMIT" \
    '{Variables: (. + {
        PAGE_SIZE: $ps,
        CRM_MAX_REQUESTS_PER_MIN: $rpm,
        PAGES_PER_SHARD: $pps,
        USER_BATCHES_PER_SHARD: $ubs,
        CONCURRENCY_LIMIT: $cl
    })}')"

info "Setting handler + environment..."
aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --handler "handlers.crm_to_s3.lambda_handler" \
    --environment "$ENV_JSON" \
    --output text --query 'FunctionName' \
    || error "Failed to update configuration."

info "Waiting for configuration update to complete..."
aws lambda wait function-updated --function-name "$LAMBDA_NAME" \
    && success "Done — $LAMBDA_NAME updated (handler + env: PAGE_SIZE=$PAGE_SIZE, CRM_MAX_REQUESTS_PER_MIN=$CRM_MAX_REQUESTS_PER_MIN, PAGES_PER_SHARD=$PAGES_PER_SHARD, USER_BATCHES_PER_SHARD=$USER_BATCHES_PER_SHARD, CONCURRENCY_LIMIT=$CONCURRENCY_LIMIT)."

rm -rf "$BUILD_DIR" "$ZIP_NAME"
