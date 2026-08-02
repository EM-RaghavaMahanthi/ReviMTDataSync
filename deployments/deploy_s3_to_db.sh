#!/bin/bash
# Deploy s3_to_db Lambda — packages source, uploads to AWS, and sets handler.
#   Handler: handlers.s3_to_db.lambda_handler
#
# Usage (run from repo root):
#   ./deployments/deploy_s3_to_db.sh
#   LAMBDA_NAME=other-function ./deployments/deploy_s3_to_db.sh

set -euo pipefail

LAMBDA_NAME="${LAMBDA_NAME:-revi-crm-db-sync}"
AWS_PROFILE="${AWS_PROFILE:-revi}"
ZIP_NAME="s3_to_db_deploy.zip"
BUILD_DIR="s3_to_db_build"
SOURCE_DIRS=("core" "clients" "handlers" "db_services" "schemas" "utils" "sql_cmds")

# S3 prefixes for the new tags/notes datasets. db_services/main_bulk_insert.py reads these
# individual settings fields directly (it does NOT use the s3_prefixes JSON override that
# Stage 1 supports) — must match deploy_crm_to_s3.sh's values so both stages agree on where
# each dataset lives in S3.
USER_NOTES_S3_PREFIX="${USER_NOTES_S3_PREFIX:-mariana-tek/raw-bulk-data/user_notes}"
USER_TAGS_S3_PREFIX="${USER_TAGS_S3_PREFIX:-mariana-tek/raw-bulk-data/user_tags}"
CUSTOMER_TAGS_S3_PREFIX="${CUSTOMER_TAGS_S3_PREFIX:-mariana-tek/raw-bulk-data/customer_tags}"

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
    --arg unp "$USER_NOTES_S3_PREFIX" \
    --arg utp "$USER_TAGS_S3_PREFIX" \
    --arg ctp "$CUSTOMER_TAGS_S3_PREFIX" \
    '{Variables: (. + {
        USER_NOTES_S3_PREFIX: $unp,
        USER_TAGS_S3_PREFIX: $utp,
        CUSTOMER_TAGS_S3_PREFIX: $ctp
    })}')"

info "Setting handler + environment..."
aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --handler "handlers.s3_to_db.lambda_handler" \
    --environment "$ENV_JSON" \
    --output text --query 'FunctionName' \
    || error "Failed to update configuration."

info "Waiting for configuration update to complete..."
aws lambda wait function-updated --function-name "$LAMBDA_NAME" \
    && success "Done — $LAMBDA_NAME updated (handler + env: USER_NOTES_S3_PREFIX=$USER_NOTES_S3_PREFIX, USER_TAGS_S3_PREFIX=$USER_TAGS_S3_PREFIX, CUSTOMER_TAGS_S3_PREFIX=$CUSTOMER_TAGS_S3_PREFIX)."

rm -rf "$BUILD_DIR" "$ZIP_NAME"
