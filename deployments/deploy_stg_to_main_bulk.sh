#!/bin/bash
# Deploy stg_to_main_bulk Lambda — bulk staging → production, one account per invocation.
#   Handler: handlers.stg_to_main_bulk.lambda_handler
#
# Credentials come from the ambient AWS environment — export AWS_PROFILE (or
# AWS_ACCESS_KEY_ID/…) yourself before running. Nothing is hardcoded here.
#
# The function must already exist with the same runtime, dependency layer and VPC config as
# revi-data-sync-stg-to-db — these scripts only ship source, they never install
# dependencies. No Athena access is needed: this stage only talks to Postgres.
#
# Usage (run from repo root):
#   ./deployments/deploy_stg_to_main_bulk.sh
#   LAMBDA_NAME=other-function ./deployments/deploy_stg_to_main_bulk.sh

set -euo pipefail

LAMBDA_NAME="${LAMBDA_NAME:-revi-bulk-stg-to-main}"
ZIP_NAME="stg_to_main_bulk_deploy.zip"
BUILD_DIR="stg_to_main_bulk_build"
# clients/, utils/ and schemas/ are needed only by the optional post-processing step
# (IS_POST_PROCESS), which is off by default for bulk — shipped anyway so enabling the
# flag does not require a different package.
# s3_to_stg_bulk is packaged for its config only: stage 2 derives the staging table
# names and the processing order from cfg rather than repeating them, so a change to
# stage 1's table set cannot silently orphan a processor here.
SOURCE_DIRS=("core" "clients" "handlers" "stg_to_main_bulk" "s3_to_stg_bulk" "utils" "schemas")

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

info "Setting handler..."
aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --handler "handlers.stg_to_main_bulk.lambda_handler" \
    --output text --query 'FunctionName' \
    || error "Failed to set handler."

info "Waiting for configuration update to complete..."
aws lambda wait function-updated --function-name "$LAMBDA_NAME" \
    && success "Done — $LAMBDA_NAME updated."

rm -rf "$BUILD_DIR" "$ZIP_NAME"
