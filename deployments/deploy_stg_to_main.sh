#!/bin/bash
# Deploy stg_to_main Lambda — packages source, uploads to AWS, and sets handler.
#   Handler: handlers.stg_to_db.lambda_handler
#
# Usage (run from repo root):
#   ./deployments/deploy_stg_to_main.sh
#   LAMBDA_NAME=other-function ./deployments/deploy_stg_to_main.sh

set -euo pipefail

LAMBDA_NAME="${LAMBDA_NAME:-revi-data-sync-stg-to-db}"
AWS_PROFILE="${AWS_PROFILE:-raghava.revi}"
ZIP_NAME="stg_to_main_deploy.zip"
BUILD_DIR="stg_to_main_build"
SOURCE_DIRS=("core" "clients" "handlers" "stg_db_services")

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

info "Setting handler..."
aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --handler "handlers.stg_to_db.lambda_handler" \
    --output text --query 'FunctionName' \
    && success "Done — $LAMBDA_NAME updated." \
    || error "Failed to set handler."

rm -rf "$BUILD_DIR" "$ZIP_NAME"
