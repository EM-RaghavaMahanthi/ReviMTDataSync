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
#   aws lambda invoke --function-name revi-backfill \
#     --payload '{"event_type":"refresh_recent_notes","account_id":4809}' out.json

set -euo pipefail

LAMBDA_NAME="${LAMBDA_NAME:-revi-backfill}"
AWS_PROFILE="${AWS_PROFILE:-revi}"
ZIP_NAME="backfill_handler_deploy.zip"
BUILD_DIR="backfill_handler_build"
# backfill_notes_and_tags is Stage-3-only (pure SQLAlchemy, no CRM calls). refresh_recent_notes
# calls the live MarianaTek API via utils.api_client (rate-limited/retried, same as Stage 1)
# and reuses crm_sync.user_notes's row mapper (pulls in schemas.revi_schema) - so crm_sync,
# utils, and schemas need to be bundled too, even though the other job doesn't use them.
SOURCE_DIRS=("core" "handlers" "stg_db_services" "crm_sync" "utils" "schemas")

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

export AWS_PROFILE="$AWS_PROFILE"

# Probe the function BEFORE resolving secrets: on an update we can reuse whatever is already
# set on it, so a redeploy needs no local .env and no connection string on the command line.
FUNCTION_EXISTS=true
aws lambda get-function --function-name "$LAMBDA_NAME" > /dev/null 2>&1 || FUNCTION_EXISTS=false

CURRENT_ENV='{}'
if [ "$FUNCTION_EXISTS" = true ]; then
    CURRENT_ENV="$(aws lambda get-function-configuration \
        --function-name "$LAMBDA_NAME" \
        --query 'Environment.Variables' --output json 2>/dev/null || echo '{}')"
    if [ -z "$CURRENT_ENV" ] || [ "$CURRENT_ENV" = "null" ]; then
        CURRENT_ENV='{}'
    fi
fi

# Read one key out of the live function's environment (empty string if unset).
current_env_var() { echo "$CURRENT_ENV" | jq -r --arg k "$1" '.[$k] // ""'; }

# Read one key out of a local .env (empty string if the file or key is absent).
dotenv_var() {
    [ -f ".env" ] || return 0
    grep -E "^$1\s*=" .env | head -1 | sed -E "s/^$1[[:space:]]*=[[:space:]]*\"?([^\"]*)\"?[[:space:]]*\$/\1/"
}

# This Lambda reads DATABASE_URL directly from the environment (see handlers/backfill/handler.py —
# deliberately avoids core.config/core.stg_db_config, which require unrelated Auth0/S3/CRM fields
# just to satisfy pydantic validation at import time).
#
# Resolution order: explicit env var > local .env > whatever the deployed function already has.
# That last fallback is what makes a plain redeploy work on a machine with no .env — the value
# is already on the function, and re-typing a connection string just to preserve it invites a
# typo that points production at the wrong database. Override with:
#   DATABASE_URL="postgresql://..." ./deployments/deploy_backfill_handler.sh
DATABASE_URL="${DATABASE_URL:-$(dotenv_var DATABASE_URL)}"
if [ -z "$DATABASE_URL" ]; then
    DATABASE_URL="$(current_env_var DATABASE_URL)"
    [ -n "$DATABASE_URL" ] && info "DATABASE_URL not set locally — reusing the value already on $LAMBDA_NAME."
fi
[ -n "$DATABASE_URL" ] || error "DATABASE_URL not set, not found in .env, and not already on $LAMBDA_NAME — set it explicitly."

# API_KEY is only needed by the refresh_recent_notes job (calls MarianaTek directly, same
# bearer token used everywhere else) — not by backfill_notes_and_tags. Same resolution order;
# not fatal if missing, since not every job needs it.
API_KEY="${API_KEY:-$(dotenv_var API_KEY)}"
if [ -z "$API_KEY" ]; then
    API_KEY="$(current_env_var API_KEY)"
    [ -n "$API_KEY" ] && info "API_KEY not set locally — reusing the value already on $LAMBDA_NAME."
fi
[ -n "$API_KEY" ] || info "API_KEY not set, not found in .env, and not already on the function — refresh_recent_notes will fail until it's added."

# refresh_recent_notes's lookback window - always set explicitly as a Lambda env var (rather
# than relying on the code's own default) so it's visible/adjustable straight from the Lambda
# console without a redeploy. 24h for now (per-request, temporary for the first run - drop
# back down after that). Override at deploy time:
#   NOTES_WINDOW_HOURS=6 ./deployments/deploy_backfill_handler.sh
# Also read from .env — same reason as the write switch below: a value sitting in .env that
# the deploy ignores is a trap. No function fallback here on purpose; this one is meant to be
# re-stated every deploy so it doesn't quietly stay wide open.
NOTES_WINDOW_HOURS="${NOTES_WINDOW_HOURS:-$(dotenv_var NOTES_WINDOW_HOURS)}"
NOTES_WINDOW_HOURS="${NOTES_WINDOW_HOURS:-24}"

# Master write switch for this Lambda. When false (the default), every step still runs and
# every count is still reported — only the INSERTs into customer_notes /
# customer_tags_default / customer_tag_assignments (and refresh_recent_notes'
# update_changed_notes) are skipped. Set explicitly so it is visible and flippable from the
# Lambda console without a redeploy; a single event field "write": true overrides it per call.
# Deploy with writes on, either way:
#   BACKFILL_WRITE_ENABLED=true ./deployments/deploy_backfill_handler.sh
#   or set BACKFILL_WRITE_ENABLED=true in .env
#
# Resolution: explicit shell var > .env > whatever is already on the function > false.
# .env is read here for the same reason DATABASE_URL and API_KEY are — a value sitting in
# .env that the deploy silently ignores is worse than no value at all.
#
# Sticky via the function fallback: an unrelated redeploy keeps whatever writes setting is
# live rather than resetting it to false, which would contradict "flippable from the
# console" — someone turns writes on, ships a code change, and the backfill silently stops
# writing. Only a first create falls through to false.
BACKFILL_WRITE_ENABLED="${BACKFILL_WRITE_ENABLED:-$(dotenv_var BACKFILL_WRITE_ENABLED)}"
BACKFILL_WRITE_ENABLED="${BACKFILL_WRITE_ENABLED:-$(current_env_var BACKFILL_WRITE_ENABLED)}"
BACKFILL_WRITE_ENABLED="${BACKFILL_WRITE_ENABLED:-false}"

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

if [ "$FUNCTION_EXISTS" = false ]; then
    info "$LAMBDA_NAME does not exist — creating it (role/VPC/layers copied from revi-data-sync-stg-to-db)..."
    ENV_JSON="$(jq -nc \
        --arg db "$DATABASE_URL" \
        --arg key "$API_KEY" \
        --arg win "$NOTES_WINDOW_HOURS" \
        --arg wr "$BACKFILL_WRITE_ENABLED" \
        '{Variables: ({DATABASE_URL: $db, BACKFILL_WRITE_ENABLED: $wr}
            + (if $key != "" then {API_KEY: $key} else {} end)
            + (if $win != "" then {NOTES_WINDOW_HOURS: $win} else {} end))}')"

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

ENV_JSON="$(echo "$CURRENT_ENV" | jq -c \
    --arg db "$DATABASE_URL" \
    --arg key "$API_KEY" \
    --arg win "$NOTES_WINDOW_HOURS" \
    --arg wr "$BACKFILL_WRITE_ENABLED" \
    '{Variables: (. + {DATABASE_URL: $db, BACKFILL_WRITE_ENABLED: $wr}
        + (if $key != "" then {API_KEY: $key} else {} end)
        + (if $win != "" then {NOTES_WINDOW_HOURS: $win} else {} end))}')"

info "Setting handler + environment..."
aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --handler "handlers.backfill.handler.lambda_handler" \
    --environment "$ENV_JSON" \
    --output text --query 'FunctionName' \
    || error "Failed to update configuration."

info "Waiting for configuration update to complete..."
aws lambda wait function-updated --function-name "$LAMBDA_NAME" \
    && success "Done — $LAMBDA_NAME updated (handler + DATABASE_URL + API_KEY + NOTES_WINDOW_HOURS=$NOTES_WINDOW_HOURS + BACKFILL_WRITE_ENABLED=$BACKFILL_WRITE_ENABLED)."

rm -rf "$BUILD_DIR" "$ZIP_NAME"
