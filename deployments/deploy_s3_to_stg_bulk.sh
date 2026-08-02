#!/bin/bash
# Deploy s3_to_stg_bulk Lambda — bulk Silver → staging + widened stale update.
#   Handler: handlers.s3_to_stg_bulk.lambda_handler
#
# Creates the function if it does not exist, otherwise updates it in place.
#
# Runtime, architecture, layers and VPC config are CLONED from a reference Lambda
# (default revi-crm-db-sync) rather than hardcoded, so the bulk function always matches
# whatever the existing pipeline is running and no ARN can go stale in this file. All
# three existing functions (revi-crm-db-sync, revi-data-sync-stg-to-db, revi-syncdata-test)
# carry the same five layers and the same subnets/security group, so any of them works as
# the reference.
#
# The execution role is NOT cloned — it is revi-dlk-gold-lambda-exec, the data-lake role
# that already grants S3 Tables, Athena, Glue and Lake Formation. See LAMBDA_ROLE_ARN
# below. The reference function's own role has none of that.
#
# Credentials come from the ambient AWS environment — export AWS_PROFILE (or
# AWS_ACCESS_KEY_ID/…) yourself before running.
#
# Usage (run from repo root):
#   export AWS_PROFILE=revi
#   ./deployments/deploy_s3_to_stg_bulk.sh
#   ATHENA_OUTPUT_BUCKET=revi-datalake-athena ATHENA_OUTPUT_PREFIX=temp ./deployments/deploy_s3_to_stg_bulk.sh
#   LAMBDA_NAME=other-function REFERENCE_LAMBDA=revi-data-sync-stg-to-db ./deployments/deploy_s3_to_stg_bulk.sh

set -euo pipefail

LAMBDA_NAME="${LAMBDA_NAME:-revi-bulk-s3-to-stg}"
REFERENCE_LAMBDA="${REFERENCE_LAMBDA:-revi-crm-db-sync}"
ZIP_NAME="s3_to_stg_bulk_deploy.zip"
BUILD_DIR="s3_to_stg_bulk_build"
SOURCE_DIRS=("core" "handlers" "s3_to_stg_bulk")

# 900s is the Lambda maximum. Stage A runs 13 Athena queries plus a COPY each, then the
# stale pass — see .gsd/plan_and_gaps.md on watching load_elapsed_seconds against this.
TIMEOUT="${TIMEOUT:-900}"
MEMORY_SIZE="${MEMORY_SIZE:-10240}"
# 10 GB, matching the three existing functions. /tmp is not used directly — Athena results
# stream from S3 into COPY — but psycopg2/pandas spill here under memory pressure.
EPHEMERAL_STORAGE="${EPHEMERAL_STORAGE:-10240}"

# Execution role. Defaults to the data-lake role, which already carries everything this
# Lambda needs — AmazonS3TablesFullAccess, the athena-access and s3tables-access inline
# policies (Athena on the primary/revi-dlk-gold workgroups, the s3tablescatalog data
# catalog, lakeformation:GetDataAccess, Glue reads, and S3 on the Athena result prefixes),
# plus AWSLambdaVPCAccessExecutionRole and AWSLambdaBasicExecutionRole. This is the same
# role reviDataInsightsAPI/revi-cloud-campaign and revi-cloud-sender reuse.
LAMBDA_ROLE_ARN="${LAMBDA_ROLE_ARN:-arn:aws:iam::491085429701:role/revi-dlk-gold-lambda-exec}"

# Silver / Athena wiring. Merged into the function's existing environment, never replacing
# it — DATABASE_URL and anything else already set is preserved.
SILVER_NAMESPACE="${SILVER_NAMESPACE:-silver}"
# The Silver tables are S3 Tables, not the default Glue catalog. Quoting is applied in
# athena.fqn, not here.
ATHENA_CATALOG="${ATHENA_CATALOG:-s3tablescatalog/revi-crm-data}"
# The data-lake workgroup, same as reviDataInsightsAPI/revi-dlk-gold. It enforces its own
# result location (s3://revi-datalake-athena/athena-results/), which is exactly what the
# athena-access policy grants. Do NOT use revi-dlk-workgroup: it also enforces, but points
# at revi-datalake-athena/results/, a prefix the role does not grant.
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-revi-dlk-gold}"
# Bucket name only, not a URI — the Lambda composes s3://<bucket>/<prefix>/. Required
# (core/bulk_config declares it with no default). The prefix must be one the athena-access
# policy grants: athena-results, temp or athena-ddl-results.
ATHENA_OUTPUT_BUCKET="${ATHENA_OUTPUT_BUCKET:-revi-datalake-athena}"
ATHENA_OUTPUT_PREFIX="${ATHENA_OUTPUT_PREFIX:-athena-results}"
BULK_DELTA_MINUTES="${BULK_DELTA_MINUTES:-10}"
# Default write mode for the stale pass. "false" = dry run. An event's "update" field
# overrides it per invocation, so leaving this false still allows a deliberate write.
BULK_UPDATE="${BULK_UPDATE:-false}"

info()    { echo "[INFO]  $*"; }
success() { echo "[OK]    $*"; }
warn()    { echo "[WARN]  $*" >&2; }
error()   { echo "[ERROR] $*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

command -v jq >/dev/null 2>&1 || error "jq is required — install it or set the configuration manually."

# ── Package ─────────────────────────────────────────────────────────────────

info "Cleaning old artifacts..."
rm -rf "$BUILD_DIR" "$ZIP_NAME"
mkdir -p "$BUILD_DIR"

info "Copying source: ${SOURCE_DIRS[*]}"
for dir in "${SOURCE_DIRS[@]}"; do
    [ -d "$dir" ] || error "Directory '$dir' not found. Run from the repo root."
    cp -R "$dir" "$BUILD_DIR/"
done

info "Zipping..."
(cd "$BUILD_DIR" && zip -r9 "../$ZIP_NAME" . -x "**/__pycache__/*" -x "**/*.pyc" > /dev/null)

info "Zip: $ZIP_NAME ($(du -h "$ZIP_NAME" | cut -f1))"

# ── Read the reference function's shape ─────────────────────────────────────

info "Reading runtime/layers/VPC from reference function $REFERENCE_LAMBDA..."
REF_CFG="$(aws lambda get-function-configuration --function-name "$REFERENCE_LAMBDA" --output json 2>/dev/null)" \
    || error "Reference function '$REFERENCE_LAMBDA' not found. Set REFERENCE_LAMBDA to one that exists."

REF_RUNTIME="$(echo "$REF_CFG" | jq -r '.Runtime')"
REF_ARCH="$(echo "$REF_CFG"    | jq -r '.Architectures[0] // "x86_64"')"
REF_LAYERS="$(echo "$REF_CFG"  | jq -r '[.Layers[]?.Arn] | join(" ")')"
REF_SUBNETS="$(echo "$REF_CFG" | jq -r '[.VpcConfig.SubnetIds[]?]        | join(",")')"
REF_SGS="$(echo "$REF_CFG"     | jq -r '[.VpcConfig.SecurityGroupIds[]?] | join(",")')"

[ -n "$REF_LAYERS" ]  || warn "Reference function has no layers — the deploy will ship no dependencies."
[ -n "$REF_SUBNETS" ] || warn "Reference function has no VPC config — the function will have no RDS access."

VPC_ARG=""
if [ -n "$REF_SUBNETS" ] && [ -n "$REF_SGS" ]; then
    VPC_ARG="SubnetIds=${REF_SUBNETS},SecurityGroupIds=${REF_SGS}"
fi

info "  runtime=$REF_RUNTIME arch=$REF_ARCH"
info "  layers=$(echo "$REF_LAYERS" | wc -w | tr -d ' ')"
info "  vpc=${VPC_ARG:-none}"
info "  role=$LAMBDA_ROLE_ARN"

# ── Build the environment ───────────────────────────────────────────────────

FUNCTION_EXISTS=0
CURRENT_ENV='{}'
if aws lambda get-function-configuration --function-name "$LAMBDA_NAME" >/dev/null 2>&1; then
    FUNCTION_EXISTS=1
    CURRENT_ENV="$(aws lambda get-function-configuration \
        --function-name "$LAMBDA_NAME" --query 'Environment.Variables' --output json)"
    [ "$CURRENT_ENV" = "null" ] && CURRENT_ENV='{}'
else
    # First deploy: seed DATABASE_URL from the reference so the new function points at the
    # same RDS instance the rest of the pipeline uses. Override by exporting DATABASE_URL.
    SEED_DB_URL="${DATABASE_URL:-$(echo "$REF_CFG" | jq -r '.Environment.Variables.DATABASE_URL // empty')}"
    if [ -n "$SEED_DB_URL" ]; then
        CURRENT_ENV="$(jq -nc --arg u "$SEED_DB_URL" '{DATABASE_URL: $u}')"
        info "Seeding DATABASE_URL from $REFERENCE_LAMBDA."
    else
        warn "No DATABASE_URL on the reference and none exported — set it before invoking."
    fi
fi

# ATHENA_OUTPUT_BUCKET is always written: the Lambda declares it as a required setting, so
# shipping without it fails at import. A URI here is a common slip — the Lambda composes
# the URI itself, so this must be a bare bucket name.
case "$ATHENA_OUTPUT_BUCKET" in
    s3://*|*/*) error "ATHENA_OUTPUT_BUCKET must be a bare bucket name, not a URI or path — got '$ATHENA_OUTPUT_BUCKET'." ;;
    "")         error "ATHENA_OUTPUT_BUCKET must not be empty." ;;
esac
ATHENA_OUTPUT_PREFIX="${ATHENA_OUTPUT_PREFIX#/}"
ATHENA_OUTPUT_PREFIX="${ATHENA_OUTPUT_PREFIX%/}"

NEW_VARS="$(jq -nc \
    --arg ns "$SILVER_NAMESPACE" \
    --arg cat "$ATHENA_CATALOG" \
    --arg wg "$ATHENA_WORKGROUP" \
    --arg ob "$ATHENA_OUTPUT_BUCKET" \
    --arg op "$ATHENA_OUTPUT_PREFIX" \
    --arg dm "$BULK_DELTA_MINUTES" \
    --arg up "$BULK_UPDATE" \
    '{SILVER_NAMESPACE: $ns, ATHENA_CATALOG: $cat, ATHENA_WORKGROUP: $wg,
      ATHENA_OUTPUT_BUCKET: $ob, ATHENA_OUTPUT_PREFIX: $op,
      BULK_DELTA_MINUTES: $dm, BULK_UPDATE: $up}')"

# A previous deploy may have left the old required-setting name behind; it is now unused
# and would only confuse anyone reading the console.
CURRENT_ENV="$(echo "$CURRENT_ENV" | jq -c 'del(.ATHENA_OUTPUT_LOCATION)')"

ENV_JSON="$(jq -nc --argjson cur "$CURRENT_ENV" --argjson new "$NEW_VARS" '{Variables: ($cur + $new)}')"

# ── Create or update ────────────────────────────────────────────────────────

if [ "$FUNCTION_EXISTS" -eq 0 ]; then
    info "Function does not exist — creating $LAMBDA_NAME..."
    # shellcheck disable=SC2086  # REF_LAYERS is a space-separated ARN list, split on purpose
    aws lambda create-function \
        --function-name "$LAMBDA_NAME" \
        --runtime "$REF_RUNTIME" \
        --architectures "$REF_ARCH" \
        --role "$LAMBDA_ROLE_ARN" \
        --handler "handlers.s3_to_stg_bulk.lambda_handler" \
        --zip-file "fileb://$ZIP_NAME" \
        --timeout "$TIMEOUT" \
        --memory-size "$MEMORY_SIZE" \
        --ephemeral-storage "Size=$EPHEMERAL_STORAGE" \
        --environment "$ENV_JSON" \
        ${REF_LAYERS:+--layers $REF_LAYERS} \
        ${VPC_ARG:+--vpc-config "$VPC_ARG"} \
        --output text --query 'FunctionName' \
        || error "Create failed."

    info "Waiting for the function to become active..."
    aws lambda wait function-active --function-name "$LAMBDA_NAME"
    success "Created $LAMBDA_NAME."
    info "Execution role: $LAMBDA_ROLE_ARN"
else
    info "Deploying code to $LAMBDA_NAME..."
    aws lambda update-function-code \
        --function-name "$LAMBDA_NAME" \
        --zip-file "fileb://$ZIP_NAME" \
        --output text --query 'FunctionName' \
        || error "Deploy failed."

    info "Waiting for the code update to complete..."
    aws lambda wait function-updated --function-name "$LAMBDA_NAME"

    info "Syncing handler, environment, layers, VPC, timeout, memory and storage..."
    # shellcheck disable=SC2086
    # --role is set here as well as on create. Omitting it on update is a silent trap: the
    # function keeps whatever role it was created with, so changing LAMBDA_ROLE_ARN appears
    # to take effect (the deploy succeeds, the log prints the new ARN) while the function
    # still executes as the old one — and every IAM or Lake Formation grant made against
    # the intended role does nothing.
    aws lambda update-function-configuration \
        --function-name "$LAMBDA_NAME" \
        --role "$LAMBDA_ROLE_ARN" \
        --handler "handlers.s3_to_stg_bulk.lambda_handler" \
        --timeout "$TIMEOUT" \
        --memory-size "$MEMORY_SIZE" \
        --ephemeral-storage "Size=$EPHEMERAL_STORAGE" \
        --environment "$ENV_JSON" \
        ${REF_LAYERS:+--layers $REF_LAYERS} \
        ${VPC_ARG:+--vpc-config "$VPC_ARG"} \
        --output text --query 'FunctionName' \
        || error "Failed to update configuration."

    info "Waiting for the configuration update to complete..."
    aws lambda wait function-updated --function-name "$LAMBDA_NAME"
    success "Updated $LAMBDA_NAME."
fi

info "SILVER_NAMESPACE=$SILVER_NAMESPACE ATHENA_CATALOG=$ATHENA_CATALOG"
info "ATHENA_WORKGROUP=$ATHENA_WORKGROUP output=s3://$ATHENA_OUTPUT_BUCKET/$ATHENA_OUTPUT_PREFIX/"
info "BULK_DELTA_MINUTES=$BULK_DELTA_MINUTES BULK_UPDATE=$BULK_UPDATE"
info "timeout=${TIMEOUT}s memory=${MEMORY_SIZE}MB ephemeral=${EPHEMERAL_STORAGE}MB"
if [ "$BULK_UPDATE" != "true" ]; then
    info "BULK_UPDATE is false — the stale pass is a dry run unless the event sets \"update\": true."
fi

rm -rf "$BUILD_DIR" "$ZIP_NAME"
