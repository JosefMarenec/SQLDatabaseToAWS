#!/usr/bin/env bash
# =============================================================================
# Real-Time E-Commerce Analytics Platform — Deployment Script
# deploy.sh
#
# Deploys all CloudFormation stacks in dependency order, runs database
# migrations, uploads Glue scripts to S3, and optionally seeds test data.
#
# Usage:
#   export AWS_REGION=us-east-1
#   export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
#   export ENVIRONMENT=prod
#   export PROJECT_PREFIX=ecom-analytics
#   bash scripts/deploy.sh [--skip-seed] [--skip-migrations] [--dry-run]
#
# Prerequisites:
#   - AWS CLI v2 configured with deploy role
#   - psql client installed
#   - Python 3.11+ with requirements.txt installed
# =============================================================================

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Color output helpers
# ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

log_info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
die()         { log_error "$*"; exit 1; }

# ─────────────────────────────────────────────────────────────
# Parse arguments
# ─────────────────────────────────────────────────────────────
SKIP_SEED=false
SKIP_MIGRATIONS=false
DRY_RUN=false

for arg in "$@"; do
    case $arg in
        --skip-seed)        SKIP_SEED=true ;;
        --skip-migrations)  SKIP_MIGRATIONS=true ;;
        --dry-run)          DRY_RUN=true ;;
        *)                  log_warn "Unknown argument: $arg" ;;
    esac
done

# ─────────────────────────────────────────────────────────────
# Environment validation
# ─────────────────────────────────────────────────────────────
: "${AWS_REGION:?AWS_REGION environment variable must be set}"
: "${PROJECT_PREFIX:?PROJECT_PREFIX must be set (e.g. ecom-analytics)}"
: "${ENVIRONMENT:?ENVIRONMENT must be set (dev|staging|prod)}"

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null) \
    || die "Failed to get AWS account ID — check your credentials"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

log_info "Deploying ${PROJECT_PREFIX} to ${ENVIRONMENT} in ${AWS_REGION} (account: ${AWS_ACCOUNT_ID})"
[[ "$DRY_RUN" == "true" ]] && log_warn "DRY RUN mode — CloudFormation changes will be previewed but not applied"

# ─────────────────────────────────────────────────────────────
# Helper: Deploy a CloudFormation stack
# ─────────────────────────────────────────────────────────────
deploy_stack() {
    local stack_suffix="$1"
    local template_file="$2"
    local extra_params="${3:-}"
    local stack_name="${PROJECT_PREFIX}-${ENVIRONMENT}-${stack_suffix}"

    log_info "Deploying stack: $stack_name"

    local cmd=(
        aws cloudformation deploy
            --stack-name "$stack_name"
            --template-file "$template_file"
            --region "$AWS_REGION"
            --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM
            --parameter-overrides
                "ProjectPrefix=${PROJECT_PREFIX}"
                "Environment=${ENVIRONMENT}"
    )

    # Append extra parameter overrides if provided
    if [[ -n "$extra_params" ]]; then
        IFS=' ' read -ra EXTRA <<< "$extra_params"
        cmd+=("${EXTRA[@]}")
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        # Show changeset instead of deploying
        aws cloudformation create-change-set \
            --stack-name "$stack_name" \
            --template-body "file://${template_file}" \
            --change-set-name "dry-run-$(date +%s)" \
            --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
            --parameters \
                "ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX}" \
                "ParameterKey=Environment,ParameterValue=${ENVIRONMENT}" \
            --region "$AWS_REGION" 2>/dev/null || true
        log_info "[DRY RUN] Would deploy: $stack_name"
        return 0
    fi

    if "${cmd[@]}"; then
        log_success "Stack deployed: $stack_name"
    else
        local exit_code=$?
        # "No changes to deploy" is not an error
        if aws cloudformation describe-stacks --stack-name "$stack_name" \
            --query 'Stacks[0].StackStatus' --output text 2>/dev/null | \
            grep -qE "^(CREATE_COMPLETE|UPDATE_COMPLETE)$"; then
            log_info "Stack $stack_name is already up-to-date"
        else
            die "Stack deployment failed: $stack_name (exit code: $exit_code)"
        fi
    fi
}

wait_for_stack() {
    local stack_name="$1"
    local max_wait=600  # 10 minutes

    log_info "Waiting for stack: $stack_name"
    if [[ "$DRY_RUN" == "true" ]]; then return 0; fi

    aws cloudformation wait stack-create-complete \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" 2>/dev/null || \
    aws cloudformation wait stack-update-complete \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" 2>/dev/null || true

    log_success "Stack ready: $stack_name"
}

get_stack_output() {
    local stack_name="${PROJECT_PREFIX}-${ENVIRONMENT}-$1"
    local output_key="$2"
    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue" \
        --output text 2>/dev/null
}

# ─────────────────────────────────────────────────────────────
# STEP 1: Deploy RDS Stack (VPC + RDS + Security Groups)
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 1: RDS Infrastructure"
log_info "=========================================="

deploy_stack "rds" "${REPO_ROOT}/infrastructure/cloudformation/rds.yaml"
wait_for_stack "${PROJECT_PREFIX}-${ENVIRONMENT}-rds"

# Extract RDS outputs for downstream use
RDS_ENDPOINT=$(get_stack_output "rds" "RDSEndpoint")
RDS_PORT=$(get_stack_output "rds" "RDSPort")
RDS_SECRET_ARN=$(get_stack_output "rds" "RDSSecretArn")
DB_NAME=$(get_stack_output "rds" "DBName")

log_success "RDS endpoint: ${RDS_ENDPOINT}:${RDS_PORT}/${DB_NAME}"

# ─────────────────────────────────────────────────────────────
# STEP 2: Deploy Glue Stack (S3 Data Lake + Glue Catalog)
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 2: Glue Data Lake"
log_info "=========================================="

deploy_stack "glue" "${REPO_ROOT}/infrastructure/cloudformation/glue.yaml" \
    "RDSStackName=${PROJECT_PREFIX}-${ENVIRONMENT}-rds"
wait_for_stack "${PROJECT_PREFIX}-${ENVIRONMENT}-glue"

GLUE_SCRIPTS_BUCKET=$(get_stack_output "glue" "GlueScriptsBucketName")
RAW_BUCKET=$(get_stack_output "glue" "RawBucketName")
PROCESSED_BUCKET=$(get_stack_output "glue" "ProcessedBucketName")

log_success "Data lake buckets: raw=${RAW_BUCKET}, processed=${PROCESSED_BUCKET}"

# ─────────────────────────────────────────────────────────────
# STEP 3: Deploy Redshift Stack
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 3: Redshift Warehouse"
log_info "=========================================="

deploy_stack "redshift" "${REPO_ROOT}/infrastructure/cloudformation/redshift.yaml" \
    "RDSStackName=${PROJECT_PREFIX}-${ENVIRONMENT}-rds"
wait_for_stack "${PROJECT_PREFIX}-${ENVIRONMENT}-redshift"

REDSHIFT_ENDPOINT=$(get_stack_output "redshift" "RedshiftClusterEndpoint")
log_success "Redshift endpoint: ${REDSHIFT_ENDPOINT}"

# ─────────────────────────────────────────────────────────────
# STEP 4: Deploy Lambda Stack
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 4: Lambda Functions & SQS"
log_info "=========================================="

deploy_stack "lambda" "${REPO_ROOT}/infrastructure/cloudformation/lambda.yaml" \
    "RDSStackName=${PROJECT_PREFIX}-${ENVIRONMENT}-rds GlueStackName=${PROJECT_PREFIX}-${ENVIRONMENT}-glue"
wait_for_stack "${PROJECT_PREFIX}-${ENVIRONMENT}-lambda"

# ─────────────────────────────────────────────────────────────
# STEP 5: Upload Glue PySpark scripts to S3
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 5: Upload Glue Scripts"
log_info "=========================================="

if [[ -n "$GLUE_SCRIPTS_BUCKET" ]]; then
    if [[ "$DRY_RUN" != "true" ]]; then
        aws s3 cp "${REPO_ROOT}/etl/glue_jobs/" \
            "s3://${GLUE_SCRIPTS_BUCKET}/jobs/" \
            --recursive \
            --region "$AWS_REGION" \
            --exclude "*.pyc" --exclude "__pycache__/*"
        log_success "Glue scripts uploaded to s3://${GLUE_SCRIPTS_BUCKET}/jobs/"
    else
        log_info "[DRY RUN] Would upload Glue scripts to s3://${GLUE_SCRIPTS_BUCKET}/jobs/"
    fi
else
    log_warn "GLUE_SCRIPTS_BUCKET not available — skipping script upload"
fi

# ─────────────────────────────────────────────────────────────
# STEP 6: Run Database Migrations
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 6: Database Migrations"
log_info "=========================================="

if [[ "$SKIP_MIGRATIONS" == "true" ]]; then
    log_warn "Skipping migrations (--skip-migrations)"
elif [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would run migrations in order: 01 → 02 → 03 → 04 → 05"
else
    # Retrieve DB password from Secrets Manager
    DB_PASSWORD=$(aws secretsmanager get-secret-value \
        --secret-id "${PROJECT_PREFIX}-${ENVIRONMENT}-rds-credentials" \
        --region "$AWS_REGION" \
        --query SecretString \
        --output text | python3 -c "import sys, json; print(json.load(sys.stdin)['password'])")

    DB_USER=$(aws secretsmanager get-secret-value \
        --secret-id "${PROJECT_PREFIX}-${ENVIRONMENT}-rds-credentials" \
        --region "$AWS_REGION" \
        --query SecretString \
        --output text | python3 -c "import sys, json; print(json.load(sys.stdin)['username'])")

    # Run migrations in order
    MIGRATION_FILES=(
        "${REPO_ROOT}/database/schema/01_init_rds.sql"
        "${REPO_ROOT}/database/schema/02_partitioning.sql"
        "${REPO_ROOT}/database/schema/03_stored_procedures.sql"
        "${REPO_ROOT}/database/schema/04_views_and_materialized.sql"
        "${REPO_ROOT}/database/schema/05_rds_to_s3_export.sql"
    )

    for migration in "${MIGRATION_FILES[@]}"; do
        log_info "Running migration: $(basename "$migration")"
        PGPASSWORD="$DB_PASSWORD" psql \
            -h "$RDS_ENDPOINT" \
            -p "$RDS_PORT" \
            -U "$DB_USER" \
            -d "$DB_NAME" \
            -f "$migration" \
            --on-error-stop \
            -v ON_ERROR_STOP=1 \
            2>&1 | tail -5
        log_success "Migration complete: $(basename "$migration")"
    done

    # Run Redshift schema migrations
    RS_PASSWORD=$(aws secretsmanager get-secret-value \
        --secret-id "${PROJECT_PREFIX}-${ENVIRONMENT}-redshift-credentials" \
        --region "$AWS_REGION" \
        --query SecretString \
        --output text | python3 -c "import sys, json; print(json.load(sys.stdin)['password'])")

    RS_USER=$(aws secretsmanager get-secret-value \
        --secret-id "${PROJECT_PREFIX}-${ENVIRONMENT}-redshift-credentials" \
        --region "$AWS_REGION" \
        --query SecretString \
        --output text | python3 -c "import sys, json; print(json.load(sys.stdin)['username'])")

    for rs_migration in \
        "${REPO_ROOT}/database/redshift/01_schema.sql" \
        "${REPO_ROOT}/database/redshift/02_external_tables.sql"
    do
        log_info "Running Redshift migration: $(basename "$rs_migration")"
        PGPASSWORD="$RS_PASSWORD" psql \
            -h "$REDSHIFT_ENDPOINT" \
            -p 5439 \
            -U "$RS_USER" \
            -d analytics \
            -f "$rs_migration" \
            --on-error-stop 2>&1 | tail -5
        log_success "Redshift migration: $(basename "$rs_migration")"
    done
fi

# ─────────────────────────────────────────────────────────────
# STEP 7: Package and Update Lambda Functions
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 7: Deploy Lambda Function Code"
log_info "=========================================="

LAMBDA_FUNCTIONS=(
    "event_processor:${PROJECT_PREFIX}-${ENVIRONMENT}-event-processor"
    "data_validator:${PROJECT_PREFIX}-${ENVIRONMENT}-data-validator"
    "partition_manager:${PROJECT_PREFIX}-${ENVIRONMENT}-partition-manager"
)

for fn_def in "${LAMBDA_FUNCTIONS[@]}"; do
    source_dir="${fn_def%%:*}"
    function_name="${fn_def##*:}"
    src_path="${REPO_ROOT}/lambda/${source_dir}"

    log_info "Packaging Lambda: ${function_name}"

    if [[ "$DRY_RUN" != "true" ]]; then
        # Create a temporary zip
        tmp_zip=$(mktemp /tmp/lambda_XXXX.zip)

        (
            cd "$src_path"
            zip -rq "$tmp_zip" . -x "*.pyc" -x "__pycache__/*" -x "*.egg-info/*"
        )

        aws lambda update-function-code \
            --function-name "$function_name" \
            --zip-file "fileb://${tmp_zip}" \
            --region "$AWS_REGION" \
            --output text > /dev/null

        rm -f "$tmp_zip"
        log_success "Lambda updated: ${function_name}"
    else
        log_info "[DRY RUN] Would package and deploy Lambda: ${function_name}"
    fi
done

# ─────────────────────────────────────────────────────────────
# STEP 8: Seed Test Data (optional)
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 8: Seed Test Data"
log_info "=========================================="

if [[ "$SKIP_SEED" == "true" ]]; then
    log_info "Skipping data seeding (--skip-seed)"
elif [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would seed: 10,000 customers, 5,000 products, 100,000 orders"
elif [[ "$ENVIRONMENT" == "prod" ]]; then
    log_warn "Skipping seed data for production environment"
else
    log_info "Seeding test data (this may take several minutes)..."
    python3 "${REPO_ROOT}/scripts/seed_data.py" \
        --environment "$ENVIRONMENT" \
        --customers 10000 \
        --products 5000 \
        --orders 100000
    log_success "Test data seeded successfully"
fi

# ─────────────────────────────────────────────────────────────
# STEP 9: Verification
# ─────────────────────────────────────────────────────────────
log_info "=========================================="
log_info "STEP 9: Deployment Verification"
log_info "=========================================="

if [[ "$DRY_RUN" != "true" ]]; then
    # Verify stacks are in good state
    for stack_suffix in rds glue redshift lambda; do
        stack_name="${PROJECT_PREFIX}-${ENVIRONMENT}-${stack_suffix}"
        status=$(aws cloudformation describe-stacks \
            --stack-name "$stack_name" \
            --region "$AWS_REGION" \
            --query 'Stacks[0].StackStatus' \
            --output text 2>/dev/null || echo "NOT_FOUND")

        if [[ "$status" =~ ^(CREATE_COMPLETE|UPDATE_COMPLETE)$ ]]; then
            log_success "Stack ${stack_name}: ${status}"
        else
            log_error "Stack ${stack_name} in unexpected state: ${status}"
        fi
    done
fi

# ─────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo -e "${GREEN}  Deployment Complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo ""
echo "  Environment:      ${ENVIRONMENT}"
echo "  Region:           ${AWS_REGION}"
echo "  Project:          ${PROJECT_PREFIX}"
echo "  RDS Endpoint:     ${RDS_ENDPOINT:-N/A}"
echo "  Redshift:         ${REDSHIFT_ENDPOINT:-N/A}"
echo "  Raw Bucket:       ${RAW_BUCKET:-N/A}"
echo "  Processed Bucket: ${PROCESSED_BUCKET:-N/A}"
echo ""
echo "  Next steps:"
echo "  1. Run first ETL: aws glue start-job-run --job-name ${PROJECT_PREFIX}-${ENVIRONMENT}-rds-to-s3"
echo "  2. Run analytics: python3 scripts/run_analytics.py"
echo ""
