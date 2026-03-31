# Real-Time E-Commerce Analytics Platform

A production-grade, end-to-end data engineering platform built on AWS that ingests transactional e-commerce data, processes it through a multi-zone data lake, and serves analytical workloads via Amazon Redshift — all orchestrated through AWS Glue, Lambda, and EventBridge.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Real-Time E-Commerce Analytics Platform               │
└─────────────────────────────────────────────────────────────────────────────┘

  Clickstream / Events                 Transactional Systems
  ┌──────────────┐                     ┌─────────────────────┐
  │  Kinesis     │                     │  RDS PostgreSQL      │
  │  Data Stream │──► SQS ──► Lambda   │  (OLTP Database)    │
  └──────────────┘           │         └─────────┬───────────┘
                             │                   │
                             ▼                   │ AWS Glue (JDBC)
                     ┌───────────────┐           │
                     │  Events Table  │           ▼
                     │  (RDS)        │   ┌───────────────────┐
                     └───────────────┘   │  S3 Data Lake     │
                                         │  ┌─────────────┐  │
                                         │  │  raw/       │  │
                                         │  │  processed/ │  │
                                         │  │  curated/   │  │
                                         │  └─────────────┘  │
                                         └────────┬──────────┘
                                                  │
                                                  │ AWS Glue (Parquet)
                                                  ▼
                                         ┌────────────────────┐
                                         │  Amazon Redshift   │
                                         │  (OLAP Warehouse)  │
                                         │  + Spectrum        │
                                         └────────────────────┘
                                                  │
                                                  ▼
                                         ┌────────────────────┐
                                         │  Analytics /       │
                                         │  BI Tools /        │
                                         │  QuickSight        │
                                         └────────────────────┘
```

### Component Breakdown

| Component | Service | Purpose |
|-----------|---------|---------|
| OLTP Database | RDS PostgreSQL 15 | Transactional e-commerce data |
| Event Streaming | Kinesis + SQS + Lambda | Real-time clickstream ingestion |
| Data Lake (Raw) | S3 | Raw unprocessed events and exports |
| Data Lake (Processed) | S3 | Cleaned, validated Parquet files |
| Data Lake (Curated) | S3 | Business-ready aggregations |
| ETL Orchestration | AWS Glue | Spark-based batch ETL jobs |
| Data Catalog | AWS Glue Catalog | Schema registry, Hive metastore |
| Data Warehouse | Amazon Redshift | Columnar OLAP analytics |
| External Tables | Redshift Spectrum | Query S3 directly from Redshift |
| Scheduling | EventBridge | Cron-based automation |
| Secrets | AWS Secrets Manager | Database credentials |
| Monitoring | CloudWatch | Logs, metrics, alarms |

---

## Repository Structure

```
.
├── README.md
├── requirements.txt
├── config/
│   └── database.ini                    # Configuration template
├── infrastructure/
│   └── cloudformation/
│       ├── rds.yaml                    # VPC, RDS, Security Groups
│       ├── redshift.yaml               # Redshift cluster + IAM
│       ├── glue.yaml                   # Glue catalog, crawlers, jobs, S3 buckets
│       └── lambda.yaml                 # Lambda, SQS, EventBridge
├── database/
│   ├── schema/
│   │   ├── 01_init_rds.sql             # Full OLTP schema
│   │   ├── 02_partitioning.sql         # Table partitioning
│   │   ├── 03_stored_procedures.sql    # Business logic procedures
│   │   ├── 04_views_and_materialized.sql # Views + materialized views
│   │   └── 05_rds_to_s3_export.sql    # RDS to S3 export functions
│   └── redshift/
│       ├── 01_schema.sql               # Warehouse schema (facts + dims)
│       ├── 02_external_tables.sql      # Redshift Spectrum setup
│       └── 03_analytics_queries.sql    # 20+ analytical queries
├── etl/
│   └── glue_jobs/
│       ├── rds_to_s3.py               # RDS to S3 incremental extract
│       ├── s3_to_redshift.py          # S3 to Redshift load (SCD Type 2)
│       └── data_quality.py            # Quality validation job
├── lambda/
│   ├── event_processor/
│   │   └── handler.py                 # SQS to RDS event writer
│   ├── data_validator/
│   │   └── handler.py                 # S3 file validator
│   └── partition_manager/
│       └── handler.py                 # Automated partition management
└── scripts/
    ├── deploy.sh                       # Full deployment automation
    ├── seed_data.py                    # Realistic test data generator
    └── run_analytics.py               # Analytics query runner
```

---

## Prerequisites

- AWS CLI v2 configured with appropriate permissions
- Python 3.11+
- PostgreSQL client (`psql`)
- Sufficient AWS quotas for RDS, Redshift, Glue, Lambda

### Required IAM Permissions

Your deployment role needs:
- `AmazonRDS*`, `AmazonRedshift*`, `AWSGlue*`
- `AWSLambda*`, `AmazonS3*`, `AmazonSQS*`
- `IAM` (for creating roles), `CloudFormation*`
- `SecretsManager*`, `Events*` (EventBridge)

---

## Quick Start

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp config/database.ini config/database.local.ini
# Edit config/database.local.ini with your AWS account details
```

### 3. Deploy Infrastructure

```bash
# Set your AWS region and account
export AWS_REGION=us-east-1
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export ENVIRONMENT=prod
export PROJECT_PREFIX=ecom-analytics

# Run full deployment
bash scripts/deploy.sh
```

The deployment script:
1. Creates VPC, RDS, Security Groups (rds.yaml)
2. Creates Redshift cluster (redshift.yaml)
3. Creates Glue catalog, S3 buckets, crawlers (glue.yaml)
4. Creates Lambda functions, SQS, EventBridge (lambda.yaml)
5. Runs database migrations in order
6. Uploads Glue PySpark scripts to S3
7. Seeds test data (optional)

### 4. Run Database Migrations

```bash
# Get RDS endpoint from CloudFormation outputs
RDS_HOST=$(aws cloudformation describe-stacks \
  --stack-name ${PROJECT_PREFIX}-rds \
  --query 'Stacks[0].Outputs[?OutputKey==`RDSEndpoint`].OutputValue' \
  --output text)

# Get password from Secrets Manager
DB_PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id ${PROJECT_PREFIX}-rds-credentials \
  --query SecretString --output text | jq -r .password)

# Run migrations
for f in database/schema/*.sql; do
  psql -h $RDS_HOST -U dbadmin -d ecommerce -f $f
done
```

### 5. Seed Test Data

```bash
python scripts/seed_data.py \
  --customers 10000 \
  --products 5000 \
  --orders 100000 \
  --environment prod
```

### 6. Trigger First ETL Run

```bash
# Start RDS to S3 Glue job
aws glue start-job-run --job-name ${PROJECT_PREFIX}-rds-to-s3

# After completion, start S3 to Redshift
aws glue start-job-run --job-name ${PROJECT_PREFIX}-s3-to-redshift
```

### 7. Run Analytics

```bash
python scripts/run_analytics.py \
  --output-dir ./reports \
  --format csv
```

---

## Data Model

### OLTP (RDS PostgreSQL)

The transactional schema follows a normalized design optimized for write performance:

```
customers --< orders --< order_items >-- products
    |                                        |
    |                                    inventory
    +--< sessions --< events
    +--< reviews >-- products
```

Key design decisions:
- **Partitioned tables**: `orders` partitioned by month, `events` by day
- **ENUM types**: order status, event types — validated at DB level
- **Trigger-based** `updated_at` timestamps on all mutable tables
- **Partial indexes** on `orders(status)` for active order queries
- **BRIN indexes** on timestamp columns for range scans

### Data Warehouse (Redshift)

Star schema optimized for analytical queries:

```
dim_date --< fact_orders >-- dim_customers
dim_products --< fact_orders    dim_geography
dim_date --< fact_events >-- dim_customers
```

Key optimizations:
- `fact_orders` DISTKEY on `customer_key` (co-locates with dim_customers)
- `fact_orders` SORTKEY on `order_date_key` (range scans on date)
- `dim_date` DISTSTYLE ALL (broadcast to all nodes)
- Column encoding: ZSTD for text, AZ64 for numeric, DELTA for dates

---

## ETL Pipeline

### Schedule

| Job | Schedule | Description |
|-----|----------|-------------|
| `rds-to-s3` | Every hour | Incremental extract from RDS |
| `data-quality` | Every hour (after extract) | Validate extracted files |
| `s3-to-redshift` | Every 4 hours | Load validated data to warehouse |
| `partition-manager` | Daily at 00:00 UTC | Create/drop RDS partitions |
| `mv-refresh` | Daily at 02:00 UTC | Refresh materialized views |
| `archive-old-data` | Weekly Sunday 03:00 UTC | Archive >2yr data to S3 |

### Glue Job Bookmarks

All Glue extract jobs use job bookmarks to track watermarks, ensuring idempotent incremental loads without duplicates.

### SCD Type 2

Dimension table updates use Slowly Changing Dimension Type 2:
- New rows get `is_current = TRUE`, `effective_from = now()`
- Changed rows: old row gets `is_current = FALSE`, `effective_to = now()`
- Enables point-in-time analysis

---

## Analytics Queries

The platform includes 20+ production analytics queries in `database/redshift/03_analytics_queries.sql`:

1. Customer Lifetime Value (LTV) with confidence intervals
2. RFM Segmentation (Recency, Frequency, Monetary)
3. Cohort Retention Analysis (monthly cohorts)
4. Product Recommendation Scoring (collaborative filtering features)
5. Revenue Attribution (first-touch, last-touch, linear models)
6. Inventory Turnover Rate by category
7. Customer Churn Prediction Features
8. Basket Analysis (co-purchase patterns)
9. Funnel Conversion Rates
10. Day-over-Day, Week-over-Week growth (window functions)
11. Geographic Revenue Heatmap
12. Moving Averages and Forecasting Base Data
13. Return Rate Analysis
14. Discount Effectiveness Analysis
15. Session-to-Purchase Conversion
16. Product Affinity by Customer Segment
17. Seasonal Trend Decomposition
18. CLV Decile Distribution
19. Order Velocity and Peak Hour Analysis
20. Cross-Sell Opportunity Scoring

---

## Security

- All database credentials stored in **AWS Secrets Manager** — never in code or config files
- RDS in **private subnets** — no public internet access
- Redshift in **private subnets** — accessible only via VPC
- Lambda functions use **least-privilege IAM roles**
- S3 buckets have **server-side encryption** (SSE-S3) and **versioning** enabled
- CloudFormation uses **deletion protection** on stateful resources
- All inter-service communication uses **VPC endpoints** where available

---

## Monitoring and Alerting

CloudWatch alarms are configured for:
- RDS CPU > 80% for 5 minutes
- RDS free storage < 10 GB
- Redshift disk usage > 85%
- Glue job failures
- Lambda error rate > 1%
- SQS queue depth > 10,000 messages

---

## Cost Estimate (us-east-1, monthly)

| Service | Configuration | Estimated Cost |
|---------|---------------|----------------|
| RDS PostgreSQL | db.t3.medium, Multi-AZ, 100GB | ~$130 |
| Redshift | dc2.large x 2 nodes | ~$360 |
| Glue | 10 DPU jobs, ~100 runs/month | ~$50 |
| Lambda | 1M invocations/month | ~$5 |
| S3 | 500 GB + requests | ~$15 |
| Data Transfer | Within VPC | ~$0 |
| **Total** | | **~$560/month** |

---

## Development

### Running Tests

```bash
# Unit tests for Lambda functions
pytest lambda/ -v

# Integration tests (requires AWS credentials)
pytest tests/integration/ -v --aws-region us-east-1
```

### Local Development with Docker

```bash
# Start local PostgreSQL
docker-compose up -d postgres

# Run migrations against local DB
psql -h localhost -U dbadmin -d ecommerce -f database/schema/01_init_rds.sql

# Seed local data
python scripts/seed_data.py --local --customers 1000 --orders 10000
```

---

## Contributing

1. Branch from `main` using `feature/` or `fix/` prefix
2. SQL changes must include migration scripts with rollback
3. Glue jobs must pass `data_quality.py` validation
4. Update `CHANGELOG.md` for any schema changes

---

## License

MIT License — see LICENSE file for details.
