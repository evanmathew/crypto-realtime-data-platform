
# Crypto Realtime Data Platform

> Production-grade real-time crypto streaming pipeline — CoinGecko API → AWS MSK Kafka → Snowflake → dbt → dbt test & validation · Orchestrated by Apache Airflow

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/snowflake-%2329B5E8.svg?style=for-the-badge&logo=snowflake&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![AWS Lambda](https://img.shields.io/badge/AWS%20Lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white)

---

## Overview

A production-grade, end-to-end real-time data engineering pipeline that ingests live cryptocurrency market data from the CoinGecko API, streams it through Apache Kafka on AWS MSK, lands it in Snowflake, transforms it through a layered dbt architecture, and validates data quality with dbt  — all orchestrated by Apache Airflow.

Built to demonstrate real-world data engineering patterns used at financial data companies — event streaming, incremental transformations, SCD snapshots, data quality gates, and CI/CD.

---

## Architecture

```
CoinGecko API (free, no key)
        ↓
AWS Lambda 1 — Producer
  · Fetches 5 coins every 5 minutes via EventBridge
  · Publishes to Kafka topic (crypto_prices_raw)
        ↓
AWS MSK — Apache Kafka 3.9.x
  · 3 brokers (ap-south-1)
  · 3 partitions · replication factor 2
  · Messages keyed by coin ID for ordered partitioning
        ↓
AWS Lambda 2 — Consumer
  · Triggered automatically by MSK
  · Authenticates to Snowflake via RSA JWT (no password over wire)
  · Writes raw JSON payload to Snowflake RAW layer as VARIANT
        ↓
Snowflake — CRYPTO_DB
  · RAW schema         → raw JSON ingestion
  · STAGING schema     → cleaned, typed columns (dbt views)
  · INTERMEDIATE schema → hourly aggregations + market dominance
  · MARTS schema       → OHLC daily prices + coin performance summary
  · SNAPSHOTS schema   → SCD Type 2 price history
        ↓
dbt — 5 models across 3 layers
  · Staging    → flatten VARIANT JSON into typed columns
  · Intermediate → hourly aggregations + market dominance %
  · Marts      → daily OHLC + BI-ready performance summary
  · Snapshots  → track price changes over time (SCD Type 2)
        ↓
Great Expectations
  · Data quality gates at RAW, STAGING, and MART layers
  · Schema enforcement, null checks, range validation
  · Pipeline fails and alerts if expectations not met
        ↓
Apache Airflow
  · Orchestrates full pipeline on 5-minute schedule
  · BranchOperator skips dbt if no new data 
```

Click Here to Get Architecture Visual: [Architecture](https://app.affine.pro/workspace/b87fd5ed-c7d1-49c2-a7bd-9c4d178504cf/LxzbsUSVJ7i-GmuW4jRf5?mode=edgeless)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | AWS Lambda (Python 3.11), CoinGecko API |
| Streaming | Apache Kafka 3.9.x on AWS MSK |
| Storage | Snowflake (Standard Edition) |
| Transformation | dbt Core 1.11 + dbt-snowflake |
| Data Quality | DBT |
| Orchestration | Apache Airflow 2.9.1 |
| Cloud | AWS (Lambda, MSK, EC2, Secrets Manager, EventBridge, CloudWatch) |
| IaC / DevOps | GitHub Actions CI/CD |
| Auth | RSA Key Pair JWT (Snowflake REST API) |

---

## Project Structure

```
crypto-realtime-data-platform/
│
├── lambda/
│   ├── producer/
│   │   └── lambda_function.py        # CoinGecko → Kafka producer
│   └── consumer/
│       └── lambda_function.py        # Kafka → Snowflake consumer (JWT auth)
│
├── dbt/
│   └── crypto_pipeline/
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml
│       │   │   ├── schema.yml
│       │   │   └── stg_crypto_prices.sql
│       │   ├── intermediate/
│       │   │   ├── int_crypto_hourly.sql
│       │   │   └── int_crypto_market_dominance.sql
│       │   └── marts/
│       │       ├── mart_crypto_daily_ohlc.sql
│       │       └── mart_coin_performance_summary.sql
│       └── snapshots/
│           └── snap_crypto_prices.sql
│
├── airflow/
│   └── dags/
│       └── crypto_pipeline_dag.py    # Full orchestration DAG
│
├── docs/
│   └── architecture.png              # Architecture diagram
│
└── README.md
```

---

## dbt Model Lineage

```
CRYPTO_DB.RAW.CRYPTO_PRICES_RAW  (source)
              ↓
      stg_crypto_prices           (view · cleans + flattens JSON)
              ↓
    ┌─────────────────────────────────────┐
    ↓                                     ↓
int_crypto_hourly              int_crypto_market_dominance
(incremental · hourly OHLC)    (incremental · market share %)
    ↓                                     ↓
mart_crypto_daily_ohlc         mart_coin_performance_summary
(incremental · daily candles)  (incremental · BI-ready table)
                                          ↓
                               snap_crypto_prices
                               (SCD Type 2 · price history)
```

---

## Key Engineering Decisions

### Why Kafka instead of writing directly to Snowflake?
Decouples the producer from all consumers. If Snowflake is slow or down, messages are safely buffered in Kafka for up to 7 days. New consumers (ML model, S3 archive, alerts) can be added without touching the producer.

### Why VARIANT column in Snowflake RAW layer?
Stores the full raw JSON payload without schema enforcement at ingestion time. No data is ever lost at the landing layer — dbt handles flattening and typing in the staging layer. This is the standard pattern for raw landing zones.

### Why RSA Key Pair JWT auth instead of password?
Private key never leaves AWS Secrets Manager. No password transmitted over the network. Industry standard for service-to-service authentication. Rotatable without code changes.

### Why incremental models in dbt?
New crypto data arrives every 5 minutes — reprocessing the full historical table on every run is wasteful. Incremental models only process new records since the last run, reducing compute cost and run time significantly.

### Why SCD Type 2 snapshot?
Tracks how coin prices, market dominance, and ATH distances change over time. Without a snapshot, you only ever see the latest state — historical analysis becomes impossible.

### Why Lambda in a private VPC subnet?
MSK is not publicly accessible — it runs inside a VPC. Lambda must be in the same VPC to reach MSK. NAT Gateway provides outbound internet access for CoinGecko API calls while keeping Lambda private.

### Why principle of least privilege for Snowflake?
`CRYPTO_PIPELINE_ROLE` has only the exact permissions needed — INSERT/SELECT on RAW, SELECT on other schemas. No ACCOUNTADMIN in application code. If credentials are compromised, blast radius is minimal.

---

## Snowflake Setup

Run the full setup script in Snowflake worksheet as ACCOUNTADMIN:

```sql
-- Database and schemas
CREATE DATABASE IF NOT EXISTS CRYPTO_DB;
USE DATABASE CRYPTO_DB;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS;

-- Dedicated warehouse
CREATE WAREHOUSE IF NOT EXISTS CRYPTO_PIPELINE_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Dedicated role and user
CREATE ROLE IF NOT EXISTS CRYPTO_PIPELINE_ROLE;
CREATE USER IF NOT EXISTS CRYPTO_PIPELINE_USER
    DEFAULT_ROLE      = CRYPTO_PIPELINE_ROLE
    DEFAULT_WAREHOUSE = CRYPTO_PIPELINE_WH
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE CRYPTO_PIPELINE_ROLE TO USER CRYPTO_PIPELINE_USER;
GRANT ALL PRIVILEGES ON DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE CRYPTO_PIPELINE_WH TO ROLE CRYPTO_PIPELINE_ROLE;

-- Raw table
CREATE OR REPLACE TABLE CRYPTO_DB.RAW.CRYPTO_PRICES_RAW (
    RAW_DATA    VARIANT,
    COIN_ID     VARCHAR,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE      VARCHAR DEFAULT 'coingecko'
);
```

---

## AWS Setup

### Lambda IAM Role permissions required
```
AWSLambdaBasicExecutionRole
AmazonMSKFullAccess
SecretsManagerReadWrite
AWSLambdaVPCAccessExecutionRole
```

### Secrets Manager secret structure
Secret name: `crypto-pipeline/snowflake`
```json
{
  "SNOWFLAKE_ACCOUNT":     "your-account-identifier",
  "SNOWFLAKE_USER":        "CRYPTO_PIPELINE_USER",
  "SNOWFLAKE_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\n...",
  "SNOWFLAKE_DATABASE":    "CRYPTO_DB",
  "SNOWFLAKE_SCHEMA":      "RAW",
  "SNOWFLAKE_WAREHOUSE":   "CRYPTO_PIPELINE_WH",
  "SNOWFLAKE_ROLE":        "CRYPTO_PIPELINE_ROLE"
}
```

### Lambda environment variables
```
Lambda 1: MSK_BOOTSTRAP_SERVERS = b-1.xxx:9092,b-2.xxx:9092,b-3.xxx:9092
Lambda 2: SECRET_NAME = crypto-pipeline/snowflake
```

### VPC configuration
```
Both Lambda functions in SAME private subnet as MSK
NAT Gateway in public subnet for outbound internet (CoinGecko API)
Security group: inbound TCP 9092 from self (Lambda ↔ MSK)
```

---

## Running Locally

### Prerequisites
```bash
python 3.11+
dbt-snowflake
apache-airflow==2.9.1
```

### dbt setup
```bash
cd dbt/crypto_pipeline
pip install dbt-snowflake
dbt debug          # test Snowflake connection
dbt run            # run all 5 models
dbt test           # run 12 data tests
dbt snapshot       # run SCD Type 2 snapshot
dbt docs generate  # generate lineage docs
dbt docs serve     # open at localhost:8080
```

### Airflow setup
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --role Admin --email you@email.com --password yourpassword
airflow scheduler &
airflow webserver --port 8080 &
# open localhost:8080
```

---

## Data Quality — Great Expectations

Expectation suites validate data at 3 pipeline stages:

| Layer | Expectations |
|---|---|
| RAW | No nulls in COIN_ID · INGESTED_AT not null · SOURCE = 'coingecko' |
| STAGING | current_price_usd > 0 · price_change_pct_24h between -100 and 100 · 5 distinct coins |
| MARTS | coin_id unique · market_dominance_pct between 0 and 100 · no nulls in key columns |

---

## CI/CD

GitHub Actions workflow runs on every push:
1. `dbt compile` — validates all SQL
2. `dbt test` — runs all 12 data tests against Snowflake
3. Fails the PR if any test fails

---

## Lessons Learned

- Lambda layers must be built on Amazon Linux 2 — local Ubuntu builds produce incompatible compiled binaries (GLIBC mismatch)
- `snowflake-connector-python` has Rust compiled binaries incompatible with Lambda — use Snowflake REST API with JWT instead
- MSK requires Lambda to be in the same VPC — use NAT Gateway for outbound internet access
- Always use `producer.flush()` before Lambda exits — buffered messages are lost on termination
- `acks="all"` is non-negotiable for financial data — silent message loss is unacceptable
- Principle of least privilege: never use ACCOUNTADMIN in application code

---

## Coins Tracked

| Coin | Symbol | CoinGecko ID |
|---|---|---|
| Bitcoin | BTC | bitcoin |
| Ethereum | ETH | ethereum |
| XRP | XRP | ripple |
| BNB | BNB | binancecoin |
| Solana | SOL | solana |

---

## Author

**Evan Saju Mathew** — Analytics Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/evansajumathew)
[![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)](https://github.com/evanmathew)
[![Gmail](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:evanptc@gmail.com)
