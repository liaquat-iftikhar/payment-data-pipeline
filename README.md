# Payment Data Pipeline

This repository contains a scalable data engineering pipeline for payment data ingestion, transformation, and enrichment. The pipeline integrates with external APIs, processes data using Apache Spark, and stores curated and enriched datasets on Amazon S3 with Athena integration for analytics.

---

## Project Structure

- `pipeline/`
  - `fetcher/` - Components for fetching raw payment data from external APIs and uploading to S3
  - `curated/` - Curated data processing pipelines and Athena table management
  - `enriched/` - Enrichment pipelines performing aggregations and advanced transformations
- `dags/` - Apache Airflow DAGs for orchestrating pipeline stages
- `tests/` - Unit and integration tests
- `config/` - Configuration files managed by Dynaconf
- `Dockerfile` - Docker image for running the pipeline
- `docker-compose.yml` - Compose file to set up dependencies and run services
- `Makefile` - Utility commands for setup, testing, linting, and cleaning

---

## Features

- Robust data fetching with retry and backoff
- Data quality validations without external frameworks
- Currency conversion handling for USD and EUR
- Curated and enriched datasets stored as partitioned Parquet on S3
- Integration with AWS Glue and Athena for data discovery and querying
- Airflow orchestration using TaskFlow API and Dynaconf configuration
- Containerized for local development and CI/CD pipelines

---

---
## Schema
### Raw Layer 
**PATH:** `s3://<bucket>/raw/payments_<start_date>_<end_date>/`

Each file is a JSON array of payment records fetched from the external API.

| Field            | Type   | Description                                           |
| ---------------- | ------ | ----------------------------------------------------- |
| `transaction_id` | string | Unique ID of the payment transaction                  |
| `game`           | string | Name or ID of the game                                |
| `price`          | float  | Amount paid in the given currency                     |
| `currency`       | string | Currency code (e.g., "USD", "EUR")                    |
| `status`         | string | Status of the transaction (e.g., "SUCCESS", "FAILED") |
| `payment_date`   | string | Date of transaction (format: YYYY-MM-DD)              |

### Curated Layer
**PATH:** `s3://<bucket>/curated/payments/`

Stored as partitioned Parquet by payment_date. Same fields as raw, stored in a normalized format:

| Column           | Type                      |
| ---------------- | ------------------------- |
| `transaction_id` | string                    |
| `game`           | string                    |
| `price`          | double                    |
| `currency`       | string                    |
| `status`         | string                    |
| `payment_date`   | string (partition column) |


### Enriched Layer
**PATH:** `s3://<bucket>/enriched/daily_game_payments/`

Partitioned by payment_date. Aggregated metrics per game and date.

| Column                    | Type   | Description                       |
| ------------------------- | ------ | --------------------------------- |
| `game`                    | string | Game name                         |
| `payment_date`            | string | Partition column                  |
| `total_revenue_in_usd`    | double | Sum of successful payments in USD |
| `successful_transactions` | long   | Count of successful transactions  |

---

## Setup

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (optional but recommended)
- AWS credentials configured for S3 and Glue access

### Local environment setup

```bash
make setup
```

### Running tests

```bash
make test
```

### Linting and formatting

```bash
make lint
make format
```

### Cleaning build artifacts

```bash
make clean
```
---
## Enhancements
- Integrate a full-fledged data quality framework (e.g., Great Expectations) to improve validation, monitoring, and alerting.
- Expand unit and integration test coverage for greater reliability and faster debugging.
- Replace static currency conversion rates with a reliable, dynamic external service or API for up-to-date exchange rates.
- Add support for additional currencies beyond USD and EUR to accommodate broader payment datasets.