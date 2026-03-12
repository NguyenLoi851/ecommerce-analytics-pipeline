# Architecture Overview

## Core Stack

- **Cloud**: AWS
- **Storage**: S3
- **Compute + Orchestration**: Databricks (Workflows, SQL Warehouse)
- **Transformations**: dbt (`dbt-databricks`)
- **Governance**: Unity Catalog
- **Version Control**: GitHub + Databricks Repos

## Data Flow

1. Olist files land in `s3://.../raw/olist/`.
2. Databricks ingestion jobs load to `dev.bronze.*` Delta tables.
3. dbt builds `dev.silver.*` and `dev.gold.*` models.
4. BI/dashboard queries use Gold tables through SQL Warehouse.

## Environment Strategy

- `dev` catalog for development and CI.
- `prod` catalog for scheduled production workloads.

## Orchestration Strategy

- Default: Databricks Workflows.
- Optional: Airflow only for multi-platform orchestration needs.
