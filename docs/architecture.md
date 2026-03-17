# Architecture Overview

## Core Stack

| Layer | Technology |
|---|---|
| Cloud | AWS |
| Storage | S3 |
| Compute | Databricks (serverless + SQL Warehouse) |
| Orchestration | Databricks Workflows |
| Transformations | dbt (`dbt-databricks` adapter) |
| Governance | Unity Catalog |
| CI/CD | GitHub Actions |
| IaC | Terraform (S3 + IAM only) |
| Version Control | GitHub + Databricks Repos |

See [adr/001-databricks-first.md](adr/001-databricks-first.md) and [adr/002-dbt-transformations.md](adr/002-dbt-transformations.md) for why these were chosen.

---

## Data Flow

```
Kaggle CSVs
    │
    ▼  scripts/upload_to_s3.py
S3  s3://<raw-bucket>/raw/olist/*.csv
    │
    ▼  Databricks notebook (01_bronze_ingestion_autoloader.py)
Bronze  dev.bronze.* / prod.bronze.*   (Delta, metadata columns added)
    │
    ▼  dbt (staging → silver)
Silver  dev.silver.* / prod.silver.*   (conformed dims + facts)
    │
    ▼  dbt (gold)
Gold    dev.gold.* / prod.gold.*       (KPI marts)
    │
    ▼
Databricks SQL Warehouse → BI dashboards
```

For Tableau-specific dashboard design and model-to-chart mapping, see [tableau_dashboard_instructions.md](tableau_dashboard_instructions.md).

---

## Data Layers

### Bronze
- One Delta table per source CSV.
- Metadata columns added: `_ingest_ts`, `_source_file`, `_batch_id`.
- No business logic — raw data preserved.

### Silver

| Model | Type |
|---|---|
| `dim_customers` | Dimension — deduplicated on `customer_unique_id` |
| `dim_sellers` | Dimension |
| `dim_products` | Dimension — English category name joined |
| `dim_geolocation` | Dimension — centroid per ZIP prefix |
| `fct_orders` | Fact — delivery KPIs derived |
| `fct_order_items` | Fact — `total_item_value` derived |
| `fct_payments` | Fact — one row per `(order_id, payment_sequential)` |
| `fct_reviews` | Fact — deduplicated, `review_response_days` added |

SCD Type 2 variants available: `dim_customers_scd2`, `dim_products_scd2` (snapshot) and `_scd2_merge` (MERGE). See [adr/003-scd2-snapshot-vs-merge.md](adr/003-scd2-snapshot-vs-merge.md).

### Gold

| Model | Grain | Key KPIs |
|---|---|---|
| `mart_sales_daily` | `order_date` | GMV, AOV, order volume, payment mix |
| `mart_category_performance` | `order_date`, `category` | Category GMV, AOV |
| `mart_customer_cohorts` | `cohort_month`, `order_month` | Retention cohorts |
| `mart_delivery_sla` | `order_date` | On-time delivery rate, avg delay |

---

## Environment Strategy

| Environment | dbt Target | Unity Catalog | Trigger |
|---|---|---|---|
| `dev` | `dev` | `dev.*` | Any PR or local run |
| `prod` | `prod` | `prod.*` | Merge to `main` + CI + manual approval |

---

## Orchestration Strategy

- **Default**: Databricks Workflows (`databricks/workflows/`).
- **CI**: GitHub Actions (`.github/workflows/`) — `dbt parse` + `dbt test` on PR, prod deploy on merge.
- **Optional**: Airflow only if multi-platform orchestration is required.

---
