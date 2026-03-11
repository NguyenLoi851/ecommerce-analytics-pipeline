# ecommerce-analytics-pipeline

Databricks-first, cloud-native analytics engineering project using the Brazilian E-Commerce Public Dataset by Olist.

Dataset source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## 1) Project Goal

Build a production-style analytics pipeline that:
- lands raw Olist data in cloud storage (GCS),
- ingests into Delta tables on Databricks (Bronze),
- transforms with dbt into conformed analytics models (Silver/Gold),
- serves curated marts for BI/reporting,
- runs fully in cloud with Databricks as the core platform.

## 2) Guiding Principles

- Databricks-first orchestration and compute (minimal external tools).
- dbt for transformation logic, testing, and documentation.
- Unity Catalog for governance, lineage, and permissions.
- GCS as storage layer.
- Keep Terraform/Airflow optional and add only when needed.

## 3) Target Architecture

1. **Source**: Kaggle Olist CSVs.
2. **Landing zone**: GCS bucket/prefix (`raw/olist/...`).
3. **Ingestion** (Databricks Workflows + notebooks/jobs): CSV -> Bronze Delta.
4. **Transformation** (dbt on Databricks SQL Warehouse): Bronze -> Silver -> Gold.
5. **Consumption**: Databricks SQL dashboards or external BI tools.
6. **Monitoring & quality**: Job alerts + `dbt test` + data quality checks.

## 4) Data Layers

### Bronze (raw, minimally transformed)
- One table per source file (orders, order_items, payments, products, customers, sellers, reviews, geolocation, etc.).
- Add metadata columns: `_ingest_ts`, `_source_file`, `_batch_id`.

### Silver (cleaned, conformed)
- Standardized types, null handling, de-duplication.
- Conformed dimensions and transactional facts.
- Expected core models:
  - `dim_customers`
  - `dim_sellers`
  - `dim_products`
  - `dim_geolocation`
  - `fct_orders`
  - `fct_order_items`
  - `fct_payments`
  - `fct_reviews`

### Gold (business marts)
- KPI-focused models optimized for reporting:
  - `mart_sales_daily`
  - `mart_category_performance`
  - `mart_customer_cohorts`
  - `mart_delivery_sla`

## 5) Detailed Implementation Plan

## Phase 0 — Foundation

1. Provision Databricks workspace on GCP.
2. Enable Unity Catalog and attach metastore.
3. Create catalogs/schemas:
	- `dev.bronze`, `dev.silver`, `dev.gold`
	- `prod.bronze`, `prod.silver`, `prod.gold`
4. Create GCS buckets (or one bucket with prefixes):
	- `olist-raw`, `olist-curated`, `olist-logs`
5. Configure Databricks access to GCS (service account + storage credential + external location).
6. Create a SQL Warehouse for dbt.
7. Create repo structure and connect Databricks Repos to GitHub.

## Phase 1 — Ingestion to Bronze

1. Download dataset and upload CSVs to GCS raw path.
2. Build ingestion notebooks/jobs in Databricks:
	- schema inference/definition,
	- robust type casting,
	- idempotent load pattern.
3. Write Bronze Delta tables.
4. Add baseline checks:
	- row-count reconciliation,
	- null checks on key columns,
	- duplicate checks.
5. Schedule with Databricks Workflows.

**Deliverable**: all source files reliably loaded to Bronze Delta tables.

## Phase 2 — dbt Silver Models

1. Bootstrap dbt project with `dbt-databricks`.
2. Create `sources.yml` and staging models (`stg_*`).
3. Build conformed Silver models (dimensions and facts).
4. Add dbt tests:
	- `not_null`, `unique`, `relationships`, `accepted_values`.
5. Add incremental strategy where appropriate.
6. Generate and publish dbt docs.

**Deliverable**: trusted, test-covered Silver layer.

## Phase 3 — Gold Analytics Marts

1. Define KPI specs (GMV, AOV, order volume, repeat rate, delivery SLA, payment mix).
2. Implement Gold marts.
3. Validate KPI logic with reconciliation queries.
4. Expose marts via Databricks SQL Warehouse.
5. Build a first dashboard for stakeholders.

**Deliverable**: business-ready analytics layer.

## Phase 4 — Productionization

1. Build end-to-end Databricks Workflow DAG:
	- ingest -> dbt run -> dbt test -> publish.
2. Configure alerting (failures/SLA delays).
3. Set up GitHub Actions CI:
	- `dbt deps`, `dbt parse`, `dbt test` (dev target).
4. Define deployment strategy (`dev` -> `prod`).
5. Add operational runbook and backfill strategy.

**Deliverable**: stable, scheduled, observable cloud pipeline.

## Phase 5 — Optional Enhancements

- Add Terraform for infrastructure as code.
- Add Airflow only if cross-platform orchestration is needed.
- Delta optimizations (`OPTIMIZE`, partition strategy, maintenance tasks).
- Data contracts and stricter SLAs.
- PII masking / column-level access controls.

## 6) First-Time Databricks Setup Checklist

Use this exact sequence for your first successful run:

1. Create Databricks workspace (GCP).
2. Enable Unity Catalog and metastore.
3. Create compute policy and starter cluster.
4. Create SQL Warehouse.
5. Connect GitHub in Databricks user settings (PAT or GitHub App).
6. Clone this repo into Databricks Repos.
7. Create catalogs/schemas for `dev` and `prod`.
8. Configure GCS access credentials + external location.
9. Test notebook read from GCS and write to Delta.
10. Create secret scope for tokens/keys.
11. Run ingestion job once.
12. Run dbt (`dbt run`, `dbt test`) against `dev`.
13. Create and schedule Databricks Workflow.
14. Promote same logic to `prod` target.

## 7) Recommended Repository Layout

```text
ecommerce-analytics-pipeline/
├── README.md
├── databricks/
│   ├── notebooks/
│   ├── jobs/
│   └── workflows/
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/
│   └── macros/
├── docs/
│   ├── architecture.md
│   ├── runbook.md
│   └── kpi_definitions.md
└── .github/
	 └── workflows/
```
