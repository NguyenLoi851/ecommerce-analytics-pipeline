# Development History

This document archives the phase-by-phase development log for the ecommerce analytics pipeline.
It is an internal reference — use the [runbook](runbook.md) for operational instructions.

---

## Phase 0 — Foundation

**Goal**: provision all infrastructure and validate end-to-end connectivity before writing any data.

1. Provision Databricks workspace on AWS.
2. Enable Unity Catalog and attach metastore.
3. Create catalogs/schemas:
   - `dev.bronze`, `dev.silver`, `dev.gold`
   - `prod.bronze`, `prod.silver`, `prod.gold`
4. ✅ Create the raw S3 bucket with Terraform.
5. Configure Databricks access to S3 (IAM role + storage credential + external location).
6. Create repo structure and connect Databricks Repos to GitHub.

**Deliverable**: clean infrastructure, smoke test passes.

### Phase 0 Execution Checklist

1. Create Databricks workspace (AWS).
2. Enable Unity Catalog and metastore.
3. Create compute policy and starter cluster.
4. Create SQL Warehouse.
5. Connect GitHub in Databricks user settings (PAT or GitHub App).
6. Clone this repo into Databricks Repos.
7. Create catalogs/schemas for `dev` and `prod`.
8. Create the raw S3 bucket with Terraform.
9. Create IAM role for Databricks Unity Catalog access to S3 buckets.
10. Configure external locations in Databricks SQL.
11. Test notebook read from S3 and write to Delta.

**What to run**:
- `cd terraform && terraform init && terraform plan && terraform apply`
- `databricks/sql/00_unity_catalog_setup.sql`
- `databricks/sql/01_external_location_s3.sql`
- Notebook: `databricks/notebooks/00_phase0_smoke_test.py`

---

## Phase 1 — Ingestion to Bronze

**Goal**: ingest all Olist CSVs from S3 into Delta Bronze tables.

1. Download dataset and upload CSVs to S3 raw path.
2. Set up dbt only after raw data is in S3:
   - bootstrap dbt project with `dbt-databricks`,
   - configure `profiles.yml` and validate connection (`dbt debug`).
3. Build ingestion notebooks/jobs in Databricks:
   - schema inference/definition,
   - robust type casting,
   - idempotent load pattern.
4. Write Bronze Delta tables.
5. Add baseline checks:
   - row-count reconciliation,
   - null checks on key columns,
   - duplicate checks.
6. Schedule with Databricks Workflows.

**Deliverable**: all source files reliably loaded to Bronze Delta tables.

### Bronze tables produced

| Table | Source File |
|---|---|
| `bronze.orders` | olist_orders_dataset.csv |
| `bronze.order_items` | olist_order_items_dataset.csv |
| `bronze.order_payments` | olist_order_payments_dataset.csv |
| `bronze.order_reviews` | olist_order_reviews_dataset.csv |
| `bronze.customers` | olist_customers_dataset.csv |
| `bronze.sellers` | olist_sellers_dataset.csv |
| `bronze.products` | olist_products_dataset.csv |
| `bronze.geolocation` | olist_geolocation_dataset.csv |
| `bronze.product_category_name_translation` | product_category_name_translation.csv |

---

## Phase 2 — dbt Silver Models

**Goal**: build conformed, test-covered Silver dimensions and fact tables.

1. ✅ Create `sources.yml` and staging models (`stg_*`).
2. ✅ Build conformed Silver models (dimensions and facts).
3. ✅ Add dbt tests: `not_null`, `unique`, `relationships`, `accepted_values`.
4. Add incremental strategy where appropriate.
5. Generate and publish dbt docs.

**Deliverable**: trusted, test-covered Silver layer.

### Silver models produced

| Model | Type | Key logic |
|---|---|---|
| `dim_customers` | Dimension | Deduplicates on `customer_unique_id`; standardises city/state casing |
| `dim_sellers` | Dimension | Standardises city/state casing; casts ZIP to string |
| `dim_products` | Dimension | Joins English category name from translation table; casts numeric dims |
| `dim_geolocation` | Dimension | Aggregates many lat/lng samples to one centroid per ZIP prefix |
| `fct_orders` | Fact | Refs `stg_orders`; adds `is_delivered`, `actual/estimated_delivery_days`, `delivery_delay_days` |
| `fct_order_items` | Fact | Casts price/freight; adds derived `total_item_value` |
| `fct_payments` | Fact | One row per `(order_id, payment_sequential)`; validates payment types |
| `fct_reviews` | Fact | Deduplicates on `review_id`; adds `review_response_days` |

---

## Phase 3 — Gold Analytics Marts

**Goal**: implement KPI-focused marts for BI reporting.

1. Define KPI specs (GMV, AOV, order volume, repeat rate, delivery SLA, payment mix).
2. Implement Gold marts.
3. Validate KPI logic with reconciliation queries.
4. Expose marts via Databricks SQL Warehouse.
5. Build a first dashboard for stakeholders.

**Deliverable**: business-ready analytics layer.

### Gold marts produced

| Model | Grain | KPI coverage |
|---|---|---|
| `mart_sales_daily` | `order_date` | GMV, AOV, order volume, delivered volume, payment mix by method |
| `mart_category_performance` | `order_date`, `product_category_name_english` | Category GMV, AOV, item/order volume |
| `mart_customer_cohorts` | `cohort_month`, `order_month` | Repeat behavior via retention cohorts |
| `mart_delivery_sla` | `order_date` | On-time delivery rate, late volume, average delay |

---

## Phase 4 — Productionisation

**Goal**: wire the full pipeline into a scheduled, observable, production-grade system.

1. Build end-to-end Databricks Workflow DAG: ingest → dbt run → dbt test → publish.
2. Configure alerting (failures/SLA delays).
3. Set up GitHub Actions CI: `dbt deps`, `dbt parse`, `dbt test` (dev target), prod deploy gate.
4. Define deployment strategy (`dev` → `prod`).
5. Add operational runbook and backfill strategy.

**Deliverable**: stable, scheduled, observable cloud pipeline.

### CI pipeline design

```
PR opened / pushed
      │
      ▼
[dbt-validate]  ← dbt deps + dbt parse (no cluster)
      │  pass
      ▼
[dbt-test]      ← dbt debug + dbt test --select staging silver gold (dev warehouse)
      │  pass
      ▼
PR can be merged → [dbt-promote-prod] (waits for manual approval)
```

---

## Phase 5 — Enhancements

### ✅ Incremental Bronze ingestion

- Uses an `_ingestion_registry` Delta table to track successfully loaded files.
- Subsequent runs skip already-loaded tables; failed loads are automatically retried.
- `geolocation` and `product_category_name_translation` always perform a full overwrite.
- `FORCE_RELOAD=true` widget bypasses the registry for all tables in a single run.

### ✅ SCD Type 2 — dbt snapshot approach

Added snapshot-backed models `dim_customers_scd2` and `dim_products_scd2`:
- exposes `valid_from`, `valid_to`, `is_current` columns,
- keeps existing `dim_customers` and `dim_products` unchanged for downstream compatibility.

Files:
- `dbt/snapshots/snap_dim_customers_scd2.sql`
- `dbt/snapshots/snap_dim_products_scd2.sql`
- `dbt/models/silver/dim_customers_scd2.sql`
- `dbt/models/silver/dim_products_scd2.sql`
- `dbt/models/silver/dim_scd2.yml`

### ✅ SCD Type 2 — Databricks MERGE approach (in parallel)

Added incremental MERGE models `dim_customers_scd2_merge` and `dim_products_scd2_merge`:
- Uses Databricks MERGE hooks to close old versions and insert new current versions.
- Keeps snapshot-based SCD2 models intact for side-by-side comparison.

Files:
- `dbt/macros/scd2_merge_hooks.sql`
- `dbt/models/silver/dim_customers_scd2_merge.sql`
- `dbt/models/silver/dim_products_scd2_merge.sql`

### ✅ Full reset tooling

- Delete raw landing data only: `raw/olist`.
- Delete Auto Loader snapshot/checkpoint state only: `state/autoloader/olist`.
- Delete both raw + state in one command.
- Drop and recreate all layer schemas (`bronze`, `silver`, `gold`) in Databricks.

Script: `scripts/reset_s3_data.py`
SQL: `databricks/sql/02_reset_layers.sql`

### Docs restructure (this change)

- README rewritten as lean quickstart (no phase terminology).
- `docs/development-history.md` (this file) archives all phase notes.
- `docs/runbook.md` consolidates operational instructions.
- `docs/troubleshooting.md` — common failures and fixes.
- `docs/adr/` — architecture decision records.

---

## First-Time S3 + IAM Setup

If this is your first S3 + Databricks setup, use this exact flow:

1. Create the raw S3 bucket with Terraform (`terraform/`).
2. In Databricks Account Console, open **Data → Credentials** and start creating an AWS IAM role-based credential.
3. Databricks will show trust-policy requirements (AWS principal + external ID). Copy these values.
4. In AWS IAM, create a role (e.g. `databricks-olist-s3-role`) and paste the trust policy from Databricks.
5. Attach an S3 policy to this IAM role with at least:
   - bucket-level: `s3:ListBucket`, `s3:GetBucketLocation`
   - object-level: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`
   - resources for the raw bucket ARN (bucket + `/*`).
6. Copy the IAM role ARN.
7. Run `databricks/sql/01_external_location_s3.sql` and replace placeholders:
   - `<databricks-s3-access-role-arn>`
   - `<your-raw-bucket>`

Example S3 policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
         "Resource": ["arn:aws:s3:::<your-raw-bucket>"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
         "Resource": ["arn:aws:s3:::<your-raw-bucket>/*"]
    }
  ]
}
```
