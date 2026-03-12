# ecommerce-analytics-pipeline

Databricks-first, cloud-native analytics engineering project using the Brazilian E-Commerce Public Dataset by Olist.

Dataset source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## 1) Project Goal

Build a production-style analytics pipeline that:
- lands raw Olist data in cloud storage (S3),
- ingests into Delta tables on Databricks (Bronze),
- transforms with dbt into conformed analytics models (Silver/Gold),
- serves curated marts for BI/reporting,
- runs fully in cloud with Databricks as the core platform.

## 2) Guiding Principles

- Databricks-first orchestration and compute (minimal external tools).
- dbt for transformation logic, testing, and documentation.
- Unity Catalog for governance, lineage, and permissions.
- S3 as storage layer.
- Keep Terraform/Airflow optional and add only when needed.

## 3) Target Architecture

1. **Source**: Kaggle Olist CSVs.
2. **Landing zone**: S3 bucket/prefix (`raw/olist/...`).
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

1. Provision Databricks workspace on AWS.
2. Enable Unity Catalog and attach metastore.
3. Create catalogs/schemas:
	- `dev.bronze`, `dev.silver`, `dev.gold`
	- `prod.bronze`, `prod.silver`, `prod.gold`
4. Create S3 buckets (or one bucket with prefixes):
	- `olist-raw`, `olist-curated`, `olist-logs`
5. Configure Databricks access to S3 (IAM role + storage credential + external location).
6. Create repo structure and connect Databricks Repos to GitHub.

## Phase 1 — Ingestion to Bronze

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

## Phase 2 — dbt Silver Models

1. Create `sources.yml` and staging models (`stg_*`).
2. Build conformed Silver models (dimensions and facts).
3. Add dbt tests:
	- `not_null`, `unique`, `relationships`, `accepted_values`.
4. Add incremental strategy where appropriate.
5. Generate and publish dbt docs.

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

1. Create Databricks workspace (AWS).
2. Enable Unity Catalog and metastore.
3. Create compute policy and starter cluster.
4. Create SQL Warehouse.
5. Connect GitHub in Databricks user settings (PAT or GitHub App).
6. Clone this repo into Databricks Repos.
7. Create catalogs/schemas for `dev` and `prod`.
8. Create S3 buckets (`raw`, `curated`, `logs`) with Terraform.
9. Create IAM role for Databricks Unity Catalog access to S3 buckets.
10. Configure external locations in Databricks SQL.
11. Test notebook read from S3 and write to Delta.
12. Run ingestion job once.
13. Set up and run dbt (after CSV upload to S3): `dbt debug`, `dbt run`, `dbt test` against `dev`.
14. Create and schedule Databricks Workflow.
15. Promote same logic to `prod` target.

## 7) First-Time S3 + IAM Setup for Databricks (Important)

If this is your first S3 + Databricks setup, use this exact flow:

1. Create the S3 buckets with Terraform (`terraform/`).
2. In Databricks Account Console, open **Data** -> **Credentials** and start creating an AWS IAM role-based credential.
3. Databricks will show trust-policy requirements (AWS principal + external ID). Copy these values.
4. In AWS IAM, create a role (for example `databricks-olist-s3-role`) and paste the trust policy from Databricks.
5. Attach an S3 policy to this IAM role with at least:
	- bucket-level: `s3:ListBucket`, `s3:GetBucketLocation`
	- object-level: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`
	- resources for both raw and curated bucket ARNs (bucket + `/*`).
6. Copy the IAM role ARN.
7. Run `databricks/sql/01_external_location_s3.sql` and replace placeholders:
	- `<databricks-s3-access-role-arn>`
	- `<your-raw-bucket>`
	- `<your-curated-bucket>`
8. Validate access with the smoke test notebook.

Example S3 policy (replace bucket names):

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": ["s3:ListBucket", "s3:GetBucketLocation"],
			"Resource": [
				"arn:aws:s3:::<your-raw-bucket>",
				"arn:aws:s3:::<your-curated-bucket>"
			]
		},
		{
			"Effect": "Allow",
			"Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
			"Resource": [
				"arn:aws:s3:::<your-raw-bucket>/*",
				"arn:aws:s3:::<your-curated-bucket>/*"
			]
		}
	]
}
```

## 8) Recommended Repository Layout

```text
ecommerce-analytics-pipeline/
├── README.md
├── databricks/
│   ├── notebooks/
│   │   └── 00_phase0_smoke_test.py
│   ├── sql/
│   │   ├── 00_unity_catalog_setup.sql
│   │   └── 01_external_location_s3.sql
│   └── workflows/
│       └── phase0_workflow.md
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_orders.yml
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/
│   └── macros/
├── docs/
│   ├── architecture.md
│   └── phase0-runbook.md
├── terraform/
│   ├── main.tf
│   ├── providers.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
└── .github/
	 └── workflows/
		  └── dbt-ci.yml
```

## 9) Phase 0: What to Run Now

1. Provision S3 buckets with Terraform:
	- `cd terraform`
	- `cp terraform.tfvars.example terraform.tfvars`
	- update `terraform.tfvars` values
	- `terraform init && terraform plan && terraform apply`
2. Run SQL setup in Databricks SQL Editor:
	- `databricks/sql/00_unity_catalog_setup.sql`
	- `databricks/sql/01_external_location_s3.sql`
3. Run smoke test notebook:
	- `databricks/notebooks/00_phase0_smoke_test.py`
4. Follow the full execution checklist:
	- `docs/phase0-runbook.md`
