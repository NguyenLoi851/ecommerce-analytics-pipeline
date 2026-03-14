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
4. ✅ Create S3 buckets (or one bucket with prefixes) with Terraform:
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

1. ✅ Create `sources.yml` and staging models (`stg_*`).
2. ✅ Build conformed Silver models (dimensions and facts).
3. ✅ Add dbt tests:
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
	- `dbt deps`, `dbt parse`, `dbt test` (dev target), prod deploy gate.
4. Define deployment strategy (`dev` -> `prod`).
5. Add operational runbook and backfill strategy.

**Deliverable**: stable, scheduled, observable cloud pipeline.

## Phase 5 — Optional Enhancements

- ✅ Implement incremental Bronze ingestion (avoid full reload) for most high-volume tables:
	- uses an `_ingestion_registry` Delta table to track successfully loaded files,
	- subsequent runs skip already-loaded tables; failed loads are automatically retried,
	- `geolocation` and `product_category_name_translation` always perform a full overwrite,
	- `FORCE_RELOAD=true` widget bypasses the registry for all tables in a single run.
- ✅ Prototype SCD Type 2 in Silver dimensions using dbt snapshots:
	- added snapshot-backed models `dim_customers_scd2` and `dim_products_scd2`,
	- exposes `valid_from`, `valid_to`, `is_current` columns,
	- keeps existing `dim_customers` and `dim_products` unchanged for downstream compatibility.
- ✅ Add Databricks `MERGE`-based SCD Type 2 option (in parallel with snapshot approach):
	- added incremental MERGE models `dim_customers_scd2_merge` and `dim_products_scd2_merge`,
	- uses Databricks MERGE hooks to close old versions and insert new current versions,
	- keeps snapshot-based SCD2 models intact for side-by-side comparison.
- Delta optimizations (`OPTIMIZE`, partition strategy, maintenance tasks).
- ✅ Add full reset tooling to rerun pipeline from scratch:
	- delete raw landing data only (`raw/olist`),
	- delete Auto Loader snapshot/checkpoint state only (`state/autoloader/olist`),
	- delete both raw + state in one command,
	- drop and recreate all layer schemas (`bronze`, `silver`, `gold`) in Databricks.
- Re-structure project docs:
	- split quickstart vs runbooks,
	- add troubleshooting + common failure playbooks,
	- add architecture decision log (ADR-style notes).

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
│   ├── workflows/
│   │   └── phase0_workflow.md
│   ├── notebooks/
│   │   ├── 00_phase0_smoke_test.py
│   │   ├── 01_bronze_ingestion.py        ← Phase 1
│   │   ├── 02_bronze_quality_checks.py  ← Phase 1
│   │   └── 03_publish_gold.py           ← Phase 4
│   ├── sql/
│   │   ├── 00_unity_catalog_setup.sql
│   │   └── 01_external_location_s3.sql
│   └── workflows/
│       ├── phase0_workflow.md
│       ├── phase1_workflow.json          ← Phase 1
│       └── phase4_workflow.json         ← Phase 4
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml             ← Phase 1
│   │   │   ├── stg_orders.sql           ← Phase 1
│   │   │   └── stg_orders.yml           ← Phase 1
│   │   ├── silver/
│   │   │   ├── dim_customers.sql        ← Phase 2
│   │   │   ├── dim_customers.yml        ← Phase 2
│   │   │   ├── dim_sellers.sql          ← Phase 2
│   │   │   ├── dim_sellers.yml          ← Phase 2
│   │   │   ├── dim_products.sql         ← Phase 2
│   │   │   ├── dim_products.yml         ← Phase 2
│   │   │   ├── dim_geolocation.sql      ← Phase 2
│   │   │   ├── dim_geolocation.yml      ← Phase 2
│   │   │   ├── fct_orders.sql           ← Phase 2
│   │   │   ├── fct_orders.yml           ← Phase 2
│   │   │   ├── fct_order_items.sql      ← Phase 2
│   │   │   ├── fct_order_items.yml      ← Phase 2
│   │   │   ├── fct_payments.sql         ← Phase 2
│   │   │   ├── fct_payments.yml         ← Phase 2
│   │   │   ├── fct_reviews.sql          ← Phase 2
│   │   │   └── fct_reviews.yml          ← Phase 2
│   │   └── gold/
│   │       ├── mart_sales_daily.sql             ← Phase 3
│   │       ├── mart_sales_daily.yml             ← Phase 3
│   │       ├── mart_category_performance.sql    ← Phase 3
│   │       ├── mart_category_performance.yml    ← Phase 3
│   │       ├── mart_customer_cohorts.sql        ← Phase 3
│   │       ├── mart_customer_cohorts.yml        ← Phase 3
│   │       ├── mart_delivery_sla.sql            ← Phase 3
│   │       └── mart_delivery_sla.yml            ← Phase 3
│   ├── tests/
│   └── macros/
├── docs/
│   ├── architecture.md
│   ├── phase0-runbook.md
│   ├── phase1-runbook.md                ← Phase 1
│   ├── phase4-runbook.md                ← Phase 4
│   └── deployment-strategy.md          ← Phase 4
├── scripts/
│   └── upload_to_s3.py                  ← Phase 1
├── terraform/
│   ├── main.tf
│   ├── providers.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
└── .github/
    └── workflows/
        └── dbt-ci.yml                   ← Phase 4
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

## 10) Phase 1: What to Run Now

> Prerequisite: Phase 0 complete (Unity Catalog + S3 external location validated).

1. Upload Olist CSVs to S3:
	```bash
	pip install kaggle boto3
	python scripts/upload_to_s3.py --bucket <your-raw-bucket> --prefix raw/olist --region us-east-1 --profile <your-project-profile>
	```
2. Configure dbt connection:
	```bash
	cp dbt/profiles.yml.example dbt/profiles.yml
	# edit dbt/profiles.yml with your Databricks host, http_path, token
	cd dbt && dbt debug --profiles-dir .
	```
3. Run Bronze ingestion notebook in Databricks:
	- `databricks/notebooks/01_bronze_ingestion.py`
	- Set `RAW_BUCKET` to your raw S3 bucket name.
4. Run quality checks notebook:
	- `databricks/notebooks/02_bronze_quality_checks.py`
5. Create Databricks Workflow for Phase 1
6. Follow the full execution checklist:
	- `docs/phase1-runbook.md`

## 11) Phase 2: What to Run Now

> Prerequisite: Phase 1 complete — all Bronze Delta tables populated in `dev.bronze`.

### Silver models built

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

### Run the Silver layer

1. Make sure `dbt/profiles.yml` is configured and `dbt debug` passes:
	```bash
	cd dbt && dbt debug --profiles-dir .
	```

2. Install dbt dependencies:
	```bash
	dbt deps --profiles-dir .
	```

3. Run only the Silver models:
	```bash
	dbt run --select silver --profiles-dir .
	```

4. Run all tests for the Silver layer:
	```bash
	dbt test --select silver --profiles-dir .
	```

5. Run staging + silver together (full Phase 2 surface):
	```bash
	dbt run --select staging silver --profiles-dir .
	dbt test --select staging silver --profiles-dir .
	```

6. Run a specific model and its upstream dependencies:
	```bash
	# e.g. rebuild fct_orders and everything it depends on
	dbt run --select +fct_orders --profiles-dir .
	```

7. Generate and serve dbt docs locally:
	```bash
	dbt docs generate --profiles-dir .
	dbt docs serve --profiles-dir .
	# opens http://localhost:8080 in your browser
	```

### Verify in Databricks

After a successful `dbt run`, confirm the tables exist:

```sql
SHOW TABLES IN dev.silver;
-- Expected: dim_customers, dim_sellers, dim_products, dim_geolocation,
--           fct_orders, fct_order_items, fct_payments, fct_reviews

-- Quick row-count sanity check
SELECT 'dim_customers'  AS model, COUNT(*) AS rows FROM dev.silver.dim_customers
UNION ALL
SELECT 'dim_sellers',           COUNT(*) FROM dev.silver.dim_sellers
UNION ALL
SELECT 'dim_products',          COUNT(*) FROM dev.silver.dim_products
UNION ALL
SELECT 'dim_geolocation',       COUNT(*) FROM dev.silver.dim_geolocation
UNION ALL
SELECT 'fct_orders',            COUNT(*) FROM dev.silver.fct_orders
UNION ALL
SELECT 'fct_order_items',       COUNT(*) FROM dev.silver.fct_order_items
UNION ALL
SELECT 'fct_payments',          COUNT(*) FROM dev.silver.fct_payments
UNION ALL
SELECT 'fct_reviews',           COUNT(*) FROM dev.silver.fct_reviews;
```

## 12) Phase 3: What to Run Now

> Prerequisite: Phase 2 complete — Silver models are built and tests are passing.

### Gold marts implemented

| Model | Grain | KPI coverage |
|---|---|---|
| `mart_sales_daily` | `order_date` | GMV, AOV, order volume, delivered volume, payment mix by method |
| `mart_category_performance` | `order_date`, `product_category_name_english` | Category GMV, AOV, item/order volume |
| `mart_customer_cohorts` | `cohort_month`, `order_month` | Repeat behavior via retention cohorts |
| `mart_delivery_sla` | `order_date` | On-time delivery rate, late volume, average delay |

### Run the Gold layer

1. Ensure dbt profile is valid:
   ```bash
   cd dbt && dbt debug --profiles-dir .
   ```

2. Install/refresh dependencies:
   ```bash
   dbt deps --profiles-dir .
   ```

3. Build only Gold marts:
   ```bash
   dbt run --select gold --profiles-dir .
   ```

4. Run Gold tests:
   ```bash
   dbt test --select gold --profiles-dir .
   ```

5. Rebuild Silver + Gold together (recommended after logic changes):
   ```bash
   dbt run --select silver gold --profiles-dir .
   dbt test --select silver gold --profiles-dir .
   ```

### Validate KPI logic in Databricks SQL

Run these reconciliation checks in Databricks SQL editor:

```sql
-- 1) GMV + order volume reconciliation: mart_sales_daily vs silver base
with base as (
	select
		cast(o.order_purchase_timestamp as date) as order_date,
		count(distinct o.order_id) as base_order_volume,
		round(sum(oi.total_item_value), 2) as base_gmv
	from dev.silver.fct_orders o
	left join dev.silver.fct_order_items oi
		on o.order_id = oi.order_id
	group by cast(o.order_purchase_timestamp as date)
)
select
	m.order_date,
	m.order_volume,
	b.base_order_volume,
	m.gmv,
	b.base_gmv,
	round(m.gmv - b.base_gmv, 2) as gmv_diff
from dev.gold.mart_sales_daily m
inner join base b
	on m.order_date = b.order_date
order by m.order_date desc
limit 30;

-- 2) Delivery SLA reconciliation: on-time rate from mart vs recompute
with base as (
	select
		cast(order_purchase_timestamp as date) as order_date,
		count(*) as delivered_orders,
		count(case when delivery_delay_days <= 0 then 1 end) as on_time_orders
	from dev.silver.fct_orders
	where is_delivered = true
	group by cast(order_purchase_timestamp as date)
)
select
	m.order_date,
	m.on_time_delivery_rate as mart_on_time_rate,
	round(b.on_time_orders / nullif(b.delivered_orders, 0), 4) as base_on_time_rate
from dev.gold.mart_delivery_sla m
inner join base b
	on m.order_date = b.order_date
order by m.order_date desc
limit 30;

-- 3) Payment mix sanity check: percentages should sum near 1.0
select
	order_date,
	payment_mix_credit_card_pct
	+ payment_mix_boleto_pct
	+ payment_mix_voucher_pct
	+ payment_mix_debit_card_pct
	+ payment_mix_not_defined_pct as payment_mix_total
from dev.gold.mart_sales_daily
order by order_date desc
limit 30;
```

### Expose via Databricks SQL Warehouse

1. In Databricks SQL, create a new dashboard.
2. Add tiles from these base marts:
   - `dev.gold.mart_sales_daily`
   - `dev.gold.mart_category_performance`
   - `dev.gold.mart_customer_cohorts`
   - `dev.gold.mart_delivery_sla`
3. Suggested first dashboard tiles:
   - Daily GMV + AOV trend
   - Payment mix stacked area (daily)
   - Top categories by GMV (last 30 days)
   - Cohort retention heatmap
   - On-time delivery rate trend

## 13) Phase 4: What to Run Now

> Prerequisite: Phase 3 complete — Gold marts built and tests passing in `dev`.

### A — Configure GitHub Secrets and Environment approval gate

1. Add these secrets under **Settings → Secrets and variables → Actions**:

	| Secret | Description |
	|---|---|
	| `DATABRICKS_HOST` | Dev workspace host |
	| `DATABRICKS_HTTP_PATH` | Dev SQL Warehouse HTTP path |
	| `DATABRICKS_TOKEN` | Dev Personal Access Token |
	| `DATABRICKS_HOST_PROD` | Prod workspace host |
	| `DATABRICKS_HTTP_PATH_PROD` | Prod SQL Warehouse HTTP path |
	| `DATABRICKS_TOKEN_PROD` | Prod Personal Access Token |

2. Create a **`production`** GitHub Environment with required reviewers:
	- **Settings → Environments → New environment** → `production` → add reviewers.

3. Enable branch protection on `main`:
	- Require PRs, require status checks (`dbt-validate`, `dbt-test`), require 1 review.

### B — Validate the CI Pipeline

```bash
git checkout -b feature/phase4-ci-validation
# make a trivial edit (e.g. add a comment to a .sql file)
git commit -am "ci: validate phase 4 ci pipeline"
git push origin feature/phase4-ci-validation
```

Open a PR and confirm both CI jobs pass before merging.

### C — Create the Production Databricks Workflow

1. In Databricks UI: **Workflows → Create job → ⋮ → Edit JSON**.
2. Paste one of these files:
	- `databricks/workflows/phase4_workflow.json` (current standard ingestion notebook)
	- `databricks/workflows/phase4_workflow_autoloader.json` (Auto Loader ingestion notebook)
	- `databricks/workflows/phase4_workflow_autoloader.json.example` (placeholder template for new environments)
3. Replace all `<placeholder>` values (GitHub username, SQL Warehouse ID, bucket name, emails).
4. Click **Create**.
5. Run manually once (**Run now**) to validate the full DAG before enabling the schedule.

### D — Enable the Daily Schedule

Once the manual run succeeds:

1. Open the Workflow → **Schedule** → Daily at **03:00 UTC** (or preferred time).
2. Set pause status to **Unpaused** → Save.

### E — Test Backfill

To re-process a historical date range:

```bash
databricks jobs run-now \
  --job-id <job-id> \
  --job-parameters '{"backfill_start_date": "2017-01-01", "backfill_end_date": "2018-12-31"}'
```

### F — Post-Deploy Verification

Run these queries in Databricks SQL after the first production run:

```sql
-- Gold mart freshness
SELECT MAX(order_date) AS latest_date FROM prod.gold.mart_sales_daily;

-- Row counts
SELECT 'mart_sales_daily'          AS model, COUNT(*) AS rows FROM prod.gold.mart_sales_daily
UNION ALL
SELECT 'mart_category_performance',           COUNT(*) FROM prod.gold.mart_category_performance
UNION ALL
SELECT 'mart_customer_cohorts',               COUNT(*) FROM prod.gold.mart_customer_cohorts
UNION ALL
SELECT 'mart_delivery_sla',                   COUNT(*) FROM prod.gold.mart_delivery_sla;
```

### G — Read the full runbooks

- `docs/phase4-runbook.md` — step-by-step setup, alerting configuration, failure scenarios.
- `docs/deployment-strategy.md` — branch strategy, CI gates, prod promotion, rollback procedure.

## 14) Reset Playbook (Run from Scratch)

Use this when you want a clean re-run of the entire project.

### A) Reset S3 data (raw and/or Auto Loader state)

Script: `scripts/reset_s3_data.py`

1. Preview deletions (safe dry-run):

```bash
python scripts/reset_s3_data.py \
	--bucket <your-raw-bucket> \
	--mode all \
	--profile <your-project-profile> \
	--dry-run
```

2. Delete **raw** data only:

```bash
python scripts/reset_s3_data.py \
	--bucket <your-raw-bucket> \
	--mode raw \
	--profile <your-project-profile> \
	--yes
```

3. Delete Auto Loader **state/checkpoints** only:

```bash
python scripts/reset_s3_data.py \
	--bucket <your-raw-bucket> \
	--mode state \
	--profile <your-project-profile> \
	--yes
```

4. Delete both raw + state:

```bash
python scripts/reset_s3_data.py \
	--bucket <your-raw-bucket> \
	--mode all \
	--profile <your-project-profile> \
	--yes
```

Defaults used by the script:
- raw prefix: `raw/olist`
- state prefix: `state/autoloader/olist`

### B) Reset Databricks Bronze/Silver/Gold layers

SQL file: `databricks/sql/02_reset_layers.sql`

1. Open the SQL file in Databricks SQL Editor.
2. Set `target_catalog` at the top of the SQL file (`dev` or `prod`).
3. Run statements top to bottom.

This script will:
- drop `<target_catalog>.bronze`, `<target_catalog>.silver`, `<target_catalog>.gold` with `CASCADE`,
- recreate empty schemas with the same names.

### C) Recommended full clean rerun sequence

1. Reset S3 state only (for incremental stream replay tests), or reset all (for full restart).
2. Reset Databricks layer schemas for the target catalog.
3. Re-upload raw files if raw data was deleted (`scripts/upload_to_s3.py`).
4. Run Bronze ingestion notebook/workflow.
5. Run quality checks + dbt Silver/Gold.

## 15) Phase 5: Run SCD Type 2 Prototype

This project now includes a dbt snapshot-based SCD Type 2 prototype for:

- `dim_customers_scd2`
- `dim_products_scd2`

The implementation keeps the existing Silver dimensions unchanged and adds new
snapshot-backed models for historical tracking.

### A) Files added for this feature

- `dbt/snapshots/snap_dim_customers_scd2.sql`
- `dbt/snapshots/snap_dim_products_scd2.sql`
- `dbt/models/silver/dim_customers_scd2.sql`
- `dbt/models/silver/dim_products_scd2.sql`
- `dbt/models/silver/dim_scd2.yml`

Databricks MERGE version (additional):

- `dbt/macros/scd2_merge_hooks.sql`
- `dbt/models/silver/dim_customers_scd2_merge.sql`
- `dbt/models/silver/dim_products_scd2_merge.sql`

### B) How to run it

From the `dbt/` directory:

1. Validate connection:

```bash
dbt debug --profiles-dir .
```

2. Run the snapshots:

```bash
dbt snapshot --select snap_dim_customers_scd2 snap_dim_products_scd2 --profiles-dir .
```

3. Build the Silver SCD2 models:

```bash
dbt run --select dim_customers_scd2 dim_products_scd2 --profiles-dir .
```

4. Run tests for the new models:

```bash
dbt test --select dim_customers_scd2 dim_products_scd2 --profiles-dir .
```

For production, add `--target prod` to the same commands.

### C) How to validate the history behavior

1. Run Bronze ingestion so the source Bronze tables exist.
2. Run the SCD2 snapshot commands once.
3. Change one or more customer/product attributes upstream in Bronze.
4. Run the same snapshot command again.
5. Re-run the SCD2 models.

Expected result:

- old version gets a populated `valid_to`,
- new version gets a new `valid_from`,
- only one row per business key has `is_current = true`.

### D) Example validation queries

```sql
-- Current customer versions
select *
from <target_catalog>.silver.dim_customers_scd2
where is_current = true
limit 20;

-- Full history for a customer
select *
from <target_catalog>.silver.dim_customers_scd2
where customer_id = '<customer_id>'
order by valid_from;

-- Full history for a product
select *
from <target_catalog>.silver.dim_products_scd2
where product_id = '<product_id>'
order by valid_from;
```

### E) Design choice

This implementation uses **dbt snapshots** instead of a custom `MERGE` because:

- dbt snapshots are simpler to operate,
- history tracking is built in,
- it avoids breaking existing downstream models,
- it is a good prototype path before deciding whether a custom incremental `MERGE`
	design is needed later.

### F) Databricks MERGE version (optional)

The snapshot-based SCD2 path above remains unchanged.

If you want the Databricks `MERGE` implementation instead, run:

```bash
dbt run --select dim_customers_scd2_merge dim_products_scd2_merge --profiles-dir .
dbt test --select dim_customers_scd2_merge dim_products_scd2_merge --profiles-dir .
```

For production:

```bash
dbt run --target prod --select dim_customers_scd2_merge dim_products_scd2_merge --profiles-dir .
dbt test --target prod --select dim_customers_scd2_merge dim_products_scd2_merge --profiles-dir .
```

Behavior summary:

- If business attributes changed, old row is closed (`valid_to` set, `is_current=false`).
- New version row is inserted with `is_current=true`.
- If no attributes changed, no new row is inserted.
