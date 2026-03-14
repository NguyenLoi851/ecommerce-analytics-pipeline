# Phase 1 Runbook — Ingestion to Bronze

This is the executable guide for Phase 1: getting all Olist CSVs from Kaggle into Bronze Delta tables in Databricks.

## Prerequisites

- Phase 0 complete: Unity Catalog, schemas, S3 external locations, and Databricks Repos all configured.
- `dev.bronze`, `dev.silver`, `dev.gold` schemas exist in Databricks.
- S3 raw bucket accessible from Databricks (external location validated).
- Kaggle account and API key (`~/.kaggle/kaggle.json`).
- Python 3.8+ with `kaggle` and `boto3` installed locally.

---

## Step 1 — Download Dataset and Upload CSVs to S3

Download the Olist dataset from Kaggle and upload all CSVs to the S3 raw path.

### 1a. Install dependencies (local machine)

```bash
# From project root — installs kaggle, boto3, and dbt-databricks
pip install -r requirements.txt
```

### 1b. Set Kaggle credentials

```bash
mkdir -p ~/.kaggle
# place your API key at ~/.kaggle/kaggle.json
# it should look like: {"username":"<your-user>","key":"<your-api-key>"}
chmod 600 ~/.kaggle/kaggle.json
```

### 1c. Run upload script

If your default AWS profile is not for this project, configure and verify a named profile first:

```bash
aws configure --profile <your-project-profile>
aws sts get-caller-identity --profile <your-project-profile>
```

```bash
python scripts/upload_to_s3.py \
  --bucket <your-raw-bucket> \
  --prefix raw/olist \
  --region us-east-1 \
  --profile <your-project-profile>
```

This script will:
1. Use the Kaggle API to download the dataset zip.
2. Extract all CSVs.
3. Upload each CSV to `s3://<your-raw-bucket>/raw/olist/<filename>`.

Expected files in S3 after upload:

```
s3://<your-raw-bucket>/raw/olist/
  olist_customers_dataset.csv
  olist_geolocation_dataset.csv
  olist_order_items_dataset.csv
  olist_order_payments_dataset.csv
  olist_order_reviews_dataset.csv
  olist_orders_dataset.csv
  olist_products_dataset.csv
  olist_sellers_dataset.csv
  product_category_name_translation.csv
```

---

## Step 2 — Set Up dbt Project

Do this only after raw data is in S3.

### 2a. Bootstrap dbt project

This project pins **Python 3.11** in `.python-version`. If you use pyenv or mise it will activate automatically; otherwise install 3.11 manually.

```bash
# From project root
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

> `dbt init` is **not needed** — the scaffold already exists in `dbt/`.

### 2b. Configure profiles.yml

Copy the example and fill in your values:

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

Edit `dbt/profiles.yml`:
- `host`: your Databricks workspace URL (e.g., `adb-<id>.azuredatabricks.net`)
- `http_path`: SQL Warehouse HTTP path from Databricks UI -> SQL Warehouses -> Connection Details
- `token`: Databricks personal access token
- `catalog`: `dev`
- `schema`: `bronze` (will be overridden per model)

### 2c. Validate connection

```bash
# Activate the venv first if not already active
source venv/bin/activate

cd dbt
dbt debug --profiles-dir .
```

Expected: all checks pass (connection, warehouse reachable, catalog/schema found).

---

## Step 3 — Run Bronze Ingestion Notebook

Choose **one** ingestion notebook:

- Standard ingestion: [databricks/notebooks/01_bronze_ingestion.py](../databricks/notebooks/01_bronze_ingestion.py)
- Auto Loader ingestion: [databricks/notebooks/01_bronze_ingestion_autoloader.py](../databricks/notebooks/01_bronze_ingestion_autoloader.py)

If you choose the Auto Loader notebook:
- Incremental tables are discovered with `cloudFiles` and skipped on re-runs via checkpoint state.
- `geolocation` and `product_category_name_translation` are still full-overwrite loads.
- Optional widget parameters:
  - `FORCE_RELOAD=true` resets Auto Loader state and reloads incremental tables.
  - `AUTOLOADER_STATE_BASE` sets where schema/checkpoint metadata is stored.

Before running, set the widget defaults or update the constants at the top:
- `RAW_BUCKET`: your S3 raw bucket name
- `RAW_PREFIX`: `raw/olist`
- `CATALOG`: `dev`
- `BRONZE_SCHEMA`: `bronze`

The notebook will:
1. Read each CSV from S3 using schema inference with defined overrides.
2. Apply robust type casting and rename columns where needed.
3. Add metadata columns (`_ingest_ts`, `_source_file`, `_batch_id`).
4. Write each table as a Delta table under `dev.bronze.*`.

Expected Bronze tables after successful run:

| Table | Source File |
|---|---|
| `dev.bronze.orders` | olist_orders_dataset.csv |
| `dev.bronze.order_items` | olist_order_items_dataset.csv |
| `dev.bronze.order_payments` | olist_order_payments_dataset.csv |
| `dev.bronze.order_reviews` | olist_order_reviews_dataset.csv |
| `dev.bronze.customers` | olist_customers_dataset.csv |
| `dev.bronze.sellers` | olist_sellers_dataset.csv |
| `dev.bronze.products` | olist_products_dataset.csv |
| `dev.bronze.geolocation` | olist_geolocation_dataset.csv |
| `dev.bronze.product_category_name_translation` | product_category_name_translation.csv |

---

## Step 4 — Run Quality Checks Notebook

Open and run [databricks/notebooks/02_bronze_quality_checks.py](../databricks/notebooks/02_bronze_quality_checks.py).

Checks performed per table:
- **Row count**: asserts non-zero, prints count.
- **Null check**: asserts no nulls on primary key columns.
- **Duplicate check**: asserts no duplicate primary keys.

All failures raise exceptions and are printed to the notebook output. Fix upstream data or ingestion logic before proceeding.

---

## Step 5 — Schedule with Databricks Workflow

Create the Phase 1 workflow using the JSON spec in [databricks/workflows/phase1_workflow.json](../databricks/workflows/phase1_workflow.json).

### Import via UI

1. Databricks UI -> Workflows -> Create Job.
2. Add tasks matching `phase1_workflow.json` (or an Auto Loader variant if you created one):
  - Task 1: `notebook_bronze_ingestion` — runs `01_bronze_ingestion` **or** `01_bronze_ingestion_autoloader`
   - Task 2: `notebook_bronze_quality_checks` — depends on Task 1, runs `02_bronze_quality_checks`
3. If using Auto Loader, include notebook parameters for Task 1:
  - `FORCE_RELOAD`: `false` (default)
  - `AUTOLOADER_STATE_BASE`: e.g. `dbfs:/pipelines/olist/autoloader_state/dev`
4. Set schedule (e.g., daily at 02:00 UTC) or leave as on-demand for now.

---

## Step 6 — Definition of Done for Phase 1

- [ ] All 9 CSV files uploaded to `s3://<your-raw-bucket>/raw/olist/`.
- [ ] All 9 Bronze Delta tables exist in `dev.bronze.*`.
- [ ] Each table has `_ingest_ts`, `_source_file`, `_batch_id` metadata columns.
- [ ] Quality checks notebook passes with zero failures.
- [ ] `dbt debug` passes against the dev SQL Warehouse.
- [ ] Databricks Workflow for Phase 1 created (can be triggered manually).

---

## Troubleshooting

| Issue | Fix |
|---|---|
| `FileNotFoundError` from kaggle CLI | Check `~/.kaggle/kaggle.json` permissions and content. |
| S3 upload fails | Verify credentials with `aws sts get-caller-identity [--profile <name>]`, then re-run script with `--profile <name>`. |
| Databricks cannot read S3 | Re-run `01_external_location_s3.sql`; verify IAM role trust policy. |
| Schema mismatch on Delta write | Drop table and re-run ingestion, or use `.option("overwriteSchema", "true")`. |
| `dbt debug` fails | Verify `http_path`, `host`, and personal access token in `profiles.yml`. |
| `python3.11: command not found` | Install via Homebrew: `brew install python@3.11`, then run `python3.11 -m venv venv`. |
| `mashumaro` / `JSONObjectSchema` import error | Python 3.14 is not supported by dbt. Recreate the venv with Python 3.11 (see above row). |
| Row count check fails | Check S3 upload — file may be empty or truncated. |
