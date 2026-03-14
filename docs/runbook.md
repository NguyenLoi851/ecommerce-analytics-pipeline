# Operational Runbook

Step-by-step instructions to set up and run the ecommerce analytics pipeline from scratch.

---

## Prerequisites

- AWS account with IAM permissions for S3 and IAM role management.
- Databricks workspace on AWS with Unity Catalog enabled (workspace admin access).
- GitHub account and repository access.
- Python 3.11 installed locally (`brew install python@3.11` on macOS).
- Kaggle account and API key in `~/.kaggle/kaggle.json`.

---

## 1. Infrastructure Setup

### 1a. Configure AWS IAM user and local CLI profile

Before running Terraform, create an AWS user/profile that has the permissions needed to create and manage the project infrastructure.

1. In AWS Console, go to **IAM → Users**.
2. Create a new user for this project, for example `ecommerce-pipeline-terraform`.
3. Grant the user programmatic access by creating an access key.
4. Attach permissions that allow Terraform to manage the required resources. For a quick start, assign broad access that includes S3 and IAM management for this project.
   - Minimum practical access for this project should include S3 and IAM permissions.
   - If you are working in a sandbox account, you can temporarily attach broader admin-style access and tighten it later.
5. Save the **Access key ID** and **Secret access key**.

Set up the AWS CLI locally:

```bash
aws configure --profile <your-project-profile>
```

Provide these values when prompted:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name, for example `us-east-1`
- Default output format, for example `json`

Validate the profile before running Terraform:

```bash
aws sts get-caller-identity --profile <your-project-profile>
```

Use this profile for subsequent AWS and Terraform commands in this runbook.

### 1b. Create S3 bucket (Terraform)

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: aws_region, raw_bucket_name
terraform init
terraform plan
terraform apply
```

### 1c. Set up Unity Catalog and schemas

Run in Databricks SQL Editor:
1. `databricks/sql/00_unity_catalog_setup.sql` — creates `dev` and `prod` catalogs with `bronze`, `silver`, `gold` schemas.

### 1d. Configure S3 access from Databricks

1. In Databricks Account Console → **Data → Credentials**, start creating an AWS IAM role-based storage credential.
2. Copy the trust policy values Databricks shows you.
3. In AWS IAM, create a role and attach the trust policy + an S3 access policy (see [architecture.md](architecture.md) for the policy template).
4. Edit `databricks/sql/01_external_location_s3.sql` — replace `<databricks-s3-access-role-arn>` and `<your-raw-bucket>`.
5. Run the SQL file in Databricks SQL Editor.

### 1e. Connect Databricks Repos to GitHub

In Databricks UI: **User Settings → Linked accounts → GitHub** → connect via PAT or GitHub App.
Then: **Repos → Add Repo** → paste this repository URL.

### 1f. Validate infrastructure

Run `databricks/notebooks/00_phase0_smoke_test.py` in Databricks. Expected result: table `dev.bronze.phase0_smoke_test` created successfully.

---

## 2. Upload Source Data

Download the [Olist dataset from Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and upload to S3.

```bash
# From project root — activate virtual environment first
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure your AWS profile if needed
aws configure --profile <your-project-profile>
aws sts get-caller-identity --profile <your-project-profile>

# Upload CSVs to S3
python scripts/upload_to_s3.py \
  --bucket <your-raw-bucket> \
  --prefix raw/olist \
  --region us-east-1 \
  --profile <your-project-profile>
```

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

## 3. Configure dbt

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

Edit `dbt/profiles.yml`:
- `host`: Databricks workspace URL (e.g. `adb-<id>.7.azuredatabricks.net`)
- `http_path`: SQL Warehouse HTTP path (from **SQL Warehouses → Connection details**)
- `token`: Databricks personal access token
- `catalog`: `dev`

Validate the connection:

```bash
cd dbt
dbt debug --profiles-dir .
```

All checks must pass before proceeding.

---

## 4. Run Bronze Ingestion

Open and run one of these notebooks in Databricks:

| Notebook | When to use |
|---|---|
| `databricks/notebooks/01_bronze_ingestion.py` | Standard full-load |
| `databricks/notebooks/01_bronze_ingestion_autoloader.py` | Incremental/Auto Loader (preferred for repeated runs) |

Set these parameters at the top of the notebook before running:
- `RAW_BUCKET`: your S3 raw bucket name
- `RAW_PREFIX`: `raw/olist`
- `CATALOG`: `dev`
- `BRONZE_SCHEMA`: `bronze`

The Auto Loader notebook adds incremental tracking — subsequent runs skip already-loaded tables automatically. Use `FORCE_RELOAD=true` to bypass the registry and reload everything.

After ingestion, run the quality checks notebook:
- `databricks/notebooks/02_bronze_quality_checks.py`

Expected Bronze tables: `dev.bronze.orders`, `order_items`, `order_payments`, `order_reviews`, `customers`, `sellers`, `products`, `geolocation`, `product_category_name_translation`.

---

## 5. Run dbt Transformations

Build and test the Silver and Gold layers:

```bash
cd dbt

# Install dbt packages
dbt deps --profiles-dir .

# Build Silver (dimensions and facts)
dbt run --select silver --profiles-dir .
dbt test --select silver --profiles-dir .

# Build Gold (analytics marts)
dbt run --select gold --profiles-dir .
dbt test --select gold --profiles-dir .

# Or build everything at once
dbt run --select staging silver gold --profiles-dir .
dbt test --select staging silver gold --profiles-dir .
```

### Verify in Databricks SQL

```sql
-- Confirm Silver tables exist
SHOW TABLES IN dev.silver;

-- Quick row-count check
SELECT 'dim_customers'   AS model, COUNT(*) AS rows FROM dev.silver.dim_customers
UNION ALL SELECT 'fct_orders',      COUNT(*) FROM dev.silver.fct_orders
UNION ALL SELECT 'fct_order_items', COUNT(*) FROM dev.silver.fct_order_items
UNION ALL SELECT 'fct_payments',    COUNT(*) FROM dev.silver.fct_payments;
```

---

## 6. Set Up the Production Workflow

### 6a. Configure GitHub Secrets

Add these under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `DATABRICKS_HOST` | Dev workspace host |
| `DATABRICKS_HTTP_PATH` | Dev SQL Warehouse HTTP path |
| `DATABRICKS_TOKEN` | Dev Personal Access Token |
| `DATABRICKS_HOST_PROD` | Prod workspace host |
| `DATABRICKS_HTTP_PATH_PROD` | Prod SQL Warehouse HTTP path |
| `DATABRICKS_TOKEN_PROD` | Prod PAT or service-principal token |

### 6b. Create the `production` GitHub Environment

**Settings → Environments → New environment** → name it `production` → add required reviewers.

### 6c. Enable branch protection on `main`

- Require PR before merging.
- Require status checks: `dbt-validate`, `dbt-test`.
- Require at least 1 approving review.

### 6d. Create the Databricks production workflow

1. Copy and fill in your values:
   ```bash
   cp databricks/workflows/phase4_workflow.json.example databricks/workflows/phase4_workflow.json
   ```
   Replace all `<placeholder>` values: Databricks user, raw bucket, SQL warehouse ID, email addresses, Slack webhook ID, GitHub username.

2. Configure Databricks CLI authentication:
  ```bash
  databricks auth login --host https://<your-databricks-host>
  ```

3. Create the workflow from JSON and capture the `job_id`:
  ```bash
  databricks jobs create --json @databricks/workflows/phase4_workflow.json
  ```

4. Run manually first and confirm all tasks succeed:
  ```bash
  databricks jobs run-now --job-id <job-id>
  ```

### 6e. Enable daily schedule

Enable or update the schedule via CLI (daily at 03:00 UTC):

```bash
databricks jobs reset --job-id <job-id> --json @databricks/workflows/phase4_workflow.json
```

If needed, inspect details (including `schedule` and `pause_status`):

```bash
databricks jobs get --job-id <job-id>
```

---

## 7. Post-Deploy Verification

Run in Databricks SQL after a successful production run:

```sql
-- Gold mart freshness
SELECT MAX(order_date) AS latest_date FROM prod.gold.mart_sales_daily;

-- Row counts
SELECT 'mart_sales_daily'          AS model, COUNT(*) AS rows FROM prod.gold.mart_sales_daily
UNION ALL SELECT 'mart_category_performance', COUNT(*) FROM prod.gold.mart_category_performance
UNION ALL SELECT 'mart_customer_cohorts',     COUNT(*) FROM prod.gold.mart_customer_cohorts
UNION ALL SELECT 'mart_delivery_sla',         COUNT(*) FROM prod.gold.mart_delivery_sla;

-- Payment mix totals ~1.0
SELECT order_date,
       ROUND(payment_mix_credit_card_pct + payment_mix_boleto_pct +
             payment_mix_voucher_pct + payment_mix_debit_card_pct +
             payment_mix_not_defined_pct, 4) AS mix_total
FROM prod.gold.mart_sales_daily
ORDER BY order_date DESC
LIMIT 10;
```

---

## 8. Backfill

To re-process a historical date range:

```bash
databricks jobs run-now \
  --job-id <job-id> \
  --job-parameters '{"backfill_start_date": "2017-01-01", "backfill_end_date": "2018-12-31"}'
```

For a full dbt refresh:
```bash
dbt run --full-refresh --select silver gold --profiles-dir dbt --target prod
```

---

## 9. Reset (Run from Scratch)

### Reset S3 data

```bash
# Dry-run first
python scripts/reset_s3_data.py --bucket <your-raw-bucket> --mode all --profile <profile> --dry-run

# Delete raw data only
python scripts/reset_s3_data.py --bucket <your-raw-bucket> --mode raw --profile <profile> --yes

# Delete Auto Loader state only
python scripts/reset_s3_data.py --bucket <your-raw-bucket> --mode state --profile <profile> --yes

# Delete both
python scripts/reset_s3_data.py --bucket <your-raw-bucket> --mode all --profile <profile> --yes
```

### Reset Databricks layers

Run `databricks/sql/02_reset_layers.sql` in Databricks SQL Editor (set `target_catalog` at the top). This drops and recreates `bronze`, `silver`, `gold` schemas.

### Full clean rerun sequence

1. Reset S3 state (or all for full restart).
2. Reset Databricks layer schemas.
3. Re-upload raw files if raw data was deleted.
4. Run Bronze ingestion notebook.
5. Run quality checks.
6. Run `dbt run` and `dbt test` for Silver + Gold.

---

## 10. SCD Type 2 (Optional)

Snapshot-backed SCD2 models are available for `dim_customers` and `dim_products`.

```bash
cd dbt

# Run snapshots
dbt snapshot --select snap_dim_customers_scd2 snap_dim_products_scd2 --profiles-dir .

# Build SCD2 Silver models
dbt run --select dim_customers_scd2 dim_products_scd2 --profiles-dir .

# Test
dbt test --select dim_customers_scd2 dim_products_scd2 --profiles-dir .
```

For the Databricks MERGE variant:
```bash
dbt run --select dim_customers_scd2_merge dim_products_scd2_merge --profiles-dir .
dbt test --select dim_customers_scd2_merge dim_products_scd2_merge --profiles-dir .
```

See [adr/003-scd2-snapshot-vs-merge.md](adr/003-scd2-snapshot-vs-merge.md) for why the snapshot approach was chosen.
