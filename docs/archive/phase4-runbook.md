# Phase 4 Runbook — Productionization

This is the executable guide for Phase 4: wiring the full pipeline into a scheduled, observable, production-grade system.

## Prerequisites

- Phase 3 complete: all Gold marts built and passing dbt tests in `dev`.
- `dbt/profiles.yml` configured with both `dev` and `prod` targets.
- GitHub repository connected to Databricks Repos (SSH or PAT).
- A Databricks SQL Warehouse exists in the **prod** workspace.
- GitHub Actions has access to the repository (Actions enabled).

---

## Step 1 — Configure GitHub Secrets

Add these secrets under **Settings → Secrets and variables → Actions → New repository secret**:

| Secret                      | Value                                         |
|-----------------------------|-----------------------------------------------|
| `DATABRICKS_HOST`           | Dev workspace host, e.g. `adb-123.7.azuredatabricks.net` |
| `DATABRICKS_HTTP_PATH`      | Dev SQL Warehouse HTTP path                   |
| `DATABRICKS_TOKEN`          | Dev Personal Access Token                     |
| `DATABRICKS_HOST_PROD`      | Prod workspace host                           |
| `DATABRICKS_HTTP_PATH_PROD` | Prod SQL Warehouse HTTP path                  |
| `DATABRICKS_TOKEN_PROD`     | Prod Personal Access Token (or Service Principal) |

---

## Step 2 — Set Up GitHub Environment Approval Gate

1. Go to **Settings → Environments → New environment** and name it `production`.
2. Under **Deployment protection rules**, enable **Required reviewers**.
3. Add yourself (and teammates) as required reviewers.
4. Save. This ensures no code reaches prod without a human sign-off.

Also set **Branch protection** on `main`:
```
Settings → Branches → Add rule → Branch name pattern: main
  ☑ Require a pull request before merging
  ☑ Require status checks to pass:  dbt-validate, dbt-test (dev target)
  ☑ Require at least 1 approving review
  ☑ Do not allow bypassing the above settings
```

---

## Step 3 — Validate the CI Pipeline

Push a small no-op change (e.g. a comment in a `.sql` file) on a feature branch:

```bash
git checkout -b feature/phase4-ci-validation
# make a trivial edit
git commit -am "ci: validate phase 4 ci pipeline"
git push origin feature/phase4-ci-validation
```

Open a PR and confirm:
- `dbt-validate` (parse) passes ✅
- `dbt-test` (dev) passes ✅

Merge, then confirm:
- `dbt-promote-prod` appears in the **Actions** tab, waiting for approval.
- Approve → confirm prod dbt run + test succeeds.

---

## Step 4 — Create the Production Databricks Workflow

1. Copy the example file and fill in your values:
   ```bash
   cp databricks/workflows/phase4_workflow.json.example databricks/workflows/phase4_workflow.json
   ```
2. Replace all `<placeholder>` values in `phase4_workflow.json`:

   | Placeholder                    | Replace with                                 |
   |--------------------------------|----------------------------------------------|
   | `<your-databricks-user>`       | Your Databricks workspace user email         |
   | `<your-raw-bucket>`            | Your S3 raw bucket name                      |
   | `<your-sql-warehouse-id>`      | Your SQL Warehouse ID (from Warehouse details) |
   | `<your-team-email>`            | Team distribution email                      |
   | `<your-oncall-email>`          | On-call engineer email                       |
   | `<your-slack-webhook-id>`      | Databricks webhook ID for Slack alerting     |
   | `<your-github-username>`       | Your GitHub username or org                  |

### Option A — Import via JSON (recommended)

3. In the Databricks UI, go to **Jobs & Pipelines → Create job**.
4. Click the **⋮** menu → **Edit JSON**.
5. Paste the contents of your filled-in `phase4_workflow.json`.
6. Click **Create**.

### Option B — Using the Databricks CLI

```bash
# Install all dependencies (includes databricks-cli)
pip install -r requirements.txt

# Upgrade to Jobs API 2.1 (required for MULTI_TASK workflows and dbt_task)
databricks jobs configure --version=2.1

databricks configure --token   # enter host + token

# Create workflow from JSON
databricks jobs create --json-file databricks/workflows/phase4_workflow.json
```

> **Note — `warehouse_id`**: use only the bare ID (e.g. `2eea9b6d8e8e61f0`), not the full HTTP path `/sql/1.0/warehouses/...`. Find it in **SQL Warehouses → your warehouse → Connection details → ID**.
>
> **Note — serverless compute**: if your workspace enforces serverless-only compute, the `job_clusters` block is not supported. The workflow JSON is already configured without it — notebook tasks run on serverless automatically, and `dbt_task` uses the SQL Warehouse directly.

---

## Step 5 — Configure Alerting

### Email alerts (already in workflow JSON)
The workflow JSON includes `email_notifications` for `on_failure` and `on_success` — just replace the email placeholders.

### Slack alerts via Databricks webhooks
1. In Databricks UI: **Settings → Notifications → New notification destination**.
2. Choose **Slack** and provide the incoming webhook URL.
3. Copy the webhook ID and replace `<your-slack-webhook-id>` in the workflow JSON.

### dbt test failure alerting
dbt test failures propagate as Databricks task failures, which trigger the workflow's `on_failure` notification automatically — no additional configuration needed.

### SLA delay alerting
The workflow `health` rule in `phase4_workflow.json` fires when the run exceeds **2 hours** (7200 seconds). Adjust the threshold under the `health.rules` block.

For finer-grained SLA alerts (e.g., gold mart row counts), add a Databricks SQL Alert:
1. **SQL → Alerts → New Alert**.
2. Query: `SELECT COUNT(*) AS cnt FROM prod.gold.mart_sales_daily WHERE order_date = CURRENT_DATE - 1`.
3. Condition: `cnt < 1` (alert if today's data is missing).
4. Add notification destination.

---

## Step 6 — Test the Full Production Run

Trigger the workflow manually for the first time before enabling the schedule:

```bash
# Via Databricks CLI
databricks jobs run-now --job-id <job-id>

# Or via Databricks UI: Workflows → olist-production-pipeline → Run now
```

Expected task sequence and approximate durations:

| Task                  | Expected duration |
|-----------------------|-------------------|
| `bronze_ingestion`    | 1-2 min         |
| `bronze_quality_checks` | 1 min           |
| `dbt_run_silver`      | 1-2 min         |
| `dbt_test_silver`     | 2-5 min          |
| `dbt_run_gold`        | 1–2 min          |
| `dbt_test_gold`       | 1–2 min           |
| `publish_gold`        | 1–2 min           |
| **Total**             | **~8–16 min**    |

Confirm all tasks **Succeeded** before enabling the schedule.

---

## Step 7 — Enable the Schedule

1. In Databricks Workflows, open `olist-production-pipeline`.
2. Click **Schedule** → set to **Daily at 03:00 UTC** (or your preferred time).
3. Set **Pause status** to **Unpaused**.
4. Save.

The pipeline will now run automatically every day.

---

## Step 8 — Backfill Strategy

Use the `backfill_start_date` and `backfill_end_date` workflow parameters to re-process historical data:

```bash
# Backfill via Databricks CLI
databricks jobs run-now \
  --job-id <job-id> \
  --job-parameters '{
    "backfill_start_date": "2017-01-01",
    "backfill_end_date": "2018-12-31"
  }'
```

Backfill behaviour per layer:

| Layer   | Strategy                                                                 |
|---------|--------------------------------------------------------------------------|
| Bronze  | The ingestion notebook re-reads from S3. With `MERGE` / `COPY INTO`, re-running is idempotent — existing rows are not duplicated. |
| Silver  | dbt models are `table` materialization by default — a full `dbt run` replaces the table. Safe to re-run without date filtering. |
| Gold    | Same as Silver — full table replacement on each run.                     |

For **incremental** Silver/Gold models (future enhancement):
```bash
# Full refresh override
dbt run --full-refresh --select silver gold --profiles-dir dbt --target prod
```

---

## Step 9 — Post-Deploy Verification Checklist

Run these checks in Databricks SQL after a production pipeline run:

```sql
-- 1. Gold mart freshness: yesterday's data must be present
SELECT MAX(order_date) AS latest_date FROM prod.gold.mart_sales_daily;
-- Expected: previous calendar day

-- 2. Row-count sanity
SELECT 'mart_sales_daily'         AS model, COUNT(*) AS rows FROM prod.gold.mart_sales_daily
UNION ALL
SELECT 'mart_category_performance',          COUNT(*) FROM prod.gold.mart_category_performance
UNION ALL
SELECT 'mart_customer_cohorts',              COUNT(*) FROM prod.gold.mart_customer_cohorts
UNION ALL
SELECT 'mart_delivery_sla',                  COUNT(*) FROM prod.gold.mart_delivery_sla;

-- 3. Payment mix totals ~1.0
SELECT order_date,
       ROUND(
         payment_mix_credit_card_pct + payment_mix_boleto_pct +
         payment_mix_voucher_pct + payment_mix_debit_card_pct +
         payment_mix_not_defined_pct, 4) AS total
FROM prod.gold.mart_sales_daily
ORDER BY order_date DESC
LIMIT 10;

-- 4. Delivery SLA completeness
SELECT order_date, on_time_delivery_rate
FROM prod.gold.mart_delivery_sla
ORDER BY order_date DESC
LIMIT 10;
```

---

## Operational Runbook — Failure Scenarios

### Scenario 1 — Bronze ingestion fails

**Symptom**: `bronze_ingestion` task fails with S3 access error.

**Resolution**:
1. Check IAM role trust policy and S3 bucket policy are still in place.
2. Confirm the external location is still valid: `SHOW EXTERNAL LOCATIONS;`.
3. Re-run the task manually: Workflows → task → **Repair run**.

---

### Scenario 2 — dbt test failure in Silver

**Symptom**: `dbt_test_silver` fails with `not_null` or `unique` violation.

**Resolution**:
1. Open the Databricks task logs to find the failing test model and column.
2. Check Bronze source for upstream data quality issues.
3. If it's a transient upstream issue, repair and re-run from `dbt_run_silver`.
4. If it's a code bug, fix on a feature branch → PR → CI → prod deploy.

---

### Scenario 3 — Pipeline exceeds SLA (> 2 hours)

**Symptom**: Health alert fires; run still in progress after 2 hours.

**Resolution**:
1. Identify the slow task in the Workflows UI timeline.
2. Check cluster metrics (shuffle spill, GC pressure).
3. For Bronze: increase cluster size in the JSON and re-deploy.
4. For dbt: run `OPTIMIZE` on slow Delta tables.

---

### Scenario 4 — Gold mart shows stale data

**Symptom**: `MAX(order_date)` in a Gold mart is > 1 day behind.

**Resolution**:
1. Check if the scheduled run fired (look at run history).
2. If run was skipped (warehouse was stopped), resume the warehouse and re-trigger manually.
3. Confirm Databricks Workflow schedule is **Unpaused**.
