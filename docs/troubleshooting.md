# Troubleshooting Guide

Common failures and how to resolve them.

---

## Local Setup

| Symptom | Cause | Fix |
|---|---|---|
| `python3.11: command not found` | Python 3.11 not installed | `brew install python@3.11` |
| `mashumaro` / `JSONObjectSchema` import error | Python 3.14+ not supported by dbt | Recreate venv: `python3.11 -m venv venv && source venv/bin/activate && pip install -r requirements.txt` |
| `FileNotFoundError` from kaggle CLI | Missing or wrong `kaggle.json` | Check `~/.kaggle/kaggle.json` exists and has `chmod 600` |
| S3 upload fails | Wrong or missing AWS credentials | `aws sts get-caller-identity --profile <name>`, then re-run upload with `--profile <name>` |

---

## dbt Connection

| Symptom | Cause | Fix |
|---|---|---|
| `dbt debug` fails — connection error | Wrong `host`, `http_path`, or expired token | Re-check `dbt/profiles.yml` values against Databricks SQL Warehouse → Connection details |
| `dbt debug` fails — catalog not found | Catalog or schema does not exist | Run `databricks/sql/00_unity_catalog_setup.sql` in Databricks SQL Editor |
| `dbt run` fails — `Table not found` in staging | Bronze tables not yet created | Run Bronze ingestion notebook first |
| `dbt test` fails — `not_null` or `unique` violation | Upstream data quality issue | Check Bronze source; review ingestion logs and quality checks notebook output |

---

## Bronze Ingestion

| Symptom | Cause | Fix |
|---|---|---|
| Databricks cannot read S3 | External location not configured or IAM role trust policy mismatch | Re-run `01_external_location_s3.sql`; verify IAM role trust policy in AWS console |
| Schema mismatch on Delta write | Table already exists with different schema | Drop table with `DROP TABLE dev.bronze.<table>` and re-run ingestion, or add `.option("overwriteSchema", "true")` |
| Row count check fails in quality notebook | File empty or truncated in S3 | Re-upload specific CSV using `upload_to_s3.py` |
| Auto Loader stuck / not picking up new files | Stale checkpoint state | Run ingestion notebook with `FORCE_RELOAD=true` or delete state with reset script: `--mode state` |
| Incremental table skipped unexpectedly | `_ingestion_registry` shows file already loaded | Inspect registry: `SELECT * FROM dev.bronze._ingestion_registry`. Use `FORCE_RELOAD=true` to bypass |

---

## Databricks Workflows

### Scenario — Bronze ingestion task fails (S3 access error)

1. Check IAM role trust policy and S3 bucket policy are still valid.
2. Confirm external location is intact: `SHOW EXTERNAL LOCATIONS;` in Databricks SQL.
3. Re-run the failed task only: **Workflows → run → Repair run**.

### Scenario — dbt Silver test fails

1. Open task logs to identify the failing test and column.
2. Check Bronze source table for nulls or duplicates.
3. If it's a transient upstream issue: repair and re-run from `dbt_run_silver`.
4. If it's a code bug: fix on a feature branch → PR → CI → prod deploy.

### Scenario — Pipeline exceeds SLA (health alert fires after 2 hours)

1. Identify the slow task in the Workflows UI timeline.
2. Check cluster metrics (shuffle spill, GC pressure).
3. For Bronze: increase cluster size in the workflow JSON.
4. For dbt: run `OPTIMIZE` on slow Delta tables.
5. Adjust the SLA threshold in the workflow JSON `health.rules` block if the limit is too aggressive.

### Scenario — Gold mart shows stale data

1. Check run history — did the scheduled run fire?
2. If the SQL Warehouse was stopped, resume it and trigger the workflow manually.
3. Confirm Databricks Workflow schedule is **Unpaused** in the UI.

### Scenario — `dbt-promote-prod` CI job never triggers

1. Check that the PR was merged to `main` (not a different branch).
2. Verify the `production` GitHub Environment has required reviewers configured.
3. Look for the pending approval in **Actions → dbt-promote-prod → Review deployments**.

---

## Terraform / AWS

| Symptom | Cause | Fix |
|---|---|---|
| `terraform apply` fails — AccessDenied | Local IAM user lacks S3 or IAM permissions | Attach `AmazonS3FullAccess` and `IAMFullAccess` to the IAM user running Terraform |
| Bucket already exists error | Bucket name collides with another AWS account's bucket | Update `raw_bucket_name` in `terraform.tfvars` to a unique name |
| `terraform plan` shows unexpected destroy | `terraform.tfstate` out of sync | Run `terraform refresh` before `plan` |

---

## dbt Docs / Freshness

| Symptom | Cause | Fix |
|---|---|---|
| `dbt docs serve` port already in use | Another process on port 8080 | Use `dbt docs serve --port 8090 --profiles-dir .` |
| Source freshness warnings in dbt docs | `dbt source freshness` not run | Run `dbt source freshness --profiles-dir .` |
