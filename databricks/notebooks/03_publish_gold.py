# Databricks notebook source
# Purpose: Post-dbt publish step — optimize Gold Delta tables and validate freshness.
#
# This notebook runs as the final task in the production pipeline after dbt tests pass.
# It:
#   1. Runs OPTIMIZE + ZORDER on each Gold table for query performance.
#   2. Runs ANALYZE to update table statistics for the SQL Warehouse query planner.
#   3. Validates data freshness and row counts — fails loudly if data is stale.
#   4. Logs a pipeline completion summary.
#
# Parameters (set via Databricks Workflow job parameters):
#   TARGET_CATALOG   : Unity Catalog to publish to (default: prod)
#   OPTIMIZE_TABLES  : Whether to run OPTIMIZE (default: true, set false to skip for speed)

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

import json
from datetime import datetime, timezone

dbutils.widgets.text("TARGET_CATALOG", "prod", "Target Unity Catalog")
dbutils.widgets.text("OPTIMIZE_TABLES", "true", "Run OPTIMIZE on Gold tables")
dbutils.widgets.text("MIN_ACCEPTED_DATE", "", "Optional minimum accepted date (YYYY-MM-DD)")

TARGET_CATALOG = dbutils.widgets.get("TARGET_CATALOG")
OPTIMIZE_TABLES = dbutils.widgets.get("OPTIMIZE_TABLES").lower() == "true"
MIN_ACCEPTED_DATE = dbutils.widgets.get("MIN_ACCEPTED_DATE").strip()
GOLD_SCHEMA = f"{TARGET_CATALOG}.gold"

print(f"TARGET_CATALOG  : {TARGET_CATALOG}")
print(f"GOLD_SCHEMA     : {GOLD_SCHEMA}")
print(f"OPTIMIZE_TABLES : {OPTIMIZE_TABLES}")
print(f"MIN_ACCEPTED_DATE : {MIN_ACCEPTED_DATE or 'not set'}")

# COMMAND ----------

# MAGIC %md ## 1 — Optimize Gold Tables

# COMMAND ----------

GOLD_TABLES = {
    "mart_sales_daily": "order_date",
    "mart_category_performance": "order_date",
    "mart_customer_cohorts": "cohort_month",
    "mart_delivery_sla": "order_date",
}

if OPTIMIZE_TABLES:
    for table, zorder_col in GOLD_TABLES.items():
        full_name = f"{GOLD_SCHEMA}.{table}"
        print(f"Optimizing {full_name} (ZORDER BY {zorder_col}) ...")
        spark.sql(f"OPTIMIZE {full_name} ZORDER BY ({zorder_col})")
        print(f"  ✓ {table} optimized")
else:
    print("Skipping OPTIMIZE (OPTIMIZE_TABLES=false)")

# COMMAND ----------

# MAGIC %md ## 2 — Refresh Table Statistics

# COMMAND ----------

for table in GOLD_TABLES:
    full_name = f"{GOLD_SCHEMA}.{table}"
    print(f"Analyzing {full_name} ...")
    spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS FOR ALL COLUMNS")
    print(f"  ✓ {table} statistics refreshed")

# COMMAND ----------

# MAGIC %md ## 3 — Freshness & Row Count Validation

# COMMAND ----------

validation_errors = []

freshness_checks = {
    "mart_sales_daily": "order_date",
    "mart_category_performance": "order_date",
    "mart_delivery_sla": "order_date",
}

if MIN_ACCEPTED_DATE:
    print(f"Validating historical freshness for date >= {MIN_ACCEPTED_DATE}")
else:
    print("Validating historical freshness (date columns must not be NULL)")

for table, date_col in freshness_checks.items():
    full_name = f"{GOLD_SCHEMA}.{table}"
    row = spark.sql(f"SELECT MAX({date_col}) AS max_date, COUNT(*) AS total_rows FROM {full_name}").collect()[0]
    max_date = str(row["max_date"]) if row["max_date"] else "NULL"
    total_rows = row["total_rows"]

    freshness_ok = max_date != "NULL"
    if freshness_ok and MIN_ACCEPTED_DATE:
        freshness_ok = max_date >= MIN_ACCEPTED_DATE

    status = "✓" if freshness_ok else "✗ INVALID"
    print(f"  [{status}] {table}: max_date={max_date}, rows={total_rows:,}")

    if not freshness_ok:
        if MIN_ACCEPTED_DATE:
            validation_errors.append(
                f"{table}: latest {date_col} is {max_date} — expected >= {MIN_ACCEPTED_DATE}"
            )
        else:
            validation_errors.append(
                f"{table}: latest {date_col} is NULL — expected a non-NULL historical date"
            )

# Row count checks (must not be empty)
for table in GOLD_TABLES:
    full_name = f"{GOLD_SCHEMA}.{table}"
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_name}").collect()[0]["cnt"]
    if count == 0:
        validation_errors.append(f"{table}: table is EMPTY (0 rows)")
        print(f"  [✗ EMPTY] {table}")

if validation_errors:
    error_msg = "Publish validation failed:\n" + "\n".join(f"  - {e}" for e in validation_errors)
    raise Exception(error_msg)

print("\n✅ All freshness and row-count checks passed.")

# COMMAND ----------

# MAGIC %md ## 4 — Pipeline Summary

# COMMAND ----------

summary = {
    "pipeline": "olist-production-pipeline",
    "publish_timestamp_utc": datetime.now(timezone.utc).isoformat(),
    "target_catalog": TARGET_CATALOG,
    "gold_schema": GOLD_SCHEMA,
    "tables_published": list(GOLD_TABLES.keys()),
    "optimize_ran": OPTIMIZE_TABLES,
    "validation": "PASSED",
}

print("\n── Pipeline Completion Summary ──────────────────────────────────")
print(json.dumps(summary, indent=2))
print("─────────────────────────────────────────────────────────────────")

dbutils.notebook.exit(json.dumps(summary))
