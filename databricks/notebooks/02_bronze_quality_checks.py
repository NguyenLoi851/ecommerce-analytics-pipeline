# Databricks notebook source
# Purpose: Baseline data quality checks on Bronze Delta tables.
#
# Checks per table:
#   1. Row count — assert non-zero.
#   2. Null check — assert no nulls on primary key columns.
#   3. Duplicate check — assert no duplicate primary keys.
#
# Run after 01_bronze_ingestion.py. All failures raise AssertionError.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "dev"
BRONZE_SCHEMA = "bronze"

failures: list[str] = []


def tbl(name: str):
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{name}")


def check_row_count(table: str, min_rows: int = 1) -> None:
    cnt = tbl(table).count()
    status = "PASS" if cnt >= min_rows else "FAIL"
    msg = f"[{status}] {table}: row_count={cnt:,} (min={min_rows:,})"
    print(msg)
    if status == "FAIL":
        failures.append(msg)


def check_nulls(table: str, pk_cols: list[str]) -> None:
    df = tbl(table)
    for col in pk_cols:
        null_cnt = df.filter(F.col(col).isNull()).count()
        status = "PASS" if null_cnt == 0 else "FAIL"
        msg = f"[{status}] {table}.{col}: null_count={null_cnt:,}"
        print(msg)
        if status == "FAIL":
            failures.append(msg)


def check_duplicates(table: str, pk_cols: list[str]) -> None:
    df = tbl(table)
    total = df.count()
    distinct = df.select(*pk_cols).distinct().count()
    dups = total - distinct
    status = "PASS" if dups == 0 else "FAIL"
    msg = f"[{status}] {table} ({', '.join(pk_cols)}): duplicate_count={dups:,}"
    print(msg)
    if status == "FAIL":
        failures.append(msg)


# COMMAND ----------
# MAGIC %md ## Orders

# COMMAND ----------

check_row_count("orders", min_rows=1000)
check_nulls("orders", ["order_id", "customer_id"])
check_duplicates("orders", ["order_id"])

# COMMAND ----------
# MAGIC %md ## Order Items

# COMMAND ----------

check_row_count("order_items", min_rows=1000)
check_nulls("order_items", ["order_id", "order_item_id", "product_id", "seller_id"])
check_duplicates("order_items", ["order_id", "order_item_id"])

# COMMAND ----------
# MAGIC %md ## Order Payments

# COMMAND ----------

check_row_count("order_payments", min_rows=1000)
check_nulls("order_payments", ["order_id", "payment_sequential"])
check_duplicates("order_payments", ["order_id", "payment_sequential"])

# COMMAND ----------
# MAGIC %md ## Order Reviews

# COMMAND ----------

check_row_count("order_reviews", min_rows=1000)
check_nulls("order_reviews", ["review_id", "order_id"])
# Reviews can have multiple entries per order if updated; skip duplicate check on (review_id, order_id)

# COMMAND ----------
# MAGIC %md ## Customers

# COMMAND ----------

check_row_count("customers", min_rows=1000)
check_nulls("customers", ["customer_id"])
check_duplicates("customers", ["customer_id"])

# COMMAND ----------
# MAGIC %md ## Sellers

# COMMAND ----------

check_row_count("sellers", min_rows=1)
check_nulls("sellers", ["seller_id"])
check_duplicates("sellers", ["seller_id"])

# COMMAND ----------
# MAGIC %md ## Products

# COMMAND ----------

check_row_count("products", min_rows=1)
check_nulls("products", ["product_id"])
check_duplicates("products", ["product_id"])

# COMMAND ----------
# MAGIC %md ## Geolocation

# COMMAND ----------

check_row_count("geolocation", min_rows=1000)
check_nulls("geolocation", ["geolocation_zip_code_prefix"])
# Geolocation is a lookup; duplicates on zip are expected

# COMMAND ----------
# MAGIC %md ## Product Category Name Translation

# COMMAND ----------

check_row_count("product_category_name_translation", min_rows=1)
check_nulls("product_category_name_translation", ["product_category_name"])
check_duplicates("product_category_name_translation", ["product_category_name"])

# COMMAND ----------
# MAGIC %md ## Results Summary

# COMMAND ----------

print("\n" + "=" * 60)
if failures:
    print(f"QUALITY CHECK FAILED — {len(failures)} issue(s):")
    for f in failures:
        print(f"  {f}")
    raise AssertionError(
        f"{len(failures)} quality check(s) failed. See output above."
    )
else:
    print("All quality checks PASSED.")
print("=" * 60)
