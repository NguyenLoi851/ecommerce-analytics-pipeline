# Databricks notebook source
# Purpose: Ingest all Olist CSVs from S3 raw path into Bronze Delta tables.
#
# Idempotent: re-running completely replaces the Bronze tables (full load).
# Each table receives metadata columns: _ingest_ts, _source_file, _batch_id.
#
# Prerequisites:
#   - Phase 0 complete (Unity Catalog, schemas, S3 external location configured).
#   - CSVs uploaded to s3://<RAW_BUCKET>/raw/olist/ via scripts/upload_to_s3.py.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# Read from job parameters (widgets); fall back to dev defaults for interactive runs.
dbutils.widgets.text("RAW_BUCKET",      "e-commercial-pipeline-olist-raw-dev")
dbutils.widgets.text("TARGET_CATALOG",  "dev")
dbutils.widgets.text("TARGET_SCHEMA",   "bronze")

RAW_BUCKET    = dbutils.widgets.get("RAW_BUCKET")
RAW_PREFIX    = "raw/olist"
CATALOG       = dbutils.widgets.get("TARGET_CATALOG")
BRONZE_SCHEMA = dbutils.widgets.get("TARGET_SCHEMA")

BATCH_ID = str(uuid.uuid4())
INGEST_TS = datetime.now(timezone.utc)

S3_BASE_PATH = f"s3://{RAW_BUCKET}/{RAW_PREFIX}"

print(f"Batch ID  : {BATCH_ID}")
print(f"Ingest TS : {INGEST_TS.isoformat()}")
print(f"Target    : {CATALOG}.{BRONZE_SCHEMA}")
print(f"Source    : {S3_BASE_PATH}")

# COMMAND ----------
# MAGIC %md ## Helper functions

# COMMAND ----------


def read_csv(path: str, **options) -> DataFrame:
    """Read a CSV from S3 with sensible defaults."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")   # all strings first; cast explicitly below
        .option("nullValue", "")      # interpret blank CSV fields as NULL
        .option("emptyValue", "")     # treat empty values consistently as missing
        .option("multiLine", "true")
        .option("escape", '"')
        .options(**options)
        .load(path)
    )


def add_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Append standard metadata columns to a Bronze DataFrame."""
    return (
        df
        .withColumn("_ingest_ts",    F.lit(INGEST_TS).cast(TimestampType()))
        .withColumn("_source_file",  F.lit(source_file))
        .withColumn("_batch_id",     F.lit(BATCH_ID))
    )


def write_bronze(df: DataFrame, table: str) -> None:
    """
    Write DataFrame to a Unity Catalog Bronze Delta table.
    Uses REPLACE to make the load idempotent (full-load pattern for Phase 1).
    """
    full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
    print(f"  Writing {df.count():,} rows -> {full_name}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    print(f"  Done: {full_name}")


def ingest(source_file: str, table: str, cast_exprs: dict, **csv_options) -> None:
    """End-to-end ingest: read CSV -> cast -> add metadata -> write Delta."""
    path = f"{S3_BASE_PATH}/{source_file}"
    print(f"\n[{table}] Reading {path} ...")

    raw = read_csv(path, **csv_options)

    # Apply explicit casts; keep all other columns as StringType.
    casted = raw
    for col_name, col_expr in cast_exprs.items():
        casted = casted.withColumn(col_name, col_expr)

    enriched = add_metadata(casted, source_file)
    write_bronze(enriched, table)


# COMMAND ----------
# MAGIC %md ## 1. Orders

# COMMAND ----------

ingest(
    source_file="olist_orders_dataset.csv",
    table="orders",
    cast_exprs={
        "order_purchase_timestamp":  F.to_timestamp("order_purchase_timestamp"),
        "order_approved_at":         F.to_timestamp("order_approved_at"),
        "order_delivered_carrier_date": F.to_timestamp("order_delivered_carrier_date"),
        "order_delivered_customer_date": F.to_timestamp("order_delivered_customer_date"),
        "order_estimated_delivery_date": F.to_timestamp("order_estimated_delivery_date"),
    },
)

# COMMAND ----------
# MAGIC %md ## 2. Order Items

# COMMAND ----------

ingest(
    source_file="olist_order_items_dataset.csv",
    table="order_items",
    cast_exprs={
        "order_item_id":      F.col("order_item_id").cast(IntegerType()),
        "price":              F.col("price").cast(DecimalType(12, 2)),
        "freight_value":      F.col("freight_value").cast(DecimalType(12, 2)),
        "shipping_limit_date": F.to_timestamp("shipping_limit_date"),
    },
)

# COMMAND ----------
# MAGIC %md ## 3. Order Payments

# COMMAND ----------

ingest(
    source_file="olist_order_payments_dataset.csv",
    table="order_payments",
    cast_exprs={
        "payment_sequential":  F.col("payment_sequential").cast(IntegerType()),
        "payment_installments": F.col("payment_installments").cast(IntegerType()),
        "payment_value":        F.col("payment_value").cast(DecimalType(12, 2)),
    },
)

# COMMAND ----------
# MAGIC %md ## 4. Order Reviews

# COMMAND ----------

ingest(
    source_file="olist_order_reviews_dataset.csv",
    table="order_reviews",
    cast_exprs={
        "review_score":              F.col("review_score").cast(IntegerType()),
        "review_creation_date":      F.to_timestamp("review_creation_date"),
        "review_answer_timestamp":   F.to_timestamp("review_answer_timestamp"),
    },
)

# COMMAND ----------
# MAGIC %md ## 5. Customers

# COMMAND ----------

ingest(
    source_file="olist_customers_dataset.csv",
    table="customers",
    cast_exprs={},   # all string columns; no casts needed
)

# COMMAND ----------
# MAGIC %md ## 6. Sellers

# COMMAND ----------

ingest(
    source_file="olist_sellers_dataset.csv",
    table="sellers",
    cast_exprs={},
)

# COMMAND ----------
# MAGIC %md ## 7. Products

# COMMAND ----------

ingest(
    source_file="olist_products_dataset.csv",
    table="products",
    cast_exprs={
        "product_name_lenght":           F.col("product_name_lenght").cast(IntegerType()),
        "product_description_lenght":    F.col("product_description_lenght").cast(IntegerType()),
        "product_photos_qty":            F.col("product_photos_qty").cast(IntegerType()),
        "product_weight_g":              F.col("product_weight_g").cast(IntegerType()),
        "product_length_cm":             F.col("product_length_cm").cast(IntegerType()),
        "product_height_cm":             F.col("product_height_cm").cast(IntegerType()),
        "product_width_cm":              F.col("product_width_cm").cast(IntegerType()),
    },
)

# COMMAND ----------
# MAGIC %md ## 8. Geolocation

# COMMAND ----------

ingest(
    source_file="olist_geolocation_dataset.csv",
    table="geolocation",
    cast_exprs={
        "geolocation_lat": F.col("geolocation_lat").cast(DoubleType()),
        "geolocation_lng": F.col("geolocation_lng").cast(DoubleType()),
    },
)

# COMMAND ----------
# MAGIC %md ## 9. Product Category Name Translation

# COMMAND ----------

ingest(
    source_file="product_category_name_translation.csv",
    table="product_category_name_translation",
    cast_exprs={},
)

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

tables = [
    "orders", "order_items", "order_payments", "order_reviews",
    "customers", "sellers", "products", "geolocation",
    "product_category_name_translation",
]

print(f"\n{'Table':<45} {'Row Count':>12}")
print("-" * 60)
for t in tables:
    full = f"{CATALOG}.{BRONZE_SCHEMA}.{t}"
    cnt = spark.table(full).count()
    print(f"{full:<45} {cnt:>12,}")

print(f"\nBatch ID  : {BATCH_ID}")
print(f"Ingest TS : {INGEST_TS.isoformat()}")
print("Bronze ingestion complete.")
