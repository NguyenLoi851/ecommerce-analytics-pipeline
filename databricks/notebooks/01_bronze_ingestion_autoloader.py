# Databricks notebook source
# Purpose: Bronze ingestion using Databricks Auto Loader.
#
# Design for this project:
#   - Incremental tables use Auto Loader (cloudFiles) + checkpoint state.
#   - Since each business entity is stored in a single CSV file, Auto Loader
#     naturally skips files already discovered in previous runs.
#   - Two small reference tables always run as full overwrite loads:
#       * geolocation
#       * product_category_name_translation
#
# Notes:
#   - This notebook is intentionally separate from 01_bronze_ingestion.py so you
#     can choose either implementation in workflows.
#   - Use FORCE_RELOAD=true to clear Auto Loader state and reload incremental tables.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType, DoubleType, IntegerType, TimestampType

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("RAW_BUCKET", "e-commercial-pipeline-olist-raw-dev")
dbutils.widgets.text("TARGET_CATALOG", "dev")
dbutils.widgets.text("TARGET_SCHEMA", "bronze")
dbutils.widgets.text("FORCE_RELOAD", "false")
dbutils.widgets.text("DELTA_BASE_PATH", "")

# Base path for Auto Loader metadata (schema + checkpoints).
# Prefer S3 so state persists independently from cluster lifecycle.
# Leave empty to auto-derive: s3://<RAW_BUCKET>/state/autoloader/olist
dbutils.widgets.text("AUTOLOADER_STATE_BASE", "")

RAW_BUCKET = dbutils.widgets.get("RAW_BUCKET")
RAW_PREFIX = "raw/olist"
CATALOG = dbutils.widgets.get("TARGET_CATALOG")
BRONZE_SCHEMA = dbutils.widgets.get("TARGET_SCHEMA")
FORCE_RELOAD = dbutils.widgets.get("FORCE_RELOAD").strip().lower() == "true"
_delta_base_widget = dbutils.widgets.get("DELTA_BASE_PATH").strip()
DELTA_BASE_PATH = (
    _delta_base_widget
    if _delta_base_widget
    else f"s3://{RAW_BUCKET}/delta/olist"
).rstrip("/")

_state_base_widget = dbutils.widgets.get("AUTOLOADER_STATE_BASE").strip()
AUTOLOADER_STATE_BASE = (
    _state_base_widget
    if _state_base_widget
    else f"s3://{RAW_BUCKET}/state/autoloader/olist"
).rstrip("/")

if not (
    AUTOLOADER_STATE_BASE.startswith("s3://")
    or AUTOLOADER_STATE_BASE.startswith("dbfs:/")
):
    raise ValueError(
        "AUTOLOADER_STATE_BASE must start with 's3://' or 'dbfs:/'. "
        f"Got: {AUTOLOADER_STATE_BASE}"
    )

if not (
    DELTA_BASE_PATH.startswith("s3://")
    or DELTA_BASE_PATH.startswith("dbfs:/")
):
    raise ValueError(
        "DELTA_BASE_PATH must start with 's3://' or 'dbfs:/'. "
        f"Got: {DELTA_BASE_PATH}"
    )

BATCH_ID = str(uuid.uuid4())
INGEST_TS = datetime.now(timezone.utc)

S3_BASE_PATH = f"s3://{RAW_BUCKET}/{RAW_PREFIX}"
TARGET_PREFIX = f"{CATALOG}.{BRONZE_SCHEMA}"

FULL_LOAD_TABLES = {"geolocation", "product_category_name_translation"}

print(f"Batch ID       : {BATCH_ID}")
print(f"Ingest TS      : {INGEST_TS.isoformat()}")
print(f"Target schema  : {TARGET_PREFIX}")
print(f"Source path    : {S3_BASE_PATH}")
print(f"Force reload   : {FORCE_RELOAD}")
print(f"State base     : {AUTOLOADER_STATE_BASE}")
print(f"Delta base     : {DELTA_BASE_PATH}")
print(f"Full-load only : {sorted(FULL_LOAD_TABLES)}")

# COMMAND ----------
# MAGIC %md ## Helpers

# COMMAND ----------


def read_csv_batch(path: str, **options) -> DataFrame:
    """Batch CSV reader used for always-full-load reference tables."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .option("emptyValue", "")
        .option("multiLine", "true")
        .option("escape", '"')
        .options(**options)
        .load(path)
    )


def apply_casts(df: DataFrame, cast_exprs: dict) -> DataFrame:
    out = df
    for col_name, col_expr in cast_exprs.items():
        out = out.withColumn(col_name, col_expr)
    return out


def rename_input_columns(df: DataFrame, rename_columns=None) -> DataFrame:
    out = df
    if rename_columns:
        for source_col, target_col in rename_columns.items():
            if source_col in out.columns and source_col != target_col:
                out = out.withColumnRenamed(source_col, target_col)
    return out


def add_metadata_batch(df: DataFrame, source_file: str) -> DataFrame:
    return (
        df.withColumn("_ingest_ts", F.lit(INGEST_TS).cast(TimestampType()))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )


def add_metadata_stream(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )


def autoloader_schema_path(table: str) -> str:
    return f"{AUTOLOADER_STATE_BASE}/schema/{CATALOG}/{BRONZE_SCHEMA}/{table}"


def autoloader_checkpoint_path(table: str) -> str:
    return f"{AUTOLOADER_STATE_BASE}/checkpoints/{CATALOG}/{BRONZE_SCHEMA}/{table}"


def full_name(table: str) -> str:
    return f"{TARGET_PREFIX}.{table}"


def bronze_table_path(table: str) -> str:
    return f"{DELTA_BASE_PATH}/{CATALOG}/{BRONZE_SCHEMA}/{table}"


def ensure_table_binding(table: str, table_path: str) -> None:
    target_name = full_name(table)
    if spark.catalog.tableExists(target_name):
        current_location = (
            spark.sql(f"DESCRIBE DETAIL {target_name}")
            .select("location")
            .first()["location"]
            .rstrip("/")
        )
        target_location = table_path.rstrip("/")
        if current_location != target_location:
            print(f"  Repointing {target_name} -> {table_path}")
            spark.sql(f"ALTER TABLE {target_name} SET LOCATION '{table_path}'")
    else:
        spark.sql(f"CREATE TABLE {target_name} USING DELTA LOCATION '{table_path}'")


def clear_autoloader_state(table: str) -> None:
    schema_path = autoloader_schema_path(table)
    checkpoint_path = autoloader_checkpoint_path(table)
    print(f"  Clearing schema state     : {schema_path}")
    print(f"  Clearing checkpoint state : {checkpoint_path}")
    dbutils.fs.rm(schema_path, True)
    dbutils.fs.rm(checkpoint_path, True)


def drop_target_table_if_exists(table: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {full_name(table)}")


def full_load_ingest(
    source_file: str,
    table: str,
    cast_exprs: dict,
    rename_columns=None,
    **csv_options,
) -> None:
    """Always-full-load mode (overwrite) for small reference tables."""
    path = f"{S3_BASE_PATH}/{source_file}"
    target_path = bronze_table_path(table)
    print(f"\n[{table}] FULL LOAD (overwrite) from {path}")
    print(f"  Delta path      : {target_path}")

    df = read_csv_batch(path, **csv_options)
    df = rename_input_columns(df, rename_columns)
    df = apply_casts(df, cast_exprs)
    df = add_metadata_batch(df, source_file)

    row_count = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    ensure_table_binding(table, target_path)

    print(f"  Wrote {row_count:,} rows -> {full_name(table)}")


def incremental_autoloader_ingest(
    source_file: str,
    table: str,
    cast_exprs: dict,
    rename_columns=None,
    **csv_options,
) -> None:
    """
    Incremental ingestion via Auto Loader.

    Auto Loader tracks discovered files through checkpoint state, so reruns skip
    previously discovered files automatically.
    """
    source_path = S3_BASE_PATH
    schema_path = autoloader_schema_path(table)
    checkpoint_path = autoloader_checkpoint_path(table)
    target_path = bronze_table_path(table)

    print(f"\n[{table}] AUTO LOADER incremental ingest from {source_path}")
    print(f"  File filter     : {source_file}")
    print(f"  Schema path     : {schema_path}")
    print(f"  Checkpoint path : {checkpoint_path}")
    print(f"  Delta path      : {target_path}")

    if FORCE_RELOAD:
        print("  FORCE_RELOAD=true: resetting Auto Loader state and target table")
        clear_autoloader_state(table)
        drop_target_table_if_exists(table)

    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("pathGlobFilter", source_file)
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("nullValue", "")
        .option("emptyValue", "")
        .options(**csv_options)
        .load(source_path)
    )

    stream_df = rename_input_columns(stream_df, rename_columns)
    stream_df = apply_casts(stream_df, cast_exprs)
    stream_df = add_metadata_stream(stream_df)

    query = (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("mergeSchema", "true")
        .option("checkpointLocation", checkpoint_path)
        .option("path", target_path)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    if query.exception() is not None:
        raise query.exception()

    ensure_table_binding(table, target_path)

    total_rows = spark.table(full_name(table)).count()
    print(f"  Current total rows in {full_name(table)}: {total_rows:,}")


# COMMAND ----------
# MAGIC %md ## Ingestion execution

# COMMAND ----------

# MAGIC %md ## 1. Orders

# COMMAND ----------

incremental_autoloader_ingest(
    source_file="olist_orders_dataset.csv",
    table="orders",
    cast_exprs={
        "order_purchase_timestamp": F.to_timestamp("order_purchase_timestamp"),
        "order_approved_at": F.to_timestamp("order_approved_at"),
        "order_delivered_carrier_date": F.to_timestamp("order_delivered_carrier_date"),
        "order_delivered_customer_date": F.to_timestamp("order_delivered_customer_date"),
        "order_estimated_delivery_date": F.to_timestamp("order_estimated_delivery_date"),
    },
)

# COMMAND ----------

# MAGIC %md ## 2. Order Items

# COMMAND ----------

incremental_autoloader_ingest(
    source_file="olist_order_items_dataset.csv",
    table="order_items",
    cast_exprs={
        "order_item_id": F.col("order_item_id").cast(IntegerType()),
        "price": F.col("price").cast(DecimalType(12, 2)),
        "freight_value": F.col("freight_value").cast(DecimalType(12, 2)),
        "shipping_limit_date": F.to_timestamp("shipping_limit_date"),
    },
)

# COMMAND ----------

# MAGIC %md ## 3. Order Payments

# COMMAND ----------

incremental_autoloader_ingest(
    source_file="olist_order_payments_dataset.csv",
    table="order_payments",
    cast_exprs={
        "payment_sequential": F.col("payment_sequential").cast(IntegerType()),
        "payment_installments": F.col("payment_installments").cast(IntegerType()),
        "payment_value": F.col("payment_value").cast(DecimalType(12, 2)),
    },
)

# COMMAND ----------

# MAGIC %md ## 4. Order Reviews

# COMMAND ----------

incremental_autoloader_ingest(
    source_file="olist_order_reviews_dataset.csv",
    table="order_reviews",
    cast_exprs={
        "review_score": F.col("review_score").cast(IntegerType()),
        "review_creation_date": F.to_timestamp("review_creation_date"),
        "review_answer_timestamp": F.to_timestamp("review_answer_timestamp"),
    },
)

# COMMAND ----------

# MAGIC %md ## 5. Customers

# COMMAND ----------

incremental_autoloader_ingest(
    source_file="olist_customers_dataset.csv",
    table="customers",
    cast_exprs={},
)

# COMMAND ----------

# MAGIC %md ## 6. Sellers

# COMMAND ----------

incremental_autoloader_ingest(
    source_file="olist_sellers_dataset.csv",
    table="sellers",
    cast_exprs={},
)

# COMMAND ----------

# MAGIC %md ## 7. Products

# COMMAND ----------

incremental_autoloader_ingest(
    source_file="olist_products_dataset.csv",
    table="products",
    rename_columns={
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
    },
    cast_exprs={
        "product_name_length": F.col("product_name_length").cast(IntegerType()),
        "product_description_length": F.col("product_description_length").cast(IntegerType()),
        "product_photos_qty": F.col("product_photos_qty").cast(IntegerType()),
        "product_weight_g": F.col("product_weight_g").cast(IntegerType()),
        "product_length_cm": F.col("product_length_cm").cast(IntegerType()),
        "product_height_cm": F.col("product_height_cm").cast(IntegerType()),
        "product_width_cm": F.col("product_width_cm").cast(IntegerType()),
    },
)

# COMMAND ----------

# MAGIC %md ## 8. Geolocation
# MAGIC *(always full load)*

# COMMAND ----------

full_load_ingest(
    source_file="olist_geolocation_dataset.csv",
    table="geolocation",
    cast_exprs={
        "geolocation_lat": F.col("geolocation_lat").cast(DoubleType()),
        "geolocation_lng": F.col("geolocation_lng").cast(DoubleType()),
    },
)

# COMMAND ----------

# MAGIC %md ## 9. Product Category Name Translation
# MAGIC *(always full load)*

# COMMAND ----------

full_load_ingest(
    source_file="product_category_name_translation.csv",
    table="product_category_name_translation",
    cast_exprs={},
)

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

tables = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "sellers",
    "products",
    "geolocation",
    "product_category_name_translation",
]

print(f"\n{'Table':<45} {'Row Count':>12}")
print("-" * 60)
for t in tables:
    name = full_name(t)
    try:
        cnt = spark.table(name).count()
        print(f"{name:<45} {cnt:>12,}")
    except Exception:
        print(f"{name:<45} {'(not found)':>12}")

print()
print(f"Batch ID  : {BATCH_ID}")
print(f"Ingest TS : {INGEST_TS.isoformat()}")
print("Bronze ingestion (Auto Loader version) complete.")
