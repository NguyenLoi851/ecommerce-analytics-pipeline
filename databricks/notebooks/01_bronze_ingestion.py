# Databricks notebook source
# Purpose: Ingest all Olist CSVs from S3 raw path into Bronze Delta tables.
#
# Incremental strategy (Phase 5):
#   Each Olist source file contains the complete history for its entity in a single
#   static CSV.  Once a file has been successfully loaded there is no new data to
#   pick up, so subsequent runs simply skip it.
#
#   Skip logic is enforced via an _ingestion_registry Delta table stored in the
#   same Bronze schema.  A row is written to the registry only after a successful
#   load; partial / failed loads are therefore automatically retried on the next run.
#
#   Two tables are exempt and always perform a full overwrite:
#     - geolocation
#     - product_category_name_translation
#   These reference datasets are small and may be refreshed independently.
#
#   Set FORCE_RELOAD=true to bypass the registry check and reload every table.
#
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
# Set to "true" to bypass the registry check and re-ingest every table from scratch.
dbutils.widgets.text("FORCE_RELOAD",    "false")

RAW_BUCKET    = dbutils.widgets.get("RAW_BUCKET")
RAW_PREFIX    = "raw/olist"
CATALOG       = dbutils.widgets.get("TARGET_CATALOG")
BRONZE_SCHEMA = dbutils.widgets.get("TARGET_SCHEMA")
FORCE_RELOAD  = dbutils.widgets.get("FORCE_RELOAD").strip().lower() == "true"

BATCH_ID = str(uuid.uuid4())
INGEST_TS = datetime.now(timezone.utc)

S3_BASE_PATH = f"s3://{RAW_BUCKET}/{RAW_PREFIX}"

# ---------------------------------------------------------------------------
# Tables that always perform a full overwrite regardless of the registry.
# Every other table loads once and is skipped on subsequent runs.
# ---------------------------------------------------------------------------
FULL_LOAD_TABLES = {"geolocation", "product_category_name_translation"}

REGISTRY_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}._ingestion_registry"

print(f"Batch ID     : {BATCH_ID}")
print(f"Ingest TS    : {INGEST_TS.isoformat()}")
print(f"Target       : {CATALOG}.{BRONZE_SCHEMA}")
print(f"Source       : {S3_BASE_PATH}")
print(f"Force reload : {FORCE_RELOAD}")
print(f"Full-load    : {sorted(FULL_LOAD_TABLES)}")

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
    """Write DataFrame to a Unity Catalog Bronze Delta table (full overwrite)."""
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
    """
    End-to-end ingest: read CSV -> cast -> add metadata -> write Delta.

    Incremental logic (Phase 5)
    ---------------------------
    * Tables in FULL_LOAD_TABLES always overwrite and are never skipped.
      The registry is still updated so the run history remains visible.
    * All other tables are skipped when source_file already appears in the
      registry — meaning the file has been successfully loaded before.
      Pass FORCE_RELOAD=true (widget) to bypass this check for the current run.
    """
    is_full_load = table in FULL_LOAD_TABLES

    if not is_full_load and is_already_loaded(source_file, table):
        print(f"\n[{table}] SKIPPED — '{source_file}' already registered. "
              f"Set FORCE_RELOAD=true to force a reload.")
        return

    if is_full_load:
        print(f"\n[{table}] FULL LOAD (always-overwrite) — reading {source_file} ...")
    else:
        print(f"\n[{table}] First load — reading {source_file} ...")

    path = f"{S3_BASE_PATH}/{source_file}"
    raw  = read_csv(path, **csv_options)

    # Apply explicit casts; keep all other columns as StringType.
    casted = raw
    for col_name, col_expr in cast_exprs.items():
        casted = casted.withColumn(col_name, col_expr)

    enriched = add_metadata(casted, source_file)
    row_count = enriched.count()
    write_bronze(enriched, table)

    # Record the successful load (MERGE keeps one row per file in the registry).
    register_load(source_file, table, row_count)


# COMMAND ----------
# MAGIC %md ## Ingestion registry (Phase 5)

# COMMAND ----------


def ensure_registry_table() -> None:
    """
    Create the ingestion registry Delta table if it does not yet exist.

    The registry tracks which source files have been successfully loaded so
    that subsequent runs can skip them automatically.  A row is written only
    after a successful write_bronze call, so failed or interrupted loads are
    never marked as done and will be retried on the next run.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
            source_file  STRING    NOT NULL COMMENT 'CSV filename relative to the S3 raw prefix',
            table_name   STRING    NOT NULL COMMENT 'Bronze Delta table name',
            loaded_at    TIMESTAMP          COMMENT 'Timestamp of the most recent successful load',
            batch_id     STRING             COMMENT 'BATCH_ID of the run that loaded this file',
            row_count    LONG               COMMENT 'Number of rows written in the last load'
        )
        USING DELTA
        COMMENT 'Tracks successfully completed Bronze ingestion loads (Phase 5 incremental).'
    """)


def is_already_loaded(source_file: str, table: str) -> bool:
    """
    Return True only when all of the following are true:
      1) FORCE_RELOAD is False
      2) source_file has a completed registry entry
      3) the target Bronze table still exists

    This protects against manual table drops: if a table is removed while
    the registry still has an entry, the load is re-run to recreate it.
    """
    if FORCE_RELOAD:
        return False

    count = spark.sql(
        f"SELECT 1 FROM {REGISTRY_TABLE} WHERE source_file = '{source_file}' LIMIT 1"
    ).count()
    if count == 0:
        return False

    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
    if not spark.catalog.tableExists(target_table):
        print(
            f"[{table}] Registry has '{source_file}', but target table "
            f"{target_table} is missing; re-ingesting."
        )
        return False

    return True


def register_load(source_file: str, table: str, row_count: int) -> None:
    """
    Upsert a registry entry for source_file after a successful load.

    Uses MERGE so that re-runs triggered by FORCE_RELOAD update the existing
    record (one row per file) rather than appending duplicates.
    """
    spark.sql(f"""
        MERGE INTO {REGISTRY_TABLE} AS tgt
        USING (
            SELECT
                '{source_file}'                                  AS source_file,
                '{table}'                                        AS table_name,
                CAST('{INGEST_TS.isoformat()}' AS TIMESTAMP)     AS loaded_at,
                '{BATCH_ID}'                                     AS batch_id,
                CAST({row_count} AS LONG)                        AS row_count
        ) AS src
        ON tgt.source_file = src.source_file
        WHEN MATCHED THEN
            UPDATE SET
                table_name = src.table_name,
                loaded_at  = src.loaded_at,
                batch_id   = src.batch_id,
                row_count  = src.row_count
        WHEN NOT MATCHED THEN
            INSERT (source_file, table_name, loaded_at, batch_id, row_count)
            VALUES (src.source_file, src.table_name, src.loaded_at, src.batch_id, src.row_count)
    """)


# Initialise the registry table before any ingest calls.
ensure_registry_table()
print(f"Registry  : {REGISTRY_TABLE} — ready.")


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
# MAGIC *(always full load)*

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
# MAGIC *(always full load)*

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
    try:
        cnt = spark.table(full).count()
        print(f"{full:<45} {cnt:>12,}")
    except Exception:
        print(f"{full:<45} {'(not found)':>12}")

print()
print(f"Batch ID  : {BATCH_ID}")
print(f"Ingest TS : {INGEST_TS.isoformat()}")
print(f"Registry  : {REGISTRY_TABLE}")
print()

# Show current registry state for observability.
print("Ingestion registry (Phase 5):")
spark.table(REGISTRY_TABLE).orderBy("table_name").show(truncate=False)

print("Bronze ingestion complete.")
