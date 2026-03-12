# Databricks notebook source
# Purpose: validate that Unity Catalog, schemas, and S3 external location access are working.

from pyspark.sql import functions as F

spark.sql("USE CATALOG dev")
spark.sql("USE SCHEMA bronze")

spark.sql("CREATE TABLE IF NOT EXISTS dev.bronze.phase0_smoke_test (id INT, created_at TIMESTAMP)")

spark.sql("INSERT INTO dev.bronze.phase0_smoke_test VALUES (1, current_timestamp())")

df = spark.sql("SELECT * FROM dev.bronze.phase0_smoke_test ORDER BY created_at DESC LIMIT 5")
display(df)

# Optional: test external location read (replace with actual existing path)
# test_df = spark.read.option("header", True).csv("s3://<your-raw-bucket>/raw/olist/olist_orders_dataset.csv")
# display(test_df.limit(10))
