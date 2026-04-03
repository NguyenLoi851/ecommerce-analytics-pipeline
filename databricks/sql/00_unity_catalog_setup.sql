-- Run in Databricks SQL Editor as account/workspace admin.
-- Replace catalog names if needed.
-- Replace <your-raw-bucket> before running.

CREATE CATALOG IF NOT EXISTS dev;
CREATE CATALOG IF NOT EXISTS prod;

CREATE SCHEMA IF NOT EXISTS dev.bronze
MANAGED LOCATION 's3://<your-raw-bucket>/delta/olist/dev/bronze';
CREATE SCHEMA IF NOT EXISTS dev.silver
MANAGED LOCATION 's3://<your-raw-bucket>/delta/olist/dev/silver';
CREATE SCHEMA IF NOT EXISTS dev.gold
MANAGED LOCATION 's3://<your-raw-bucket>/delta/olist/dev/gold';

CREATE SCHEMA IF NOT EXISTS prod.bronze
MANAGED LOCATION 's3://<your-raw-bucket>/delta/olist/prod/bronze';
CREATE SCHEMA IF NOT EXISTS prod.silver
MANAGED LOCATION 's3://<your-raw-bucket>/delta/olist/prod/silver';
CREATE SCHEMA IF NOT EXISTS prod.gold
MANAGED LOCATION 's3://<your-raw-bucket>/delta/olist/prod/gold';
