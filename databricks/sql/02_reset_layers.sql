-- DANGER: Destructive reset script.
-- This drops all tables/views/materialized views/functions in the target schemas.
--
-- Run in Databricks SQL Editor and set target_catalog at the top.
-- Typical values: dev or prod.
-- Set raw_bucket_name to your real S3 bucket before running.

-- Example:
--   SET VAR target_catalog = 'dev';
--   Drops: dev.bronze, dev.silver, dev.gold (CASCADE)

DECLARE OR REPLACE target_catalog STRING DEFAULT 'dev';
DECLARE OR REPLACE raw_bucket_name STRING DEFAULT '<your-raw-bucket-name>';
-- Change only this line when needed:
-- SET VAR target_catalog = 'prod';
-- SET VAR raw_bucket_name = 'your-real-raw-bucket-name';

-- Optional safety output
SELECT target_catalog AS selected_catalog, raw_bucket_name AS selected_raw_bucket;

-- 1) Drop all layer schemas (and all contained objects)
EXECUTE IMMEDIATE 'DROP SCHEMA IF EXISTS ' || target_catalog || '.bronze CASCADE';
EXECUTE IMMEDIATE 'DROP SCHEMA IF EXISTS ' || target_catalog || '.silver CASCADE';
EXECUTE IMMEDIATE 'DROP SCHEMA IF EXISTS ' || target_catalog || '.gold CASCADE';

-- 2) Recreate empty layer schemas
EXECUTE IMMEDIATE
	'CREATE SCHEMA IF NOT EXISTS ' || target_catalog || '.bronze MANAGED LOCATION ' ||
	chr(39) || 's3://' || raw_bucket_name || '/delta/olist/' || target_catalog || '/bronze' || chr(39);
EXECUTE IMMEDIATE
	'CREATE SCHEMA IF NOT EXISTS ' || target_catalog || '.silver MANAGED LOCATION ' ||
	chr(39) || 's3://' || raw_bucket_name || '/delta/olist/' || target_catalog || '/silver' || chr(39);
EXECUTE IMMEDIATE
	'CREATE SCHEMA IF NOT EXISTS ' || target_catalog || '.gold MANAGED LOCATION ' ||
	chr(39) || 's3://' || raw_bucket_name || '/delta/olist/' || target_catalog || '/gold' || chr(39);

-- 3) Verify
EXECUTE IMMEDIATE 'SHOW TABLES IN ' || target_catalog || '.bronze';
EXECUTE IMMEDIATE 'SHOW TABLES IN ' || target_catalog || '.silver';
EXECUTE IMMEDIATE 'SHOW TABLES IN ' || target_catalog || '.gold';
