-- DANGER: Destructive reset script.
-- This drops all tables/views/materialized views/functions in the target schemas.
--
-- Run in Databricks SQL Editor and set target_catalog at the top.
-- Typical values: dev or prod.

-- Example:
--   SET VAR target_catalog = 'dev';
--   Drops: dev.bronze, dev.silver, dev.gold (CASCADE)

DECLARE OR REPLACE target_catalog STRING DEFAULT 'dev';
-- Change only this line when needed:
-- SET VAR target_catalog = 'prod';

-- Optional safety output
SELECT target_catalog AS selected_catalog;

-- 1) Drop all layer schemas (and all contained objects)
EXECUTE IMMEDIATE 'DROP SCHEMA IF EXISTS ' || target_catalog || '.bronze CASCADE';
EXECUTE IMMEDIATE 'DROP SCHEMA IF EXISTS ' || target_catalog || '.silver CASCADE';
EXECUTE IMMEDIATE 'DROP SCHEMA IF EXISTS ' || target_catalog || '.gold CASCADE';

-- 2) Recreate empty layer schemas
EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || target_catalog || '.bronze';
EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || target_catalog || '.silver';
EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || target_catalog || '.gold';

-- 3) Verify
EXECUTE IMMEDIATE 'SHOW TABLES IN ' || target_catalog || '.bronze';
EXECUTE IMMEDIATE 'SHOW TABLES IN ' || target_catalog || '.silver';
EXECUTE IMMEDIATE 'SHOW TABLES IN ' || target_catalog || '.gold';
