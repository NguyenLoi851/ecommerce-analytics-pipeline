-- Run in Databricks SQL Editor.
-- Replace placeholders before running.
-- Requires Unity Catalog and an IAM role that Databricks can assume.
--
-- REQUIREMENTS before running:
--   1. Unity Catalog must be enabled on this workspace.
--   2. You must be a metastore admin or storage credential admin.
--   3. The IAM role trust policy must allow Databricks to assume the role.
--   4. Run each statement individually (not as a batch) in the SQL Editor.

-- 1) Storage credential using an AWS IAM role
-- Syntax error will occur (config by UI instead)
-- CREATE STORAGE CREDENTIAL IF NOT EXISTS olist_s3_credential
-- WITH AWS_IAM_ROLE '<databricks-s3-access-role-arn>'
-- COMMENT 'Storage credential for Olist S3 access';

-- 2) External locations that point to your S3 bucket/prefixes
CREATE EXTERNAL LOCATION IF NOT EXISTS olist_raw_ext_loc
URL 's3://<your-raw-bucket>/raw/olist'
WITH (STORAGE CREDENTIAL olist_s3_credential)
COMMENT 'Olist raw landing data in S3';

-- Auto Loader state base path (schema + checkpoints)
CREATE EXTERNAL LOCATION IF NOT EXISTS olist_autoloader_state_ext_loc
URL 's3://<your-raw-bucket>/state/autoloader/olist'
WITH (STORAGE CREDENTIAL olist_s3_credential)
COMMENT 'Auto Loader schema and checkpoint state for Olist ingestion';

-- 3) Permissions (adjust principals)
GRANT READ FILES ON EXTERNAL LOCATION olist_raw_ext_loc TO `account users`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION olist_autoloader_state_ext_loc TO `account users`;
