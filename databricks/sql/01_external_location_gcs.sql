-- Run in Databricks SQL Editor.
-- Replace placeholders before running.
-- Requires Unity Catalog and a GCP service account key in secret scope.

-- 1) Storage credential using a service account JSON key in Databricks secret scope
--    secret format: {{secrets/<scope>/<key>}}
CREATE STORAGE CREDENTIAL IF NOT EXISTS olist_gcs_credential
WITH GCP_SERVICE_ACCOUNT_KEY = '{{secrets/olist-secrets/gcs-sa-key}}'
COMMENT 'Storage credential for Olist GCS access';

-- 2) External location that points to your GCS bucket/prefix
CREATE EXTERNAL LOCATION IF NOT EXISTS olist_raw_ext_loc
URL 'gs://e-commercial-pipeline-olist-raw-dev/raw/olist'
WITH (STORAGE CREDENTIAL olist_gcs_credential)
COMMENT 'Olist raw landing data in GCS';

-- Optional curated location
CREATE EXTERNAL LOCATION IF NOT EXISTS olist_curated_ext_loc
URL 'gs://e-commercial-pipeline-olist-curated-dev/curated/olist'
WITH (STORAGE CREDENTIAL olist_gcs_credential)
COMMENT 'Olist curated zone in GCS';

-- 3) Permissions (adjust principals)
GRANT READ FILES ON EXTERNAL LOCATION olist_raw_ext_loc TO `account users`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION olist_curated_ext_loc TO `account users`;
