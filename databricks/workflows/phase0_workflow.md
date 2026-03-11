# Phase 0 Workflow (Manual to Scheduled)

Create a Databricks Workflow named `olist-phase0-bootstrap` with these tasks:

1. `sql_uc_setup`
   - Type: SQL task
   - Script: `databricks/sql/00_unity_catalog_setup.sql`

2. `sql_external_location_setup`
   - Type: SQL task
   - Script: `databricks/sql/01_external_location_gcs.sql`
   - Depends on: `sql_uc_setup`

3. `notebook_smoke_test`
   - Type: Notebook task
   - Notebook: `databricks/notebooks/00_phase0_smoke_test.py`
   - Depends on: `sql_external_location_setup`

For first run, execute manually and verify all tasks pass.
After validation, keep this workflow on-demand (no schedule) as an environment bootstrap utility.
