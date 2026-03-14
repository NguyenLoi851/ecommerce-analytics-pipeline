# ADR 002 — dbt for Transformation Logic

**Status**: Accepted  
**Date**: 2026  

---

## Context

The pipeline requires a transformation layer that goes from raw Bronze Delta tables to conformed Silver dimensions/facts and Gold analytics marts. Options considered:

- **dbt** (`dbt-databricks`) — SQL-first transformation tool
- **PySpark notebooks** — Databricks-native transform approach
- **Spark SQL** directly in Databricks Jobs

---

## Decision

Use **dbt with the `dbt-databricks` adapter** for all Silver and Gold transformations.

---

## Rationale

- **SQL readability**: transformation logic is plain SQL, making it easier to review, test, and onboard new contributors.
- **Built-in testing**: `not_null`, `unique`, `relationships`, and `accepted_values` tests are declared in YAML — no custom test harness needed.
- **Data lineage and docs**: `dbt docs generate` produces an interactive DAG and data dictionary for free.
- **Idempotency**: dbt's materialisation strategies (table, view, incremental) handle full or incremental runs cleanly.
- **Snapshot support**: dbt snapshots provide a straightforward path to SCD Type 2 without custom MERGE code.
- **dbt-databricks adapter**: runs dbt tasks natively on Databricks SQL Warehouse with `dbt_task` in Workflows — no separate execution environment needed.

---

## Consequences

- Bronze ingestion remains in PySpark notebooks (CSV → Delta), since it requires file discovery and Auto Loader features that dbt cannot handle.
- dbt `profiles.yml` must be configured locally and secrets must be supplied to GitHub Actions via repository secrets.
- Python version must be 3.11 — dbt dependencies do not yet support Python 3.14+.
