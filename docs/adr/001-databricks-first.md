# ADR 001 — Databricks as Core Compute and Orchestration

**Status**: Accepted  
**Date**: 2026  

---

## Context

The pipeline needs compute, orchestration, and governance in a single platform. Options considered:

- **Databricks** on AWS (Spark + SQL Warehouse + Workflows + Unity Catalog)
- **AWS-native** stack (Glue + Step Functions + Athena + Lake Formation)
- **Airflow + Spark** (managed via MWAA or self-hosted)

---

## Decision

Use **Databricks** as the primary compute, orchestration, and governance platform.

---

## Rationale

| Criterion | Databricks | AWS-native | Airflow + Spark |
|---|---|---|---|
| Integrated governance (Unity Catalog) | ✅ Native | ⚠️ Lake Formation (complex) | ❌ External |
| dbt-databricks integration | ✅ First-class | ⚠️ dbt-athena (limited) | ✅ With connector |
| Workflow scheduling (no extra infra) | ✅ Built-in | ✅ Step Functions | ❌ Needs MWAA |
| Delta Lake (ACID, CDC, Z-order) | ✅ Native | ⚠️ Via EMR | ⚠️ Via EMR or Glue |
| Auto Loader (incremental ingest) | ✅ Native | ❌ | ❌ |
| Operational overhead | Low | Medium | High |

---

## Consequences

- All notebooks and workflows are Databricks-specific.
- Airflow is explicitly _optional_ — only introduced if multi-platform orchestration is needed.
- Terraform provisions only S3 buckets and IAM roles; Databricks resources (clusters, warehouses) are managed in the Databricks UI or via the Databricks CLI/API.
