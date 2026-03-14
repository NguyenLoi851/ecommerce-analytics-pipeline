# ADR 003 — SCD Type 2: Snapshot vs MERGE

**Status**: Accepted  
**Date**: 2026  

---

## Context

The `dim_customers` and `dim_products` dimensions require historical tracking (slow-changing dimension Type 2) to answer questions like "what was this customer's city when they placed this order?"

Two implementation approaches were prototyped in parallel:

1. **dbt snapshots** — dbt's built-in `snapshot` materialisation.
2. **Databricks MERGE** — custom `pre-hook` / `post-hook` running a `MERGE INTO` statement via a dbt macro.

---

## Decision

Use **dbt snapshots** as the primary SCD2 path. The Databricks MERGE implementation is kept as an optional parallel model for reference and future migration.

---

## Rationale

| Criterion | dbt Snapshots | Databricks MERGE |
|---|---|---|
| Operational complexity | Low — `dbt snapshot` handles history automatically | High — custom macro, hook ordering, MERGE logic |
| Correctness | High — built-in `check` / `timestamp` strategies | Requires careful MERGE condition design |
| Debuggability | Good — snapshot table is inspectable | Harder to trace issues |
| Portability | Portable to any dbt adapter | Databricks-specific SQL |
| Performance at scale | Suitable for this dataset size | More efficient for very large tables |
| Breaking existing models | None — snapshot is a separate target | None |

---

## Consequences

- `dim_customers` and `dim_products` remain unchanged; SCD2 models are additive (`_scd2` suffix).
- `dbt snapshot` must be run separately before `dbt run` for SCD2 models to reflect new history.
- The MERGE-based models (`_scd2_merge`) are available for teams that prefer native Databricks SQL and can be promoted to the primary path if scale demands it.
- `valid_from`, `valid_to`, `is_current` are the standard columns exposed by both approaches.
