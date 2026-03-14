# ADR 004 — Incremental Bronze Ingestion via Registry Table

**Status**: Accepted  
**Date**: 2026  

---

## Context

The initial Bronze ingestion notebook performed a full `overwrite` of every table on each run. For tables that rarely change (e.g., `customers`, `products`), this wasted time and compute. Options considered:

1. **Registry table** — track which files have been successfully loaded; skip on re-runs.
2. **Auto Loader (`cloudFiles`)** — Databricks streaming source that checkpoints file progress natively.
3. **Full overwrite** (status quo) — always reload all tables.

---

## Decision

Implement **both** approaches:

- **Auto Loader (`01_bronze_ingestion_autoloader.py`)**: uses `cloudFiles` format with a checkpoint directory in S3. This is the **preferred notebook** for production use.
- **Registry table (`01_bronze_ingestion.py`)**: uses a `_ingestion_registry` Delta table in `dev.bronze` as a simple fallback for environments where Auto Loader state management is complex.

---

## Rationale

### Auto Loader

- Native Databricks file discovery — no manual bookkeeping.
- Checkpoint state stored in S3 (`state/autoloader/olist`) — survives cluster restarts.
- `FORCE_RELOAD=true` widget bypasses state for a full reload.
- Two tables (`geolocation`, `product_category_name_translation`) always use full overwrite because they are small reference tables with no natural primary key for deduplication.

### Registry table (fallback)

- Zero external dependencies — state lives in Delta alongside the data.
- Simple to inspect: `SELECT * FROM dev.bronze._ingestion_registry`.
- Useful when Auto Loader checkpoint state becomes stale or corrupted and needs a clean reset without touching S3.

---

## Consequences

- The `reset_s3_data.py` script includes `--mode state` to purge Auto Loader checkpoint directories independently of raw data.
- `databricks/sql/02_reset_layers.sql` drops the registry table along with all Bronze tables when resetting the layer.
- Both notebooks coexist — `phase4_workflow.json` supports either notebook as the ingestion task.
