# Deployment Strategy — dev → prod

This document describes how changes move from development to production in the Olist analytics pipeline.

---

## Environment Overview

| Environment | dbt Target | Unity Catalog  | Trigger               | Approvals Required |
|-------------|-----------|----------------|-----------------------|--------------------|
| `dev`       | `dev`     | `dev.*`         | Any PR or local run   | None               |
| `prod`      | `prod`    | `prod.*`        | Merge to `main` + CI pass + manual approval | 1 reviewer |

---

## Branch Strategy

```
feature/<ticket>   ─►  main (PR review + dbt CI)  ─►  prod deploy (manual approval)
hotfix/<ticket>    ─►  main (PR review + dbt CI)  ─►  prod deploy (expedited)
```

- **Feature branches**: all development work lives on `feature/*` branches.
- **Pull requests** to `main` trigger the `dbt-validate` + `dbt-test` CI jobs.
- **Merges to `main`** auto-trigger the `dbt-promote-prod` job after a GitHub Environment approval.
- **Direct commits to `main` are blocked** — enforce this via branch protection rules.

---

## GitHub Repository Settings (apply once)

1. **Branch protection on `main`**:
   - Require PR before merging.
   - Require status checks: `dbt-validate`, `dbt-test`.
   - Require at least 1 approving review.
   - Restrict who can push to `main`.

2. **GitHub Environment: `production`**:
   - Go to **Settings → Environments → New environment** → name it `production`.
   - Add **Required reviewers** (yourself or a teammate).
   - This gates the `dbt-promote-prod` job until a reviewer approves.

3. **GitHub Secrets** (Settings → Secrets → Actions):

   | Secret name                  | Description                              |
   |------------------------------|------------------------------------------|
   | `DATABRICKS_HOST`            | Dev workspace URL (no `https://`)        |
   | `DATABRICKS_HTTP_PATH`       | Dev SQL Warehouse HTTP path              |
   | `DATABRICKS_TOKEN`           | Dev PAT or service-principal token       |
   | `DATABRICKS_HOST_PROD`       | Prod workspace URL                       |
   | `DATABRICKS_HTTP_PATH_PROD`  | Prod SQL Warehouse HTTP path             |
   | `DATABRICKS_TOKEN_PROD`      | Prod PAT or service-principal token      |

---

## CI Gates (per PR)

```
PR opened / pushed
      │
      ▼
[dbt-validate]  ← dbt deps + dbt parse (no cluster)
      │  pass
      ▼
[dbt-test]      ← dbt debug + dbt test --select staging silver gold (dev warehouse)
      │  pass
      ▼
PR can be merged
```

---

## Prod Deployment Flow

```
Merge to main
      │
      ▼
[dbt-validate] (re-runs on main)
      │  pass
      ▼
[dbt-test] (re-runs on main against dev)
      │  pass
      ▼
[dbt-promote-prod]  ← waits for manual approval in GitHub UI
      │  approved
      ▼
dbt run  --select staging silver gold --target prod
dbt test --select staging silver gold --target prod
```

After prod dbt succeeds, the Databricks **Production Workflow** (`olist-production-pipeline`) picks up on its own schedule (daily at 03:00 UTC) — or can be triggered manually via the Databricks UI / REST API.

---

## Hotfix Procedure

For urgent prod fixes:

1. Branch from `main`: `git checkout -b hotfix/<short-description>`.
2. Make the minimal necessary change.
3. Open PR → get 1 review → CI must pass.
4. Merge → prod deploy follows the same approval gate, but reviewers should expedite.

---

## dbt Variable Overrides per Environment

The `dbt_project.yml` uses the `catalog` var to route models to the right Unity Catalog:

```bash
# dev (default)
dbt run --target dev

# prod explicit
dbt run --target prod --vars '{"catalog": "prod"}'
```

The `profiles.yml` already sets `catalog: dev` for the `dev` output and `catalog: prod` for the `prod` output, so the `--vars` override is only needed when overriding the default.

---

## Rollback Procedure

dbt writes to Delta tables — rollback is via Delta time-travel:

```sql
-- Inspect history
DESCRIBE HISTORY prod.silver.fct_orders;

-- Restore to a previous version
RESTORE TABLE prod.silver.fct_orders TO VERSION AS OF <version_number>;

-- Or restore to a timestamp
RESTORE TABLE prod.silver.fct_orders TO TIMESTAMP AS OF '2026-03-12 03:00:00';
```

For a full-layer rollback:

```bash
# Re-run the previous tag
git checkout <previous-tag>
dbt run --target prod --select staging silver gold --profiles-dir dbt
dbt test --target prod --profiles-dir dbt
```

---

## Monitoring Post-Deploy Checklist

After every prod deployment:

- [ ] Databricks Workflow last run status: **Succeeded**.
- [ ] `dbt test` results: 0 failures.
- [ ] Row counts in `prod.gold.mart_sales_daily` match T-1 expectations.
- [ ] `mart_delivery_sla` on-time rate within expected range (>85 %).
- [ ] No `ERROR` entries in the Databricks job event log.
