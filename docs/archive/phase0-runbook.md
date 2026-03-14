# Phase 0 Runbook (Databricks + AWS + GitHub)

This runbook is the executable guide for Phase 0 foundation.

## Prerequisites

- AWS account and IAM permissions for S3 + IAM role management.
- Databricks workspace on AWS with Unity Catalog enabled.
- Workspace admin access in Databricks.
- GitHub account and repository access.

## Step 1 — Create S3 Bucket (Terraform)

Use Terraform in [terraform/](../terraform/) as the default provisioning method.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` values:
- `aws_region`
- `aws_profile` (optional; remove or set `null` if you use env credentials)
- `raw_bucket_name`

Then run:

```bash
terraform init
terraform plan
terraform apply
```

## Step 2 — Unity Catalog + Schemas

Run [databricks/sql/00_unity_catalog_setup.sql](../databricks/sql/00_unity_catalog_setup.sql) in Databricks SQL Editor.

Expected result:
- `dev` and `prod` catalogs created.
- `bronze`, `silver`, `gold` schemas created under both.

## Step 3 — Storage Credential + External Location

1. Create a new IAM role in AWS with full access to S3.
2. Create a new Storage Credential in Databricks (through the Catalog tab). Follow the instructions provided to update the policy of the IAM role in AWS.

Update placeholders in [databricks/sql/01_external_location_s3.sql](../databricks/sql/01_external_location_s3.sql):
- `<databricks-s3-access-role-arn>`
- `<your-raw-bucket>`

Then run it in Databricks SQL Editor.

## Step 4 — GitHub Integration + Repos

In Databricks UI:
1. User Settings -> Linked accounts -> GitHub
2. Connect PAT/GitHub App
3. Repos -> Add Repo -> this repository URL

## Step 5 — Phase 0 Smoke Test

Run notebook [databricks/notebooks/00_phase0_smoke_test.py](../databricks/notebooks/00_phase0_smoke_test.py).

Success criteria:
- Table `dev.bronze.phase0_smoke_test` exists.
- Insert/select runs successfully.
- Optional S3 CSV read works when bucket/path is set.

## Step 6 — Definition of Done for Phase 0

- Unity Catalog and schemas exist in `dev` and `prod`.
- S3 external locations are configured and accessible.
- GitHub repository is connected in Databricks Repos.
- Smoke test notebook passes.
