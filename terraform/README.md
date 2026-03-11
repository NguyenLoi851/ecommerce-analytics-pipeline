# Terraform: GCS Buckets

This folder provisions GCS buckets for the data pipeline:
- raw bucket
- curated bucket
- logs bucket

## Prerequisites

- Terraform >= 1.5
- Service account key JSON file (recommended for this project)
- IAM permissions to create and manage storage buckets

## Usage

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# update gcp_credentials_file and other values in terraform.tfvars
terraform init
terraform plan
terraform apply
```

## Notes

- Bucket names must be globally unique.
- Keep `force_destroy = false` for safety in non-ephemeral environments.
- After apply, use output bucket URLs in Databricks external location SQL setup.
