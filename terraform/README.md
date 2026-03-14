# Terraform: AWS S3 Bucket

This folder provisions the S3 bucket used by the data pipeline:
- raw bucket

## Prerequisites

- Terraform >= 1.5
- AWS CLI installed
- IAM permissions to create and manage S3 buckets

## Recommended: create a dedicated AWS profile for this project

1) Pick a profile name for this repo (example: `ecommerce-dev`)

2) Configure that profile with credentials for the project AWS account

Option A — access key:

```bash
aws configure --profile ecommerce-dev
```

Values you need to enter for this command:
- `AWS Access Key ID` -> from IAM user/role credentials in the project AWS account
- `AWS Secret Access Key` -> pair for the access key above
- `Default region name` -> example `us-east-1` (must match `aws_region` in `terraform.tfvars`)
- `Default output format` -> `json`

Example prompt/output flow:

```text
AWS Access Key ID [None]: AKIA...
AWS Secret Access Key [None]: ********
Default region name [None]: us-east-1
Default output format [None]: json
```

Option B — SSO:

```bash
aws configure sso --profile ecommerce-dev
aws sso login --profile ecommerce-dev
```

Values you need for SSO setup:
- `SSO start URL` (from your organization IAM Identity Center portal)
- `SSO region` (where IAM Identity Center is configured, e.g. `us-east-1`)
- `SSO account ID` (project AWS account ID)
- `SSO role name` (role granted to you in that account)
- `Default CLI region` (example `us-east-1`)
- `CLI default output format` (`json`)

3) Verify it points to the correct account:

```bash
aws sts get-caller-identity --profile ecommerce-dev
```

4) Set Terraform to use this profile in `terraform.tfvars`:

```hcl
aws_region          = "us-east-1"
aws_profile         = "ecommerce-dev"
environment         = "dev"

raw_bucket_name     = "your-olist-raw-bucket"

force_destroy       = false
```

5) Run Terraform from this folder:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# update aws_region, aws_profile and raw_bucket_name in terraform.tfvars
terraform init
terraform plan
terraform apply
```

## Common issues

- `The config profile could not be found`
  - profile name is wrong; check with `aws configure list-profiles`
- `ExpiredToken` or SSO auth error
  - run `aws sso login --profile ecommerce-dev` again
- `AccessDenied` on S3 operations
  - your IAM user/role is missing S3 permissions in this AWS account
- STS returns unexpected account
  - you are using the wrong profile; pass `--profile ecommerce-dev` explicitly

## Notes

- Bucket names must be globally unique in AWS.
- Keep `force_destroy = false` for safety in non-ephemeral environments.
- After apply, use the raw bucket URL in Databricks external location SQL setup.
