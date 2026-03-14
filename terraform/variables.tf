variable "aws_region" {
  description = "AWS region for S3 buckets"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Optional AWS CLI profile name used by Terraform"
  type        = string
  default     = null
}

variable "environment" {
  description = "Environment name (e.g. dev, prod)"
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "S3 bucket name for raw data"
  type        = string
}

variable "force_destroy" {
  description = "Whether to allow destroying non-empty buckets"
  type        = bool
  default     = false
}
