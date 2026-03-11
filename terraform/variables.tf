variable "gcp_project_id" {
  description = "GCP project ID where buckets will be created"
  type        = string
}

variable "gcp_credentials_file" {
  description = "Path to the GCP service account JSON key file used by Terraform."
  type        = string
}

variable "region" {
  description = "GCP region for bucket location"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment label (e.g., dev, prod)"
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "Name of raw landing bucket"
  type        = string
}

variable "curated_bucket_name" {
  description = "Name of curated bucket"
  type        = string
}

variable "logs_bucket_name" {
  description = "Name of logs bucket"
  type        = string
}

variable "force_destroy" {
  description = "Whether to allow bucket deletion with objects"
  type        = bool
  default     = false
}
