locals {
  common_labels = {
    project     = "ecommerce-analytics-pipeline"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_storage_bucket" "raw" {
  name                        = var.raw_bucket_name
  location                    = var.region
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  versioning {
    enabled = true
  }

  labels = merge(local.common_labels, {
    zone = "raw"
  })
}

resource "google_storage_bucket" "curated" {
  name                        = var.curated_bucket_name
  location                    = var.region
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  versioning {
    enabled = true
  }

  labels = merge(local.common_labels, {
    zone = "curated"
  })
}

resource "google_storage_bucket" "logs" {
  name                        = var.logs_bucket_name
  location                    = var.region
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  versioning {
    enabled = true
  }

  labels = merge(local.common_labels, {
    zone = "logs"
  })
}
