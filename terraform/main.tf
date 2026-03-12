locals {
  common_tags = {
    project     = "ecommerce-analytics-pipeline"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "aws_s3_bucket" "raw" {
  bucket        = var.raw_bucket_name
  force_destroy = var.force_destroy

  tags = merge(local.common_tags, {
    zone = "raw"
  })
}

resource "aws_s3_bucket" "curated" {
  bucket        = var.curated_bucket_name
  force_destroy = var.force_destroy

  tags = merge(local.common_tags, {
    zone = "curated"
  })
}

resource "aws_s3_bucket" "logs" {
  bucket        = var.logs_bucket_name
  force_destroy = var.force_destroy

  tags = merge(local.common_tags, {
    zone = "logs"
  })
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "curated" {
  bucket = aws_s3_bucket.curated.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "logs" {
  bucket = aws_s3_bucket.logs.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket = aws_s3_bucket.curated.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket = aws_s3_bucket.logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
