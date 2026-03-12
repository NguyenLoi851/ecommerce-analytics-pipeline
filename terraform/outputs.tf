output "raw_bucket_url" {
  value       = "s3://${aws_s3_bucket.raw.bucket}"
  description = "Raw bucket URL"
}

output "curated_bucket_url" {
  value       = "s3://${aws_s3_bucket.curated.bucket}"
  description = "Curated bucket URL"
}

output "logs_bucket_url" {
  value       = "s3://${aws_s3_bucket.logs.bucket}"
  description = "Logs bucket URL"
}

output "raw_bucket_name" {
  value       = aws_s3_bucket.raw.bucket
  description = "Raw bucket name"
}

output "curated_bucket_name" {
  value       = aws_s3_bucket.curated.bucket
  description = "Curated bucket name"
}

output "logs_bucket_name" {
  value       = aws_s3_bucket.logs.bucket
  description = "Logs bucket name"
}
