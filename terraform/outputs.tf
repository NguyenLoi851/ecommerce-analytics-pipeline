output "raw_bucket_url" {
  value       = "s3://${aws_s3_bucket.raw.bucket}"
  description = "Raw bucket URL"
}

output "raw_bucket_name" {
  value       = aws_s3_bucket.raw.bucket
  description = "Raw bucket name"
}
