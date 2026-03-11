output "raw_bucket_url" {
  value       = "gs://${google_storage_bucket.raw.name}"
  description = "Raw bucket URL"
}

output "curated_bucket_url" {
  value       = "gs://${google_storage_bucket.curated.name}"
  description = "Curated bucket URL"
}

output "logs_bucket_url" {
  value       = "gs://${google_storage_bucket.logs.name}"
  description = "Logs bucket URL"
}
