variable "credential" {
  description = "GCP crediential file path"
  default     = "myServiceKeyPath"
}

variable "gcp_project_id" {
  default     = "ProjectId"
}

variable "gcp_bucket_name" {
  description = "Cloud Storage Bucket Name (Globally Unique)"
  default     = "gcp-de-bikes-project"
}

variable "location" {
  default = "US"
}

variable "bq_dataset_id" {
  description = "Big Query Dataset id (Globally Unique)"
  default     = "pa_shared_bikes"
}