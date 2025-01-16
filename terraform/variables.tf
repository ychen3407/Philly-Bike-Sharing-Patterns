variable "credential" {
  description = "GCP crediential file path"
  default     = "C:/Users/Febru/Desktop/de_project/my-cred.json"
}

variable "gcp_project_id" {
  default     = "essential-wharf-443315-p3"
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

