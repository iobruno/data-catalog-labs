variable "airbyte_client_id" {
    type = string
    description = "Airbyte Client ID - when running w/ abctl - fetch it with `abctl local credentials`"
}

variable "airbyte_client_secret" { 
    type = string
    description = "Airbyte Client Secret - when running w/ abctl - fetch it with `abctl local credentials`"
}

variable "airbyte_workspace_id" {
    type = string
    description = "Default Airbyte Workspace ID"
}

variable "airbyte_hackernews_rss_sources" {
  type        = map(string)
  description = "Map of RSS source names to URLs"
}

variable "airbyte_hackernews_bq_destination" {
  type  = string
  description = "Destination Name"
}

variable "bq_hackernews_raw_dataset" {
    type    = string
    description = "BigQuery Dataset where HackerNews RSS Feed will be stored in as tables"
}

variable "gcp_project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "gcp_data_region" {
  type        = string
  description = "Region for GCP Resources"
  default     = "us-central1"
}

variable "gcp_credentials_path" {
  type        = string
  sensitive   = true
  description = "Path to GCP service account JSON key file (use $GOOGLE_APPLICATION_CREDENTIALS)"
}