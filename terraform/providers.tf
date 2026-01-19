terraform {
    required_providers {
        airbyte = {
            source = "airbytehq/airbyte"
            version = "0.6.5"
        }
        google = {
            source  = "hashicorp/google"
            version = "7.17.0"
        }
    }
}

provider "airbyte" {
    client_id     = var.airbyte_client_id
    client_secret = var.airbyte_client_secret

    server_url = "http://localhost:8000/api/public/v1/"
}

provider "google" {
    project = var.gcp_project_id
    region  = var.gcp_data_region
}