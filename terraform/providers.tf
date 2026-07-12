terraform {
    required_providers {
        airbyte = {
            source = "airbytehq/airbyte"
            version = "1.0.2"
        }
        google = {
            source  = "hashicorp/google"
            version = "7.39.0"
        }
    }

    backend "gcs" {
        bucket = "iobruno-gcp-labs-tfstate"
        prefix = "data-catalog-labs"
    }
}

provider "airbyte" {
    client_id     = var.airbyte_client_id
    client_secret = var.airbyte_client_secret

    server_url = "http://localhost:8000/api/public/v1/"
    token_url = "http://localhost:8000/api/public/v1/applications/token"
}

provider "google" {
    project = var.gcp_project_id
    region  = var.gcp_data_region
}
