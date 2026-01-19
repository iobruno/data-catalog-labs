resource "airbyte_source_rss" "hackernews_rss" {
    for_each = var.airbyte_hackernews_rss_sources

    configuration = {
        url = each.value
    }
    name         = each.key
    workspace_id = var.airbyte_workspace_id
}

resource "airbyte_destination_bigquery" "hackernews_bq" {
  name          = var.airbyte_hackernews_bq_destination
  workspace_id  = var.airbyte_workspace_id

  configuration = {
    dataset_id                      = var.bq_hackernews_raw_dataset
    dataset_location                = var.gcp_data_region
    project_id                      = var.gcp_project_id
    
    credentials_json                = file(var.gcp_credentials_path)
    loading_method                  = {
      batched_standard_inserts      = {}
    }
  }
}

resource "airbyte_connection" "hackernews_rss_to_bigquery" {
  for_each = var.airbyte_hackernews_rss_sources

  source_id      = airbyte_source_rss.hackernews_rss[each.key].source_id
  destination_id = airbyte_destination_bigquery.hackernews_bq.destination_id
  status         = "active"
  prefix         = "${reverse(split("-", each.key))[0]}_"
}

resource "google_bigquery_dataset" "hackernews_raw_dataset" {
  dataset_id = var.bq_hackernews_raw_dataset
  location   = var.gcp_data_region
  delete_contents_on_destroy = true   
}
