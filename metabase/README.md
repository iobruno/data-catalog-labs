# Data visualization with Metabase

[![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=flat&logo=metabase&logoColor=white&labelColor=509EE3)](https://github.com/metabase/metabase)
[![BigQuery](https://img.shields.io/badge/BigQuery-262A38?style=flat&logo=googlebigquery&logoColor=white&labelColor=3772FF)](https://console.cloud.google.com/bigquery)


## Getting Started

**1.** Start off by spinning Metabase up:
```shell
docker compose up -d
```

**2.** Metabase Initial Setup

After the `metabase` container is in a healthy state, `metabase-setup` automatically creates the admin user (skipping the UI wizard).

You can access [Metabase at http://localhost:3000/](http://localhost:3000/)
```txt
Email: admin@metabase.local
Password: admin
```

**3.** Setup the Connection to BigQuery:

3.1. Access [Metabase Admin > Add database](http://localhost:3000/admin/databases)

3.2. Fill in the database credentials with:
```txt
Database type: BigQuery
Connection string: <empty>

Display name: "bigquery-hackernews"
Project ID: <empty>
Service account JSON file: [GOOGLE_APPLICATION_CREDENTIALS]

Datasets: "Only these"
Comma separated names(...): "hackernews_rss"
```

**4.** Set up an API_KEY for programmatic access

4.1. Access [Metabase Admin > Authentication > API keys](http://localhost:3000/admin/settings/authentication/api-keys)

4.2. Create API Key
```txt
Key name: datahub-metabase-ingest
Which group(...): Administrators
```

Copy and save the API key


## Dashboards & Charts

Export the ENV_VARS:
```shell
export METABASE_API_KEY=<metabase-api-key>
export METABASE_URL="http://localhost:3000"
```

Run the ingestion script:
```shell
uv run metabase_init.py
```


## TODO's
- [x] Bootstrap Metabase infrastructure in Docker
- [x] Build Dashboards and Charts on Metabase
- [x] Integrate with DataHub using the [Metabase source connector](https://docs.datahub.com/docs/generated/ingestion/sources/metabase) 
- [x] Generate Metabase charts and Dashboards programmatically for reproducibility
