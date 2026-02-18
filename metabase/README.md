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


## Dashboards & Charts

After Metabase is running:
* Configure the connection to BigQuery.  
* Create some Charts and Dashboards using HackerNews datasets


## TODO's
- [x] Bootstrap Metabase infrastructure in Docker
- [x] Build Dashboards and Charts on Metabase
- [x] Integrate with DataHub using the [Metabase source connector](https://docs.datahub.com/docs/generated/ingestion/sources/metabase) 
