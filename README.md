# Data Catalog with DataHub

[![Airbyte](https://img.shields.io/badge/Airbyte-2.1-007CEE?style=flat&logo=airbyte&logoColor=5F5DFF&labelColor=14193A)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
[![Airflow](https://img.shields.io/badge/Airflow-2.11-007CEE?style=flat&logo=apacheairflow&logoColor=white&labelColor=14193A)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
[![dbt](https://img.shields.io/badge/dbt-1.11-262A38?style=flat&labelColor=262A38&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGZpbGw9IiNmZjY5NGIiIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZD0iTTE3LjkgOS4zOGE4IDggMCAwIDAtMy4wNC0zLjEybDEuNzcuODNhMTAgMTAgMCAwIDEgMy43NCAzbDMuMjMtNS45M2EyLjkgMi45IDAgMCAwLS4wNi0yLjk2IDIuNzMgMi43MyAwIDAgMC0zLjU2LS44N0wxNC4xIDMuNTRhNC40IDQuNCAwIDAgMS00LjE4IDBMNC4xOC40MWEyLjkgMi45IDAgMCAwLTIuOTYuMDYgMi43MyAyLjczIDAgMCAwLS44OCAzLjU3TDMuNTYgOS45YTQuNCA0LjQgMCAwIDEgMCA0LjE4TC40MiAxOS44M2EyLjkgMi45IDAgMCAwIC4wOSAzIDIuNzMgMi43MyAwIDAgMCAzLjU0Ljg0bDYuMDYtMy4zYTEwIDEwIDAgMCAxLTMtMy43NmwtLjg0LTEuNzdhOCA4IDAgMCAwIDMuMTIgMy4wNWwxMC41OCA1Ljc4YTIuNzMgMi43MyAwIDAgMCAzLjU1LS44NCAyLjkgMi45IDAgMCAwIC4wOC0zem0zLjM4LTcuNzRhMS4wOSAxLjA5IDAgMSAxIDAgMi4xOCAxLjA5IDEuMDkgMCAwIDEgMC0yLjE4TTIuNzQgMy44MmExLjA5IDEuMDkgMCAxIDEgMC0yLjE4IDEuMDkgMS4wOSAwIDAgMSAwIDIuMThtMCAxOC41NGExLjA5IDEuMDkgMCAxIDEgMC0yLjE4IDEuMDkgMS4wOSAwIDAgMSAwIDIuMThNMTMuMSAxMC45YTIuMTcgMi4xNyAwIDAgMC0yLjE4IDIuMTcgMi4yIDIuMiAwIDAgMCAuNyAxLjYgMi43MiAyLjcyIDAgMSAxIC43Ny01LjM4IDIuNyAyLjcgMCAwIDEgMi4zIDIuMzIgMi4yIDIuMiAwIDAgMC0xLjU5LS43MW04LjE4IDExLjQ1YTEuMDkgMS4wOSAwIDEgMSAwLTIuMTggMS4wOSAxLjA5IDAgMCAxIDAgMi4xOCIvPjwvc3ZnPgo=)](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-262A38?style=flat-square&logo=apachespark&logoColor=E36B22&labelColor=262A38)](https://spark.apache.org/docs/3.5.7/api/python/user_guide/index.html)
[![BigQuery](https://img.shields.io/badge/BigQuery-3772FF?style=flat&logo=googlebigquery&logoColor=white&labelColor=3772FF)](https://console.cloud.google.com/bigquery)

This project aims to provision end-to-end pipeline lineage with Airbyte, Airflow, dbt, BigQuery and DataHub as the Data Catalog/Lineage platform. Also ensuring sibling relationships are not duplicate (e.g: Airbyte destination table for a given source matches the same entity as dbt source table)


## Quick Start:

1. Spin up DataHub
```shell
docker compose -f datahub/compose.yaml up -d
```

2. Spin up Airflow
```shell
docker compose -f airflow/compose.yaml up --build -d
``` 

3. Spin up Airbyte with abctl
```shell
brew tap airbytehq/tap
brew install abctl

abctl local install
```

4. Fetch Airbyte credentials
```shell
abctl local credentials
```

5. Build the datahub-dbt-bigquery-ingest Docker Image
```shell
docker build -t datahub-dbt-bigquery-ingest:latest dbt/ --no-cache
```

6. Build the databahub-airbyte-ingest Docker Image
```shell
docker build -t datahub-airbyte-ingest:latest datahub-connectors/airbyte/ --no-cache
```

7. Build the datahub-bigquery-ingest Docker Image
```shell
docker build -t datahub-bigquery-ingest:latest -f datahub/Dockerfile.bigquery datahub/ --no-cache
```

7. Terraform 
```txt
Follow the instructions on [terraform](./terraform/) for guidelines on how to run/apply
```

8. Update `/etc/hosts` to resolve 'host.docker.internal' to loopback address
```shell
sudo sh -c 'echo "127.0.0.1       host.docker.internal" >> /etc/hosts'
```

## Reference Docs
Refer to the specific project folder on how to start each component individually

- [DataHub](./datahub/README.md)
- [Airflow](./airflow/README.md)
- [Airbyte](./airbyte/README.md)
- [datahub-Airbyte](./datahub-connectors/airbyte/README.md)
- [dbt-bigQuery](./dbt/README.md)
- [Terraform](./terraform/README.md)
