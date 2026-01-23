# Data Catalog with DataHub

[![Airbyte](https://img.shields.io/badge/Airbyte-2.0.19-007CEE?style=flat&logo=airbyte&logoColor=5F5DFF&labelColor=14193A)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
[![Airflow](https://img.shields.io/badge/Airflow-2.10-007CEE?style=flat&logo=apacheairflow&logoColor=white&labelColor=14193A)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
[![dbt](https://img.shields.io/badge/dbt-1.11-262A38?style=flat&logo=dbt&logoColor=FF6849&labelColor=262A38)](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)
[![BigQuery](https://img.shields.io/badge/BigQuery-3772FF?style=flat&logo=googlebigquery&logoColor=white&labelColor=3772FF)](https://console.cloud.google.com/bigquery)

This project aims to provision end-to-end pipeline lineage with Airbyte, Airflow, dbt, BigQuery and DataHub as the Data Catalog/Lineage platform. Also ensuring sibling relationships are not duplicate (e.g: Airbyte destination table for a given source matches the same entity as dbt source table)


## Quick Start:

1. Spin up DataHub
```shell
docker compose -f datahub/compose.yaml up -d
```

2. Spin up Airflow
```shell
docker compose -f airflow/compose.yaml up --build --force-recreate -d
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

5. Build the dbt-bigquery Docker Image
```shell
docker build -t dbt-bigquery:latest dbt/ --no-cache
```

6. Build the datahub-ingest Docker Image
```shell
docker build -t datahub-ingest:latest datahub/ --no-cache
```

7. Terraform 

Follow the instructions on [terraform](./terraform/) for guidelines on how to run/apply


## Reference Docs
Refer to the specific project folder on how to start each component individually

- [DataHub](datahub/README.md)
- [Airflow](airflow/README.md)
- [Airbyte](airbyte/README.md)
- [dbt-bigQuery](dbt/README.md)
- [Terraform](terraform/REA)
