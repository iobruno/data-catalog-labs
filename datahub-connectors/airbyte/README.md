# DataHub Connector - Airbyte

[![Airbyte](https://img.shields.io/badge/Airbyte-2.1-007CEE?style=flat&logo=airbyte&logoColor=5F5DFF&labelColor=14193A)](https://docs.airbyte.com/platform/2.0/using-airbyte/getting-started/oss-quickstart)
[![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)](https://docs.docker.com/get-docker/)


## Getting Started

To work on this, you'll need DataHub, Airflow, and Airbyte infrastructure up-and-running:
* [DataHub: Getting Started](../../datahub/README.md#getting-started) 
* [Airflow: Getting Started](../../airflow/README.md#getting-started)
* [Airbyte: Getting Started](../../airbyte/README.md#getting-started)

⚠️ As it stands `airbyte-api>=1.0,<1.1` breaks on: [get_destination](https://github.com/airbytehq/airbyte-api-python-sdk/blob/main/docs/sdks/destinations/README.md#get_destination) and  [list_destinations](https://github.com/airbytehq/airbyte-api-python-sdk/blob/main/docs/sdks/destinations/README.md#list_destinations). So, keep `"airbyte-api>=0.53.0,<1.0"` on [pyproject.toml](./pyproject.toml) dependencies for now.


## DataHub Custom Recipe Ingestion

Install dependencies from pyproject.toml and activate the created virtualenv:
```shell
uv sync && source .venv/bin/activate
```

### Register Airbyte Platform (one-time setup)

```shell
export DATAHUB_GMS_URL=http://localhost:9090
```
```shell
uv run datahub put platform \
  --name airbyte \
  --display_name "Airbyte" \
  --logo "https://cdn.brandfetch.io/id2WO4wLxK/theme/dark/symbol.svg?c=1bxid64Mup7aczewSAYMX&t=1668082116314"
```

### Local Execution

**1.** Fetch your airbyte local credentials (Client-Id and Client-Secret)
```shell
abctl local credentials
```

**2.** Setup the ENV VARs to sink to DataHub:

2.1A. If you're using the `datahub-kafka` on [dbt_datahub.yml](./dbt_datahub.yml), run:
```shell
export DATAHUB_KAFKA_BOOSTRAP_SERVERS=host.docker.internal:9093
export DATAHUB_SCHEMA_REGISTRY_URL=http://host.docker.internal:8081
```

2.1B: Otherwise, if using the `datahub-rest` on [dbt_datahub.yml](./dbt_datahub.yml), run:
```shell
export DATAHUB_REST_SERVER=http://host.docker.internal:9090
```

**3.** Trigger the ingestion pipeline with [recipe.yml](./recipe.yml):
```shell
AIRFLOW_DAG_NAME=<airflow-dag-name> \
AIRFLOW_TASK_NAME=<airflow-dag-task-name>> \
AIRBYTE_CONNECTION_ID=<airbyte-connection-id> \
AIRBYTE_SERVER_URL=http://localhost:8000/api/public/v1/ \
AIRBYTE_CLIENT_ID=<client-id> \
AIRBYTE_CLIENT_SECRET=<client-secret> \
datahub ingest -c recipe.yml
```


### Containerization

**1.** Build the Docker Image with:
```shell
docker build -t datahub-airbyte-ingest:latest . --no-cache
```

**2.** Start a container with it:
```shell
docker run --rm \                                         
    -e AIRFLOW_DAG_NAME=<airflow-dag-name> \
    -e AIRFLOW_TASK_NAME=<airflow-dag-task-name> \
    -e AIRBYTE_CONNECTION_ID=<airbyte-connection-id> \
    -e AIRBYTE_CLIENT_ID=<airbyte-client-id> \
    -e AIRBYTE_CLIENT_SECRET=<airbyte-client-secret> \
    -e AIRBYTE_SERVER_URL=http://host.docker.internal:8000/api/public/v1/ \
    -e DATAHUB_KAFKA_BOOSTRAP_SERVERS=host.docker.internal:9093 \
    -e DATAHUB_SCHEMA_REGISTRY_URL=http://host.docker.internal:8081 \
    --name datahub-ingest-airbyte \
    datahub-airbyte-ingest:latest
```


## TODO:
- [x] Create a custom `recipe.yml` to ingest Airbyte connections as DataJobs
- [x] Build Upstream URNs based on `AIRFLOW_DAG_NAME` and `AIRFLOW_TASK_ID`
- [x] Build an Airbyte Client to fetch downstream details from Airbyte (BigQuery FQN table, Connection URL, Workspace name)
- [x] Upstream and Downstream relationships are reflected accordingly on DataHub
- [x] The DataJob have a link to `View in Airbyte`
- [x] Register Airbyte as a Platform so users can browser by Platform
- [x] Register an SVG logo for Airbyte to uniquely differenciate from other platforms
