# DataHub Connector - Airbyte

[![Kafka](https://img.shields.io/badge/Confluent_Platform-8.1-141414?style=flat&logo=apachekafka&logoColor=white&labelColor=141414)](https://docs.confluent.io/platform/current/)
[![Airbyte](https://img.shields.io/badge/Airbyte-2.0-007CEE?style=flat&logo=airbyte&logoColor=5F5DFF&labelColor=14193A)](https://docs.airbyte.com/platform/2.0/using-airbyte/getting-started/oss-quickstart)
[![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)](https://docs.docker.com/get-docker/)

## Getting Started

To work on this, you'll need DataHub, Airflow, and Airbyte infrastructure up and running:

**1.** Spin up DataHub if it's not already running - follow [DataHub - Getting Started](../../datahub/README.md#getting-started) 

**2.** Spin up Airflow if it's not already running - follow [Airflow - Getting Started](../../airflow/README.md#getting-started)

**3.** Spin up Airbyte if it's not already running - follow [Airbyte - Getting Started](../../airbyte/README.md#getting-started)


## One-time Setup: Register Airbyte Platform

Airbyte is not a built-in DataHub platform. Register it once so the UI displays a proper name and logo:

```shell
datahub put platform \
  --name airbyte \
  --display_name "Airbyte" \
  --logo "https://cdn.brandfetch.io/id2WO4wLxK/theme/dark/symbol.svg?c=1bxid64Mup7aczewSAYMX&t=1668082116314"
```


## DataHub Custom Recipe Ingestion

### Local Execution

**1.** Install dependencies from pyproject.toml and activate the created virtualenv:
```shell
uv sync && source .venv/bin/activate
```

**2.** Fetch your airbyte local credentials (Client-Id and Client-Secret)
```shell
abctl local credentials
```

**3.** Trigger the ingestion pipeline with [recipe.yml](./recipe.yml):
```shell
AIRBYTE_SERVER_URL=http://localhost:8000/api/public/v1/ \
AIRBYTE_CLIENT_ID=<client-id> \
AIRBYTE_CLIENT_SECRET=<client-secret> \
AIRBYTE_CONNECTION_ID=<airbyte-connection-id> \
AIRFLOW_DAG_NAME=<airflow-dag-name> \
AIRFLOW_TASK_NAME=<airflow-dag-task-name>> \
datahub ingest -c recipe.yml
```


### Containerization

**1.** Build the Docker Image with:
```shell
docker build -t datahub-ingest-airbyte:latest . --no-cache
```

**2.** Start a container with it:
```shell
docker run -d --rm \
    -e AIRBYTE_SERVER_URL=http://host.docker.internal:8000/api/public/v1/ \
    -e AIRBYTE_CLIENT_ID=<client-id> \
    -e AIRBYTE_CLIENT_SECRET=<client-secret> \
    -e AIRBYTE_CONNECTION_ID=<airbyte-connection-id> \
    -e AIRFLOW_DAG_NAME=<airflow-dag-name> \
    -e AIRFLOW_TASK_NAME=<airflow-dag-task-name> \
    --name datahub-ingest-airbyte \
    datahub-ingest-airbyte
```


## TODO:
- [x] Create a custom `recipe.yml` to ingest Airbyte connections as DataJobs
- [x] Build Upstream URNs based on `AIRFLOW_DAG_NAME` and `AIRFLOW_TASK_ID`
- [x] Build an Airbyte Client to fetch downstream details from Airbyte (BigQuery FQN table, Connection URL, Workspace name)
- [x] Upstream and Downstream relationships are reflected accordingly on DataHub
- [x] The DataJob have a link to `View in Airbyte`
- [x] Register Airbyte as a Platform so users can browser by Platform
- [x] Register an SVG logo for Airbyte to uniquely differenciate from other platforms
