# Data Catalog with DataHub

[![Kafka](https://img.shields.io/badge/Confluent_Platform-7.8-141414?style=flat&logo=apachekafka&logoColor=white&labelColor=141414)](https://docs.confluent.io/platform/current/)
[![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)](https://docs.docker.com/get-docker/)


## Getting Started

**1.** Spin up the whole stack with:

```shell
docker compose up -d
```

**2.** Access DataHub Web UI:
```shell
open http://localhost:9002
```

**3.** (Optional) Conduktor Web UI for Kafka
```shell
open http://localhost:9000
```


## DataHub Custom Recipe Ingestion

Install dependencies from pyproject.toml to generate/update uv.lock:
```shell
uv sync && source .venv/bin/activate
```

### BigQuery

**4.1.** Update GCP `project_ids` and the `sink` connection details in [bigquery.yaml](./recipes/bigquery.yaml):
```yaml
project_ids:
  - your-project-id
```

**4.2a** Docker run:
```shell
docker build -t datahub-ingest:latest . --no-cache
```
```shell
❯ docker run --rm \
  -v /PATH/TO/YOUR/gcp_credentials.json:/secrets/gcp_credentials.json \
  datahub-ingest:latest
```

**4.2b.** Run (Local):
```shell
datahub ingest -c recipes/bigquery.yaml
```


## GraphQL Queries

To get a better idea of how the entities are modeles on DataHub as `dataFlow`, `dataJob`, `dataProcessInstance`, `dataSets`, among others,

You can use the following Postman Collection of GraphQL queries:

```
https://www.postman.com/iobruno/workspace/vault/collection/6983fb194d8a7c94d2b82c6b?action=share&creator=52118286
```


## TODO's:
- [x] Single-broker Kafka Cluster (with KRaft)
- [x] Kafka Admin UI: `Conduktor Console`
- [x] Spin-up DataHub using Kafka-Kraft
- [x] Build a Docker Image for ingesting custom recipes (e.g.: dbt-core)
- [x] Create a Repository/Collection of useful GraphQL Queries for debugging
