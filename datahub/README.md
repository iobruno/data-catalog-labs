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

**4.1.** Update GCP `project_ids` in [bigquery.yaml](./recipes/bigquery.yaml):
```yaml
project_ids:
  - your-project-id
```

**4.2.** Run:
```shell
datahub ingest -c recipes/bigquery.yaml
```


### dbt (via Docker - depends on `dbt-bigquery` )

**5.1.** Build the Docker Image for the recipe ingestion (used for dbt-core) as it'll be used by Airflow:
```shell
docker build -t datahub-ingest:latest . --no-cache
```

**5.2.** Then, trigger an execution with:
```shell
docker run --rm \
  -v vol-dbt-openlineage-artifacts:/datahub/dbt-openlineage-artifacts/ \
  --name datahub-ingest \
  datahub-ingest
``` 

**IMPORTANT**: The Docker volume `vol-dbt-openlineage-artifacts` is created during the [dbt-bigquery](../dbt/) execution. Follow the [dbt/README.md - Containerization](../dbt/README.md#containerization) for details


## TODO's:
- [x] Single-broker Kafka Cluster (with KRaft)
- [x] Kafka Admin UI: `Conduktor Console`
- [x] Spin-up DataHub using Kafka-Kraft
- [x] Build a Docker Image for ingesting custom recipes (e.g.: dbt-core)
