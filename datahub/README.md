# Data Catalog with DataHub

[![Kafka](https://img.shields.io/badge/Confluent_Platform-8.1-141414?style=flat&logo=apachekafka&logoColor=white&labelColor=141414)](https://docs.confluent.io/platform/current/)
[![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)](https://docs.docker.com/get-docker/)

This setup replicates a production Kubernetes Helm deployment using Docker Compose.

![DataHub Architecture](../.assets/datahub-architecture.svg)

### Core Services

| Service                  | Purpose |
|--------------------------|---|
| **datahub-gms**          | Generalized Metadata Service. The central API server that handles all metadata CRUD operations, search, and graph queries |
| **datahub-frontend**     | React-based Web UI for browsing metadata, lineage, and managing the data catalog |
| **datahub-mae-consumer** | Metadata Change Log consumer. Processes committed metadata changes and updates secondary indexes (OpenSearch) to keep search and graph in sync with the primary database |
| **datahub-mce-consumer** | Metadata Change Proposal consumer. Processes inbound ingestion proposals from Kafka and writes them to the primary database (PostgreSQL) |

### Infrastructure Dependencies

| Service | Purpose |
|---|---|
| **datahub-postgres** | PostgreSQL as the primary metadata storage backend |
| **datahub-opensearch** | OpenSearch 2.x for full-text search and graph traversal (`GRAPH_SERVICE_IMPL=elasticsearch`) |
| **datahub-kafka-broker0** | Kafka broker (KRaft mode, no ZooKeeper) used as the event streaming backbone for metadata change events (MCP/MCL) |
| **datahub-schema-registry** | Confluent Schema Registry for Avro schema management of Kafka topics |

### Init Containers (run once, then exit)

| Service | Purpose |
|---|---|
| **datahub-postgres-init** | Initializes the PostgreSQL database schema |
| **datahub-opensearch-init** | Creates and configures OpenSearch indices and mappings |
| **datahub-kafka-init** | Pre-creates required Kafka topics |
| **datahub-upgrade** | Runs `-u SystemUpdate` to apply database schema migrations, OpenSearch index mappings, bootstrap configs, and BrowsePathsV2 backfills. Must run on every version change before GMS starts. On repeated runs of the same version it's effectively a no-op and exits quickly |

### Monitoring (Conduktor)

| Service | Purpose |
|---|---|
| **conduktor-console** | Web UI for managing and monitoring the Kafka cluster |
| **conduktor-metastore** | PostgreSQL instance used internally by Conduktor |
| **conduktor-cortex** | Monitoring backend for Conduktor (Cortex + Alertmanager + Prometheus APIs) |


## Getting Started

**1.** Spin up the whole stack with:

```shell
docker compose up -d
```

**2.** Access [DataHub Web UI on http://localhost:9002](http://localhost:9002)

**3.** (Optional) Access [Conduktor Web UI for Kafka on http://localhost:9000](http://localhost:9000)


## dbt OpenLineage Ingestion

**4.1.** Install dependencies from pyproject.toml to generate/update uv.lock:
```shell
uv sync && source .venv/bin/activate
```

**4.2.** Build the Docker Image for the recipe ingestion (used for dbt-core) as it'll be used by Airflow:
```shell
docker build -t datahub-ingest:latest . --no-cache
```

**4.3.** Then, trigger an execution with:
```shell
docker run --rm \
  -v vol-dbt-openlineage-artifacts:/datahub/dbt-openlineage-artifacts/ \
  --name datahub-ingest \
  datahub-ingest
``` 

**IMPORTANT**: The volume `vol-dbt-openlineage-artifacts` is created when manually executing the [dbt run via Docker execution](../dbt/) or through Airflow DAG execution


## TODO's:
- [x] Single-broker Kafka Cluster (with KRaft)
- [x] Kafka Admin UI: `Conduktor Console`
- [x] Spin-up DataHub using Kafka-Kraft
- [x] Build a Docker Image for ingesting custom recipes (e.g.: dbt-core)
