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


## TODO's:
- [x] Single-broker Kafka Cluster (with KRaft)
- [x] Kafka Admin UI: `Conduktor Console`
- [x] Spin-up DataHub using Kafka-Kraft
