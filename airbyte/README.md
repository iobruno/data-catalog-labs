# Airbyte OSS

[![Airbyte](https://img.shields.io/badge/Airbyte-2.1-007CEE?style=flat&logo=airbyte&logoColor=5F5DFF&labelColor=14193A)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
[![Docker](https://img.shields.io/badge/Kubernetes-316CE6?style=flat&logo=kubernetes&logoColor=white&labelColor=316CE6)](https://docs.docker.com/get-docker/)
[![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)](https://docs.docker.com/get-docker/)

Local deployments with `docker-compose` was [officially deprecated as of August 2024 and is no longer supported](https://github.com/airbytehq/airbyte/discussions/40599)

Airbyte now uses `abctl` - a CLI tool that deploys locally with [KinD](https://kind.sigs.k8s.io/).


## Getting started

### Bootstraping Airbyte

1. Install `abctl` using one of the [recommended methods](https://docs.airbyte.com/platform/deploying-airbyte/abctl):
```shell
brew tap airbytehq/tap
brew install abctl
```

2. Execute:
```shell
abctl local install
```

3. Get your credentials with:
```shell
abctl local credentials
```

**4.** Airbyte WebUI can be accessed at:
```shell
open http://localhost:8000
```


## Custom Connectors

- T.B.D.


## TODO
- [x] Spin up Airbyte on Docker (KinD)
- [x] Configure native Airbyte Connectors with Terraform/OpenTofu
- [ ] Write a Custom Source connector for a REST API (HackerNews)
