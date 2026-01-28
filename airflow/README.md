# Workflow orchestration with Airflow

![Python](https://img.shields.io/badge/Python-3.11-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
[![Airflow](https://img.shields.io/badge/Airflow-2.10-007CEE?style=flat&logo=apacheairflow&logoColor=white&labelColor=14193A)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
[![Pandas](https://img.shields.io/badge/pandas-150458?style=flat&logo=pandas&logoColor=E70488&labelColor=150458)](https://pandas.pydata.org/docs/user_guide/)
[![uv](https://img.shields.io/badge/astral/uv-261230?style=flat&logo=uv&logoColor=DE5FE9&labelColor=261230)](https://docs.astral.sh/uv/getting-started/installation/)
[![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)](https://docs.docker.com/get-docker/)

This sets up the infrastructure for Airflow, in Docker, as close as possible to that of Airflow on Kubernetes (Helm charts).

It also uses the same base image as that available in [GCP Composer for Airflow](https://docs.cloud.google.com/composer/docs/composer-versions).


## Getting Started

**1.** Start setting up the infrastructure in Docker with:
```shell
docker compose up --build -d
```

The default [compose.yaml](./compose.yaml) is a symlink to the **LocalExecutor**. 

Alternatively you can run it with the **CeleryExecutor** with:

```shell
docker compose -f compose.celery.yaml up --build -d
```

**2.** Setup an Airflow Connections to Airbyte:
```txt
Connection Id: airbyte_default
Connection Type: Airbyte
Host: host.docker.internal
Schema: http
Port: 8000
Login:    <Email>
Password: <Password>
```

**3.** Setup a Connection to DataHub:
```txt
Connection Id: datahub_rest_default
Connection Type: DataHub REST Server
Server Endpoint: http://host.docker.internal:9090
Password: <blank>
```


## TODO's:
- [x] PEP-517: Packaging and dependency management with `uv`
- [x] Code format/lint with `ruff`
- [x] Run Airflow DAGs on Docker
