# Spark → DataHub OpenLineage

![Python](https://img.shields.io/badge/Python-3.13_|_3.12-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
[![PySpark](https://img.shields.io/badge/PySpark-4.0-262A38?style=flat-square&logo=apachespark&logoColor=E36B22&labelColor=262A38)](https://spark.apache.org/docs/4.0.2/api/python/user_guide/index.html)
[![Scala](https://img.shields.io/badge/Scala-2.13-262A38?style=flat-square&logo=scala&logoColor=E03E3C&labelColor=262A38)](https://sdkman.io/)
[![JDK](https://img.shields.io/badge/JDK-21_|_17-35667C?style=flat&logo=openjdk&logoColor=FFFFFF&labelColor=1D213B)](https://sdkman.io/)
[![Acryl-Spark](https://img.shields.io/badge/acryl--spark--lineage-262A38?style=flat-square&logo=lineageos&logoColor=73A4BC&labelColor=262A38)](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage)
[![uv](https://img.shields.io/badge/astral/uv-261230?style=flat&logo=uv&logoColor=DE5FE9&labelColor=261230)](https://docs.astral.sh/uv/getting-started/installation/)
[![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)](https://docs.docker.com/get-docker/)

![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)


## Getting Started

**1.** Install JDK 21 | 17 (earlier versions are deprecated) for Spark 4.0 with [SDKMan](https://sdkman.io/):
```shell
sdk i java 21.0.10-librca
sdk i java 17.0.18-librca
```

**2.** Install dependencies from pyproject.toml and activate the created virtualenv:
```shell
uv sync && source .venv/bin/activate
```

**3.** (Optional) Install pre-commit:
```shell
brew install pre-commit

# From root folder where `.pre-commit-config.yaml` is located, run:
pre-commit install
```

**4.** Spin up the Spark Cluster with:
```shell
docker compose up -d
```

## TODO's:
- [x] PEP-517: Packaging and dependency management with `uv`
- [ ] Spin up a Spark Cluster in Standalone mode w/ Spark Connect
- [ ] Connect a Notebook to a Spark Cluster via Spark Connect (emit events to DataHub)
- [ ] `spark-submit` a SparkJob to a Standalone Cluster (emit events to DataHub)
