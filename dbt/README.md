# dbt and BigQuery for Analytics

![Python](https://img.shields.io/badge/Python-3.13_|_3.12-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
[![BigQuery](https://img.shields.io/badge/BigQuery-3772FF?style=flat&logo=googlebigquery&logoColor=white&labelColor=3772FF)](https://console.cloud.google.com/bigquery)
[![dbt](https://img.shields.io/badge/dbt-1.11-262A38?style=flat&logo=dbt&logoColor=FF6849&labelColor=262A38)](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)
[![uv](https://img.shields.io/badge/astral/uv-261230?style=flat&logo=uv&logoColor=DE5FE9&labelColor=261230)](https://docs.astral.sh/uv/getting-started/installation/)


## Getting Started

**1.** Install dependencies from pyproject.toml and activate the created virtualenv:
```shell
uv sync && source .venv/bin/activate
```

**2.** (Optional) Install pre-commit:
```shell
brew install pre-commit

# From root folder where `.pre-commit-config.yaml` is located, run:
pre-commit install
```

**3.** Setup dbt profiles.yaml accordingly (use the `profiles.tmpl.yaml` as template)

3.1. By default, the profiles_dir is the user '$HOME/.dbt/'
```shell
mkdir -p ~/.dbt/
cat profiles.tmpl.yml >> ~/.dbt/profiles.yml
```

3.2. Set the environment variables for `dbt-bigquery`:
```shell
export DBT_BIGQUERY_PROJECT=iobruno-gcp-labs
export DBT_BIGQUERY_SOURCE_DATASET=hackernews_rss_raw
export DBT_BIGQUERY_TARGET_DATASET=hackernews_rss
export DBT_BIGQUERY_DATASET_LOCATION=us-central1
```

3.3. Since we're doing `oauth` authentication for our Development env, run:
```shell
gcloud auth login
```

**4.** Install dbt dependencies and trigger the pipeline

4.1. Run `dbt deps` to install  dbt plugins
```shell
dbt deps
```

4.2. Run dbt run to trigger the dbt models to run
```shell
dbt build --target [prod|dev]

# Alternatively you can run only a subset of the models with:

## +models/staging: Runs the dependencies/preceding models first that lead 
## to 'models/staging', and then the target models
dbt build --target [prod|dev] --select +models/staging

## models/staging+: Runs the target models first, and then all models that depend on it
dbt build --target [prod|dev] --select models/staging+
```

**5.** (Optional) If you're ingesting dbt-metadata to DataHub,

5.1A. If you're using the `datahub-kafka` on [dbt_datahub.yml](./dbt_datahub.yml), run:
```shell
export DATAHUB_KAFKA_BOOSTRAP_SERVERS=host.docker.internal:9093
export DATAHUB_SCHEMA_REGISTRY_URL=http://host.docker.internal:8081
```

5.1B: Otherwise, if using the `datahub-rest` on [dbt_datahub.yml](./dbt_datahub.yml), run:
```shell
export DATAHUB_REST_SERVER=http://host.docker.internal:9090
```

5.2. Make sure to backup `target/run_results.json`, as they're overwritten by `dbt docs generate`:
```shell
cp target/run_results.json target/run_results_backup.json
```

**6.** dbt Metadata and Lineage

6.1. Generate the docs with (overrides `target/run_results.json`):
```shell
dbt docs generate --target [prod|dev]
```

6.2. (Optional) You can see the generated artifacts at [http://localhost:8080](http://localhost:8080) with:
```shell
dbt docs serve [--port=<port>]
```

**7.** Data Catalog ingest

7.1. Recover the dbt executions (`runs`):
```shell
cp target/run_results_backup.json target/run_results.json
```

7.2. Ingest into DataHub:
```shell
datahub ingest -c dbt_datahub.yml
```


## Containerization

**1.** Build the Docker Image with:

```shell
docker build -t datahub-dbt-bigquery-ingest:latest . --no-cache
```

**2.** Start a container with it:
```shell
docker run --rm \
  -e DATAHUB_REST_SERVER=http://host.docker.internal:9090 \
  -e DATAHUB_KAFKA_BOOSTRAP_SERVERS=host.docker.internal:9093 \
  -e DATAHUB_SCHEMA_REGISTRY_URL=http://host.docker.internal:8081 \
  -e DBT_BIGQUERY_PROJECT=iobruno-gcp-labs \
  -e DBT_BIGQUERY_SOURCE_DATASET=hackernews_rss_raw \
  -e DBT_BIGQUERY_TARGET_DATASET=hackernews_rss \
  -e DBT_BIGQUERY_DATASET_LOCATION=us-central1 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:/secrets/gcp_credentials.json \
  datahub-dbt-bigquery-ingest:latest
```


## TODO's:
- [x] PEP-517: Packaging and dependency management with `uv`
- [x] Bootstrap dbt with BigQuery Adapter ([dbt-bigquery](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup))
- [x] Generate and serve docs and Data Lineage Graphs locally
- [x] Add dbt macro to configure target schemas dinamically
- [x] Run `dbt-core` in Docker
- [x] Verify the Metadata and Data Lineage for dbt and BigQuery tables on DataHub
