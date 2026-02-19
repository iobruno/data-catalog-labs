from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

hackernews_rss_front_conn_id = Variable.get("hackernews_rss_front_conn_id")
hackernews_rss_comments_conn_id = Variable.get("hackernews_rss_comments_conn_id")
hackernews_rss_newest_conn_id = Variable.get("hackernews_rss_newest_conn_id")
airbyte_conn = BaseHook.get_connection("airbyte_default")

with DAG(
    dag_id="hackernews_rss_bigquery",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["airbyte"],
) as dag:
    hackernews_rss_front = AirbyteTriggerSyncOperator(
        task_id="hackernews_rss_front",
        airbyte_conn_id="airbyte_default",
        connection_id=hackernews_rss_front_conn_id,
        asynchronous=False,
        wait_seconds=3,
        timeout=3600,
    )

    hackernews_rss_newest = AirbyteTriggerSyncOperator(
        task_id="hackernews_rss_newest",
        airbyte_conn_id="airbyte_default",
        connection_id=hackernews_rss_newest_conn_id,
        asynchronous=False,
        wait_seconds=3,
        timeout=3600,
    )

    hackernews_rss_comments = AirbyteTriggerSyncOperator(
        task_id="hackernews_rss_comments",
        airbyte_conn_id="airbyte_default",
        connection_id=hackernews_rss_comments_conn_id,
        asynchronous=False,
        wait_seconds=3,
        timeout=3600,
    )

    dbt_execution = DockerOperator(
        task_id="run_dbt_bigquery",
        image="datahub-dbt-bigquery-ingest:latest",
        container_name="dbt-bigquery-hackernews",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "DBT_BIGQUERY_PROJECT": "iobruno-gcp-labs",
            "DBT_BIGQUERY_SOURCE_DATASET": "hackernews_rss_raw",
            "DBT_BIGQUERY_TARGET_DATASET": "hackernews_rss",
            "DBT_BIGQUERY_DATASET_LOCATION": "us-central1",
            "DATAHUB_KAFKA_BOOSTRAP_SERVERS": "host.docker.internal:9093",
            "DATAHUB_SCHEMA_REGISTRY_URL": "http://host.docker.internal:8081",
        },
        mounts=[
            Mount(
                source="/Users/iobruno/.gcp/gcp_credentials.json",
                target="/secrets/gcp_credentials.json",
                type="bind",
                read_only=True,
            ),
        ],
    )

    airbyte_lineage_hackernews_rss_front = DockerOperator(
        task_id="airbyte_lineage_hackernews_rss_front",
        image="datahub-airbyte-ingest:latest",
        auto_remove="force",
        network_mode="bridge",
        environment={
            "AIRBYTE_CONNECTION_ID": hackernews_rss_front_conn_id,
            "AIRFLOW_TASK_NAME": hackernews_rss_front.task_id,
            "AIRFLOW_DAG_NAME": dag.dag_id,
            "AIRBYTE_SERVER_URL": airbyte_conn.host,
            "AIRBYTE_CLIENT_ID": airbyte_conn.login,
            "AIRBYTE_CLIENT_SECRET": airbyte_conn.password,
            "DATAHUB_KAFKA_BOOSTRAP_SERVERS": "host.docker.internal:9093",
            "DATAHUB_SCHEMA_REGISTRY_URL": "http://host.docker.internal:8081",
        },
    )

    airbyte_lineage_hackernews_rss_newest = DockerOperator(
        task_id="airbyte_lineage_hackernews_rss_newest",
        image="datahub-airbyte-ingest:latest",
        auto_remove="force",
        network_mode="bridge",
        environment={
            "AIRBYTE_CONNECTION_ID": hackernews_rss_newest_conn_id,
            "AIRFLOW_TASK_NAME": hackernews_rss_newest.task_id,
            "AIRFLOW_DAG_NAME": dag.dag_id,
            "AIRBYTE_SERVER_URL": airbyte_conn.host,
            "AIRBYTE_CLIENT_ID": airbyte_conn.login,
            "AIRBYTE_CLIENT_SECRET": airbyte_conn.password,
            "DATAHUB_KAFKA_BOOSTRAP_SERVERS": "host.docker.internal:9093",
            "DATAHUB_SCHEMA_REGISTRY_URL": "http://host.docker.internal:8081",
        },
    )

    airbyte_lineage_hackernews_rss_comments = DockerOperator(
        task_id="airbyte_lineage_hackernews_rss_comments",
        image="datahub-airbyte-ingest",
        auto_remove="force",
        network_mode="bridge",
        environment={
            "AIRBYTE_CONNECTION_ID": hackernews_rss_comments_conn_id,
            "AIRFLOW_TASK_NAME": hackernews_rss_comments.task_id,
            "AIRFLOW_DAG_NAME": dag.dag_id,
            "AIRBYTE_SERVER_URL": airbyte_conn.host,
            "AIRBYTE_CLIENT_ID": airbyte_conn.login,
            "AIRBYTE_CLIENT_SECRET": airbyte_conn.password,
            "DATAHUB_KAFKA_BOOSTRAP_SERVERS": "host.docker.internal:9093",
            "DATAHUB_SCHEMA_REGISTRY_URL": "http://host.docker.internal:8081",
        },
    )

    (
        [hackernews_rss_front, hackernews_rss_newest, hackernews_rss_comments]
        >> dbt_execution
        >> [
            airbyte_lineage_hackernews_rss_front,
            airbyte_lineage_hackernews_rss_newest,
            airbyte_lineage_hackernews_rss_comments,
        ]
    )
