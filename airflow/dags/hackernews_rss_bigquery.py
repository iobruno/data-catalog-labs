from datetime import datetime

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HACKERNEWS_RSS_FRONT_CONN_ID = "e37988e6-8ed5-465c-abb2-150639819c62"
HACKERNEWS_RSS_COMMENTS_CONN_ID = "2fda0c2f-6a50-427e-af1b-25050ad40384"
HACKERNEWS_RSS_NEWEST_CONN_ID = "4ca6f367-93f7-4f88-b0fc-b765daf09a28"

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
        connection_id=HACKERNEWS_RSS_FRONT_CONN_ID,
        asynchronous=False,
        wait_seconds=3,
        timeout=3600,
    )

    hackernews_rss_newest = AirbyteTriggerSyncOperator(
        task_id="hackernews_rss_newest",
        airbyte_conn_id="airbyte_default",
        connection_id=HACKERNEWS_RSS_NEWEST_CONN_ID,
        asynchronous=False,
        wait_seconds=3,
        timeout=3600,
    )

    hackernews_rss_comments = AirbyteTriggerSyncOperator(
        task_id="hackernews_rss_comments",
        airbyte_conn_id="airbyte_default",
        connection_id=HACKERNEWS_RSS_COMMENTS_CONN_ID,
        asynchronous=False,
        wait_seconds=3,
        timeout=3600,
    )

    dbt_execution = DockerOperator(
        task_id="run_dbt_bigquery",
        image="dbt-bigquery:latest",
        container_name="dbt-bigquery-hackernews",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "DBT_BIGQUERY_PROJECT": "iobruno-gcp-labs",
            "DBT_BIGQUERY_SOURCE_DATASET": "hackernews_rss_raw",
            "DBT_BIGQUERY_TARGET_DATASET": "hackernews_rss",
            "DBT_BIGQUERY_DATASET_LOCATION": "us-central1",
        },
        mounts=[
            Mount(
                source="/Users/iobruno/.gcp/gcp_credentials.json",
                target="/secrets/gcp_credentials.json",
                type="bind",
                read_only=True,
            ),
            Mount(
                source="vol-dbt-openlineage-artifacts",
                target="/dbt/target/",
                type="volume",
                read_only=False,
            ),
        ],
    )

    (
        [hackernews_rss_front, hackernews_rss_newest, hackernews_rss_comments]
        >> dbt_execution
    )
