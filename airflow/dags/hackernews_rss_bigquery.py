from datetime import datetime

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HACKERNEWS_RSS_FRONT_CONN_ID = "a69f80b8-53f0-4f3c-b742-f7d064683fb9"
HACKERNEWS_RSS_COMMENTS_CONN_ID = "43e6123d-29b1-4172-9ab9-9430102dbf6a"
HACKERNEWS_RSS_NEWEST_CONN_ID = "e7a66c6d-60bb-4ee0-8cf7-f88dad6dff29"

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

    airbyte_lineage_export = DockerOperator(
        task_id="airbyte_lineage_export",
        image="datahub-ingest:latest",
        container_name="datahub-ingest-airbyte",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        entrypoint="datahub ingest -c recipes/airbyte.yml",
        environment={
            # Airbyte API endpoint - uses /api/public/v1 prefix
            # "AIRBYTE_SERVER_URL": "http://host.docker.internal:8000/api/public/v1",
            # Use credentials from the airbyte_default connection
            "AIRBYTE_API_TOKEN": "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwMDAiLCJhdWQiOiJhaXJieXRlLXNlcnZlciIsInN1YiI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCIsImV4cCI6MTc3MDQyMDMzMywicm9sZXMiOlsiQVVUSEVOVElDQVRFRF9VU0VSIiwiUkVBREVSIiwiRURJVE9SIiwiQURNSU4iLCJPUkdBTklaQVRJT05fTUVNQkVSIiwiT1JHQU5JWkFUSU9OX1JFQURFUiIsIk9SR0FOSVpBVElPTl9SVU5ORVIiLCJPUkdBTklaQVRJT05fRURJVE9SIiwiT1JHQU5JWkFUSU9OX0FETUlOIiwiV09SS1NQQUNFX1JFQURFUiIsIldPUktTUEFDRV9SVU5ORVIiLCJXT1JLU1BBQ0VfRURJVE9SIiwiV09SS1NQQUNFX0FETUlOIiwiREFUQVBMQU5FIl19.4WmoGBK2iQTwn9vMV8BbFlEpQWHNJSFBb09Gdag7VSs",  # short lived token - 15min
            "AIRBYTE_WORKSPACE_ID": "4a36b1d6-4886-4420-a5f1-0fb463d077ab",  # Replace with your workspace id
        },
        mounts=[
            Mount(
                source="/Users/axelfurlan/.config/gcloud/application_default_credentials.json",
                target="/secrets/gcp_credentials.json",
                type="bind",
                read_only=True,
            )
        ],
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
                source="/Users/axelfurlan/.config/gcloud/application_default_credentials.json",
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
        >> airbyte_lineage_export
        >> dbt_execution
    )
