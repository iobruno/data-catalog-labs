from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.docker.operators.docker import DockerOperator

GCS_BUCKET = "iobruno-lakehouse-raw"
GCS_PATH = "sftp-data/taxi_zone_lookup.csv"
SFTP_PATH = "/data/taxi_zone_lookup.csv"

airbyte_conn = BaseHook.get_connection("airbyte_default")
sftp2gcs_to_bigquery_conn_id = Variable.get("sftp2gcs_to_bigquery_conn_id")

with DAG(
    dag_id="sftp2gcs_to_bigquery",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sftp", "gcs"],
) as dag:
    file_upload_to_gcs = SFTPToGCSOperator(
        task_id="file_upload_to_gcs",
        source_path=SFTP_PATH,
        destination_bucket=GCS_BUCKET,
        destination_path=GCS_PATH,
        sftp_conn_id="sftp_default",
        move_object=False,
        inlets=[Dataset(f"sftp:{SFTP_PATH}")],
        outlets=[Dataset(f"gs://{GCS_BUCKET}/{GCS_PATH}")],
    )

    gcs_to_bigquery = AirbyteTriggerSyncOperator(
        task_id="gcs_to_bigquery",
        airbyte_conn_id="airbyte_default",
        connection_id=sftp2gcs_to_bigquery_conn_id,
        asynchronous=False,
        wait_seconds=3,
        timeout=3600,
    )

    airbyte_lineage_gcs_to_bigquery = DockerOperator(
        task_id="airbyte_lineage_gcs_to_bigquery",
        image="datahub-airbyte-ingest:latest",
        auto_remove="force",
        network_mode="bridge",
        environment={
            "AIRBYTE_CONNECTION_ID": sftp2gcs_to_bigquery_conn_id,
            "AIRFLOW_TASK_NAME": gcs_to_bigquery.task_id,
            "AIRFLOW_DAG_NAME": dag.dag_id,
            "AIRBYTE_SERVER_URL": airbyte_conn.host,
            "AIRBYTE_CLIENT_ID": airbyte_conn.login,
            "AIRBYTE_CLIENT_SECRET": airbyte_conn.password,
            "DATAHUB_KAFKA_BOOSTRAP_SERVERS": "host.docker.internal:9093",
            "DATAHUB_SCHEMA_REGISTRY_URL": "http://host.docker.internal:8081",
        },
    )

    file_upload_to_gcs >> gcs_to_bigquery >> airbyte_lineage_gcs_to_bigquery
