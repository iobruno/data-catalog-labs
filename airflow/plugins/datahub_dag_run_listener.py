"""
Custom Airflow listener to emit DAG-level execution events to DataHub.

The acryl-datahub-airflow-plugin only emits task-level execution events.
This plugin fills the gap by emitting DataFlow run start/completion events.

See: https://github.com/datahub-project/datahub/blob/master/metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/airflow2/datahub_listener.py#L1068
"""

import logging
from os import getenv
from typing import ClassVar

from airflow.listeners import hookimpl
from airflow.models import DagRun
from airflow.plugins_manager import AirflowPlugin
from datahub_airflow_plugin._config import DatahubLineageConfig, get_lineage_config
from datahub_airflow_plugin.client.airflow_generator import AirflowGenerator

logger = logging.getLogger(__name__)


class DataHubDagRunListener:
    """Listener that emits DataFlow execution events to DataHub."""

    def __init__(self, config: DatahubLineageConfig):
        self._config = config
        self._emitter = config.make_emitter_hook().make_emitter()
        logger.info(f"DataHub DAG run listener using {self._emitter!r}")

    def _should_process(self, dag_id: str) -> bool:
        """Check if this DAG should be processed."""
        if not self._config.capture_executions:
            return False
        return self._config.dag_filter_pattern.allowed(dag_id)

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str) -> None:
        """Emit DataFlow run START event when DAG run begins."""
        if not dag_run.dag_id or not self._should_process(dag_run.dag_id):
            return

        try:
            logger.info(f"Emitting DataHub DataFlow run start for {dag_run.dag_id}")
            AirflowGenerator.run_dataflow(
                emitter=self._emitter,
                config=self._config,
                dag_run=dag_run,
            )
            self._emitter.flush()
        except Exception as e:
            logger.warning(f"Failed to emit DataFlow run start: {e}", exc_info=True)

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str) -> None:
        """Emit DataFlow run COMPLETION event with SUCCESS status."""
        if not dag_run.dag_id or not self._should_process(dag_run.dag_id):
            return

        try:
            logger.info(f"Emitting DataHub DataFlow run success for {dag_run.dag_id}")
            AirflowGenerator.complete_dataflow(
                emitter=self._emitter,
                config=self._config,
                dag_run=dag_run,
            )
            self._emitter.flush()
        except Exception as e:
            logger.warning(f"Failed to emit DataFlow run success: {e}", exc_info=True)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str) -> None:
        """Emit DataFlow run COMPLETION event with FAILURE status."""
        if not dag_run.dag_id or not self._should_process(dag_run.dag_id):
            return

        try:
            logger.info(f"Emitting DataHub DataFlow run failure for {dag_run.dag_id}")
            AirflowGenerator.complete_dataflow(
                emitter=self._emitter,
                config=self._config,
                dag_run=dag_run,
            )
            self._emitter.flush()
        except Exception as e:
            logger.warning(f"Failed to emit DataFlow run failure: {e}", exc_info=True)


def get_dagrun_listener() -> DataHubDagRunListener | None:
    """Create and return the DAG run listener if plugin is enabled."""
    is_enabled = getenv("AIRFLOW__DATAHUB__ENABLE_DAG_RUN_LISTENER", "false").lower() == 'true'

    if not is_enabled:
        return None

    try:
        config = get_lineage_config()
        if config.enabled:
            logger.info("DataHub DAG run listener initialized")
            return DataHubDagRunListener(config=config)
    except Exception as e:
        logger.warning(f"Failed to initialize DataHub DAG run listener: {e}")

    return None


class DataHubDagRunPlugin(AirflowPlugin):
    """Airflow plugin that registers the DataHub DAG run listener."""

    name = "datahub_dagrun_plugin"
    listeners: ClassVar[list] = list(filter(None, [get_dagrun_listener()]))
