"""
Custom Airflow listener to emit DAG-level execution events to DataHub.

The acryl-datahub-airflow-plugin only emits task-level execution events.
This plugin fills the gap by emitting DataFlow run start/completion events.

See: https://github.com/datahub-project/datahub/blob/master/metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/airflow2/datahub_listener.py#L1068
"""
import logging
from typing import Optional

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
        self._emitter = None

    def _get_emitter(self):
        if self._emitter is None:
            self._emitter = self._config.make_emitter_hook().make_emitter()
        return self._emitter

    def _should_process(self, dag_id: str) -> bool:
        if not self._config.capture_executions:
            return False
        if not self._config.dag_filter_pattern.allowed(dag_id):
            return False
        return True

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str) -> None:
        """Emit DataFlow run START event when DAG run begins."""
        if not dag_run.dag_id or not self._should_process(dag_run.dag_id):
            return

        try:
            emitter = self._get_emitter()
            if not emitter:
                return

            logger.info(f"Emitting DataHub DataFlow run start for {dag_run.dag_id}")
            AirflowGenerator.run_dataflow(
                emitter=emitter,
                config=self._config,
                dag_run=dag_run,
            )
            emitter.flush()
        except Exception as e:
            logger.warning(f"Failed to emit DataFlow run start: {e}", exc_info=True)

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str) -> None:
        """Emit DataFlow run COMPLETION event with SUCCESS status."""
        if not dag_run.dag_id or not self._should_process(dag_run.dag_id):
            return

        try:
            emitter = self._get_emitter()
            if not emitter:
                return

            logger.info(f"Emitting DataHub DataFlow run success for {dag_run.dag_id}")
            AirflowGenerator.complete_dataflow(
                emitter=emitter,
                config=self._config,
                dag_run=dag_run,
            )
            emitter.flush()
        except Exception as e:
            logger.warning(f"Failed to emit DataFlow run success: {e}", exc_info=True)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str) -> None:
        """Emit DataFlow run COMPLETION event with FAILURE status."""
        if not dag_run.dag_id or not self._should_process(dag_run.dag_id):
            return

        try:
            emitter = self._get_emitter()
            if not emitter:
                return

            logger.info(f"Emitting DataHub DataFlow run failure for {dag_run.dag_id}")
            AirflowGenerator.complete_dataflow(
                emitter=emitter,
                config=self._config,
                dag_run=dag_run,
            )
            emitter.flush()
        except Exception as e:
            logger.warning(f"Failed to emit DataFlow run failure: {e}", exc_info=True)


def get_dagrun_listener() -> Optional[DataHubDagRunListener]:
    """Create and return the DAG run listener if plugin is enabled."""
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
    listeners = list(filter(None, [get_dagrun_listener()]))
