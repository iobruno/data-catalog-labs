"""
Custom Airflow listener to emit DAG-level execution events to DataHub.

The acryl-datahub-airflow-plugin only emits task-level execution events.
This plugin fills the gap by emitting DataFlow run start/completion events.

See: https://github.com/datahub-project/datahub/blob/master/metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/airflow2/datahub_listener.py#L1068
"""
import logging

from airflow.listeners import hookimpl
from airflow.models import DagRun

from datahub_airflow_plugin._config import get_lineage_config
from datahub_airflow_plugin.client.airflow_generator import AirflowGenerator

logger = logging.getLogger(__name__)


class DataHubDagRunListener:
    """Listener that emits DataFlow execution events to DataHub."""

    def __init__(self):
        self._config = None
        self._emitter = None

    def _get_config(self):
        if self._config is None:
            self._config = get_lineage_config()
        return self._config

    def _get_emitter(self):
        if self._emitter is None:
            config = self._get_config()
            if config.enabled:
                self._emitter = config.make_emitter_hook().make_emitter()
        return self._emitter

    def _should_process(self, dag_id: str) -> bool:
        config = self._get_config()
        if not config.enabled or not config.capture_executions:
            return False
        if not config.dag_filter_pattern.allowed(dag_id):
            return False
        return True

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str) -> None:
        """Emit DataFlow run START event when DAG run begins."""
        if not dag_run.dag_id or not self._should_process(dag_run.dag_id):
            return

        try:
            config = self._get_config()
            emitter = self._get_emitter()
            if not emitter:
                return

            logger.info(f"Emitting DataHub DataFlow run start for {dag_run.dag_id}")
            AirflowGenerator.run_dataflow(
                emitter=emitter,
                config=config,
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
            config = self._get_config()
            emitter = self._get_emitter()
            if not emitter:
                return

            logger.info(f"Emitting DataHub DataFlow run success for {dag_run.dag_id}")
            AirflowGenerator.complete_dataflow(
                emitter=emitter,
                config=config,
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
            config = self._get_config()
            emitter = self._get_emitter()
            if not emitter:
                return

            logger.info(f"Emitting DataHub DataFlow run failure for {dag_run.dag_id}")
            AirflowGenerator.complete_dataflow(
                emitter=emitter,
                config=config,
                dag_run=dag_run,
            )
            emitter.flush()
        except Exception as e:
            logger.warning(f"Failed to emit DataFlow run failure: {e}", exc_info=True)


# Instantiate the listener for Airflow's plugin manager
datahub_dagrun_listener = DataHubDagRunListener()
