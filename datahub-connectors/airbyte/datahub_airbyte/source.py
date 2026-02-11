from dataclasses import dataclass, field

from datahub.api.entities.datajob.dataflow import DataFlow
from datahub.api.entities.datajob.datajob import DataJob
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn

from datahub_airbyte.airbyte_client import AirbyteClient
from datahub_airbyte.config import AirbyteConnectionSourceConfig


@dataclass
class AirbyteConnectionSource(Source):
    """Custom DataHub ingestion source that registers an Airbyte connection as
    a DataJob entity, bridging the lineage gap between Airflow and BigQuery.

    Airbyte does not emit OpenLineage events, so DataHub has no visibility into
    the Airflow-task -> Airbyte-sync -> BigQuery-table relationship.  This
    source fills that gap by producing Metadata Change Proposals (MCPs) for:

    * A **DataFlow** (``orchestrator="airbyte"``) that groups connections.
    * A **DataJob** representing the Airbyte connection, linked to:
        - an **upstream** Airflow DataJob (the task that triggers the sync), and
        - **downstream** BigQuery Datasets (the tables the sync produces).

    The resulting lineage in DataHub looks like::

        Airflow DataJob  -->  Airbyte DataJob  -->  BigQuery Dataset
        (existing)            (created here)        (existing)

    This source processes **one connection per invocation**.  Run the recipe
    multiple times with different env vars to register several connections.
    """

    source_config: AirbyteConnectionSourceConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AirbyteConnectionSource":
        """Factory method called by the DataHub ingestion framework.

        The framework reads the ``source.config`` block from the recipe YAML,
        resolves any ``${ENV_VAR}`` placeholders, and passes the resulting
        dictionary here.  This method validates it against
        :class:`AirbyteConnectionSourceConfig` (raising on missing or extra
        fields) and returns a ready-to-run source instance.

        Args:
            config_dict: Raw configuration dictionary parsed from the recipe
                YAML after environment-variable substitution.  Expected keys
                match :class:`AirbyteConnectionSourceConfig` fields.
            ctx: Pipeline context provided by the framework, carrying the
                run ID, DataHub graph connection, and pipeline-level settings.

        Returns:
            A fully initialised :class:`AirbyteConnectionSource`.
        """
        config = AirbyteConnectionSourceConfig.model_validate(config_dict)
        return cls(ctx=ctx, source_config=config)

    def get_report(self) -> SourceReport:
        """The framework calls this after ingestion completes to display
        statistics (events produced, warnings, failures) in the CLI summary.

        Returns:
            The :class:`SourceReport` instance tracking this run's metrics.
        """
        return self.report

    def get_workunits_internal(self):
        """Generate Metadata Change Proposals (MCPs) for one Airbyte connection.

        This is the core method called by the DataHub ingestion framework.
        It builds a DataFlow and a DataJob, wires up lineage, and yields
        the resulting MCPs as :class:`MetadataWorkUnit` objects that the
        framework forwards to the configured sink.

        1. **DataFlow** -- groups Airbyte connections under a single
           orchestrator.  Its ``flow.id`` is populated dynamically from the
           Airbyte API with the workspace slug (e.g. ``default-workspace``)::

               urn:li:dataFlow:(airbyte, <workspace-slug>, <env>)

        2. **DataJob** -- represents the Airbyte connection itself.
           The connection UUID is used as the job ID for traceability.
           ``job.url`` is set to the Airbyte connection UI URL fetched from the API::

               urn:li:dataJob:(<Flow URN>, <connection-uuid>)

        3. **Upstream** -- the Airflow task that triggers this sync is added
          to ``job.upstream_urns``, creating a ``consumes`` edge from the
          Airflow DataJob to the Airbyte DataJob::

            urn:li:dataJob:(urn:li:dataFlow:(airflow, <dag_name>, <env>), <task_name>)

        4. **Downstream** -- the BigQuery tables produced by the sync are added
          to ``job.outlets``, creating ``produces`` edges from the Airbyte
          DataJob to the BigQuery Datasets.  ``job.url`` is also set to the
          Airbyte connection UI URL so DataHub can link back to the source::

            urn:li:dataset:(urn:li:dataPlatform:bigquery, <project.dataset.table>, PROD)

        Yields:
            :class:`MetadataWorkUnit` instances wrapping each MCP generated
            by the DataFlow and DataJob helpers.
        """
        cfg = self.source_config
        env = cfg.environment

        flow = DataFlow(orchestrator="airbyte", id="", env=env)
        job = DataJob(id=cfg.airbyte_connection_id, flow_urn=flow.urn, name=cfg.airflow_task)

        upstream_urns = self.build_upstream_urns(cfg)
        job.upstream_urns.extend(upstream_urns)

        job.url, flow.id, downstream_urns = self.fetch_downstream_details(cfg)
        job.outlets.extend(downstream_urns)

        for mcp in flow.generate_mcp():
            yield MetadataWorkUnit.from_metadata(mcp)

        for mcp in job.generate_mcp():
            yield MetadataWorkUnit.from_metadata(mcp)

    def build_upstream_urns(self, cfg: AirbyteConnectionSourceConfig) -> list[DataJobUrn]:
        """Build the upstream Airflow DataJob URN from config.

        Constructs the URN for the Airflow task that triggers this Airbyte
        sync, so DataHub can draw lineage from the Airflow job to the
        Airbyte connection.
        """
        airflow_flow_urn = DataFlowUrn("airflow", cfg.airflow_dag, cfg.environment)
        airflow_task_urn = DataJobUrn(airflow_flow_urn, cfg.airflow_task)
        return [airflow_task_urn]

    def fetch_downstream_details(
        self, cfg: AirbyteConnectionSourceConfig
    ) -> tuple[str, str, list[DatasetUrn]]:
        """Fetch connection metadata from Airbyte and build downstream Dataset URNs.

        Calls the Airbyte API to discover the destination's BigQuery project,
        dataset, and stream names, then returns the connection URL, workspace
        slug (used as the DataFlow ID), and a list of BigQuery Dataset URNs.
        """
        client = AirbyteClient(
            cfg.airbyte_server_url, cfg.airbyte_client_id, cfg.airbyte_client_secret
        )
        conn_details = client.fetch_connection_details(conn_id=cfg.airbyte_connection_id)

        dataset_urns = [
            DatasetUrn.create_from_ids("bigquery", table, "PROD") for table in conn_details.tables
        ]

        return conn_details.url, conn_details.workspace, dataset_urns
