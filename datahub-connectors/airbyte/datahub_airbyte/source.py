from dataclasses import dataclass, field

from datahub.api.entities.datajob.dataflow import DataFlow
from datahub.api.entities.datajob.datajob import DataJob
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn

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
        - a **downstream** BigQuery Dataset (the table the sync produces).

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

        Example::

            # Normally called by the framework, but can be used directly:
            from datahub.ingestion.api.common import PipelineContext

            ctx = PipelineContext(run_id="manual-test-run")
            source = AirbyteConnectionSource.create(
                config_dict={
                    "airflow_dag": "hackernews_rss_bigquery",
                    "airflow_task": "hackernews_rss_front",
                    "bigquery_table": "iobruno-gcp-labs.hackernews_rss_raw.frontpage_items",
                    "airbyte_connection_id": "e37988e6-8ed5-465c-abb2-150639819c62",
                },
                ctx=ctx,
            )
        """
        config = AirbyteConnectionSourceConfig.model_validate(config_dict)
        return cls(ctx=ctx, source_config=config)

    def get_report(self) -> SourceReport:
        """Return the ingestion report for this source.

        The framework calls this after ingestion completes to display
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

        **Entities created:**

        1. **DataFlow** -- groups Airbyte connections under a single
           orchestrator.  Its URN uses the Airflow DAG name as the flow ID
           so that the Airbyte flow mirrors the Airflow DAG it belongs to::

               urn:li:dataFlow:(airbyte,hackernews_rss_bigquery,prod)

        2. **DataJob** -- represents the Airbyte connection itself.  The
           connection UUID is used as the job ID for traceability::

               urn:li:dataJob:(
                   urn:li:dataFlow:(airbyte,hackernews_rss_bigquery,prod),
                   e37988e6-8ed5-465c-abb2-150639819c62
               )

        **Lineage wired:**

        * **Upstream** -- the Airflow task that triggers this sync is added
          to ``job.upstream_urns``, creating a ``consumes`` edge from the
          Airflow DataJob to the Airbyte DataJob::

              urn:li:dataJob:(
                  urn:li:dataFlow:(airflow,hackernews_rss_bigquery,prod),
                  hackernews_rss_front
              )

        * **Downstream** -- the BigQuery table produced by the sync is added
          to ``job.outlets``, creating a ``produces`` edge from the Airbyte
          DataJob to the BigQuery Dataset::

              urn:li:dataset:(
                  urn:li:dataPlatform:bigquery,
                  iobruno-gcp-labs.hackernews_rss_raw.frontpage_items,
                  PROD
              )

        Yields:
            :class:`MetadataWorkUnit` instances wrapping each MCP generated
            by the DataFlow and DataJob helpers.
        """
        cfg = self.source_config
        env = cfg.environment

        flow = DataFlow(orchestrator="airbyte", id="Default_Workspace", env=env)
        job = DataJob(id=cfg.airbyte_connection_id, flow_urn=flow.urn, name=cfg.airflow_task)
        job.upstream_urns.append(self.fetch_upstream_relations(cfg))
        job.outlets.append(self.fetch_downstream_relations(cfg))

        for mcp in flow.generate_mcp():
            yield MetadataWorkUnit.from_metadata(mcp)

        for mcp in job.generate_mcp():
            yield MetadataWorkUnit.from_metadata(mcp)

    def fetch_upstream_relations(self, cfg: AirbyteConnectionSourceConfig) -> DataJobUrn:
        airflow_flow_urn = DataFlowUrn("airflow", cfg.airflow_dag, cfg.environment)
        airflow_task_urn = DataJobUrn(airflow_flow_urn, cfg.airflow_task)
        return airflow_task_urn

    def fetch_downstream_relations(self, cfg: AirbyteConnectionSourceConfig) -> DatasetUrn:
        return DatasetUrn.create_from_ids("bigquery", cfg.bigquery_table, "PROD")
