"""
Custom DataHub connector for Airbyte.
Ingests Airbyte connections, sources, destinations, and jobs into DataHub.
"""

import logging
from typing import Dict, Iterable, List, Optional
import requests

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    DataFlowSnapshotClass,
    DataFlowInfoClass,
    DataJobSnapshotClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    EdgeClass,
    DataPlatformInfoClass,
    PlatformTypeClass,
)

logger = logging.getLogger(__name__)


class AirbyteSourceConfig(ConfigModel):
    """Configuration for Airbyte source connector."""

    server_url: str
    workspace_id: str
    username: Optional[str] = None
    password: Optional[str] = None
    api_token: Optional[str] = None
    platform_instance: str = "airbyte"
    ingest_connections: bool = True
    ingest_sources: bool = True
    ingest_destinations: bool = True
    ingest_jobs: bool = True

    # BigQuery environment for dataset URNs
    bigquery_env: str = "PROD"

    # Optional Airflow linkage: maps Airbyte connection_id -> Airflow task_id
    airflow_dag_id: Optional[str] = None
    airflow_cluster: str = "prod"
    airflow_connection_mapping: Optional[Dict[str, str]] = None


class AirbyteSource(Source):
    """Custom DataHub source connector for Airbyte."""

    def __init__(self, config: AirbyteSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.ctx = ctx
        self.report = SourceReport()
        self.session = requests.Session()
        self._destination_cache: Dict[str, Optional[Dict]] = {}

        # Set up authentication
        self.session.headers.update({"Authorization": f"Bearer {config.api_token}"})

        logger.info(f"Initialized AirbyteSource with server_url={config.server_url}")

    def get_connections(self, workspace_id: str) -> List[Dict]:
        """Fetch all connections for a workspace."""
        try:
            api_base = self.config.server_url
            url = f"{api_base}/connections"
            params = {"workspaceIds": workspace_id}
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # API might return list directly or wrapped in a key
            connections = (
                data
                if isinstance(data, list)
                else data.get("connections", data.get("data", []))
            )
            return connections
        except Exception as e:
            logger.error(f"Failed to fetch connections: {e}")
            self.report.report_failure(
                "connections", f"Error fetching connections: {e}"
            )
            return []

    def get_destination(self, destination_id: str) -> Optional[Dict]:
        """Fetch destination details by ID, with caching."""
        if destination_id in self._destination_cache:
            return self._destination_cache[destination_id]

        try:
            url = f"{self.config.server_url}/destinations/{destination_id}"
            response = self.session.get(url)
            response.raise_for_status()
            destination = response.json()
            self._destination_cache[destination_id] = destination
            return destination
        except Exception as e:
            logger.warning(f"Failed to fetch destination {destination_id}: {e}")
            self.report.report_failure(
                f"destination-{destination_id}",
                f"Error fetching destination: {e}",
            )
            self._destination_cache[destination_id] = None
            return None

    def get_connection_details(self, connection_id: str) -> Optional[Dict]:
        """Fetch full connection details by ID (includes stream configurations)."""
        try:
            url = f"{self.config.server_url}/connections/{connection_id}"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to fetch connection details {connection_id}: {e}")
            self.report.report_failure(
                f"connection-detail-{connection_id}",
                f"Error fetching connection details: {e}",
            )
            return None

    def _extract_stream_names(self, connection: Dict) -> List[str]:
        """Extract configured stream/table names from a connection."""
        streams = connection.get("configurations", {}).get("streams", [])
        names = []
        for stream_entry in streams:
            name = stream_entry.get("name")
            if name:
                names.append(name)
        return names

    def _build_bigquery_dataset_urns(
        self, stream_names: List[str], destination: Dict
    ) -> List[str]:
        """Build BigQuery dataset URNs from stream names and destination config."""
        config = destination.get("configuration", {})
        project_id = config.get("project_id")
        dataset_id = config.get("dataset_id")

        if not project_id or not dataset_id:
            logger.warning(
                f"Destination {destination.get('destinationId')} missing "
                f"project_id or dataset_id, skipping dataset URN creation"
            )
            return []

        return [
            make_dataset_urn(
                platform="bigquery",
                name=f"{project_id}.{dataset_id}.{table_name}",
                env=self.config.bigquery_env,
            )
            for table_name in stream_names
        ]

    def _make_flow_urn(self, connection: Dict) -> str:
        """Build a DataFlow URN for an Airbyte connection."""
        connection_name = connection.get(
            "name", f"connection-{connection.get('connectionId')}"
        )
        sanitized_name = (
            connection_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
        )
        return f"urn:li:dataFlow:(airbyte,{sanitized_name},{self.config.platform_instance})"

    def create_connection_workunit(self, connection: Dict) -> MetadataWorkUnit:
        """Create a DataFlow workunit for an Airbyte connection."""
        connection_id = connection.get("connectionId")
        connection_name = connection.get("name", f"connection-{connection_id}")
        flow_urn = self._make_flow_urn(connection)

        flow_snapshot = DataFlowSnapshotClass(
            urn=flow_urn,
            aspects=[
                DataFlowInfoClass(
                    name=connection_name,
                    description=f"Airbyte connection: {connection_name}",
                    customProperties={
                        "connection_id": connection_id,
                        "source_id": connection.get("sourceId"),
                        "destination_id": connection.get("destinationId"),
                        "status": connection.get("status", "unknown"),
                    },
                )
            ],
        )

        mce = MetadataChangeEvent(proposedSnapshot=flow_snapshot)
        return MetadataWorkUnit(id=f"airbyte-connection-{connection_id}", mce=mce)

    def create_datajob_workunit(
        self, connection: Dict, flow_urn: str
    ) -> Optional[MetadataWorkUnit]:
        """Create a DataJob workunit with dataset lineage for an Airbyte connection."""
        connection_id = connection.get("connectionId")
        connection_name = connection.get("name", f"connection-{connection_id}")
        destination_id = connection.get("destinationId")

        if not destination_id:
            logger.warning(
                f"Connection {connection_id} has no destinationId, skipping DataJob"
            )
            return None

        destination = self.get_destination(destination_id)
        if destination is None:
            return None

        dest_type = (destination.get("destinationType") or "").lower()

        # Fetch full connection details (list endpoint may omit stream config)
        connection_detail = self.get_connection_details(connection_id)
        if connection_detail is None:
            return None

        stream_names = self._extract_stream_names(connection_detail)
        if not stream_names:
            logger.warning(
                f"Connection {connection_id} has no configured streams, skipping DataJob"
            )
            return None

        output_dataset_urns = self._build_bigquery_dataset_urns(
            stream_names, destination
        )
        if not output_dataset_urns:
            return None

        # Build optional Airflow linkage
        input_datajob_edges = None
        mapping = self.config.airflow_connection_mapping or {}
        airflow_task_id = mapping.get(connection_id)
        if self.config.airflow_dag_id and airflow_task_id:
            airflow_task_urn = (
                f"urn:li:dataJob:(urn:li:dataFlow:(airflow,"
                f"{self.config.airflow_dag_id},{self.config.airflow_cluster}),"
                f"{airflow_task_id})"
            )
            input_datajob_edges = [EdgeClass(destinationUrn=airflow_task_urn)]

        job_urn = f"urn:li:dataJob:({flow_urn},sync)"
        job_snapshot = DataJobSnapshotClass(
            urn=job_urn,
            aspects=[
                DataJobInfoClass(
                    name=f"{connection_name} sync",
                    type="COMMAND",
                    description=f"Airbyte sync job for connection: {connection_name}",
                    flowUrn=flow_urn,
                    customProperties={
                        "connection_id": connection_id or "",
                        "destination_id": destination_id,
                        "stream_count": str(len(stream_names)),
                    },
                ),
                DataJobInputOutputClass(
                    inputDatasets=[],
                    outputDatasets=output_dataset_urns,
                    inputDatajobEdges=input_datajob_edges,
                ),
            ],
        )

        mce = MetadataChangeEvent(proposedSnapshot=job_snapshot)
        return MetadataWorkUnit(id=f"airbyte-datajob-{connection_id}", mce=mce)

    def create_platform_workunit(self) -> MetadataWorkUnit:
        """Create a workunit to register the Airbyte platform in DataHub."""
        platform_urn = "urn:li:dataPlatform:airbyte"
        mcp = MetadataChangeProposalWrapper(
            entityUrn=platform_urn,
            aspect=DataPlatformInfoClass(
                name="airbyte",
                displayName="Airbyte",
                type=PlatformTypeClass.OTHERS,
                logoUrl="http://host.docker.internal:8000/favicon.ico",
                datasetNameDelimiter=".",
            ),
        )
        return MetadataWorkUnit(id="airbyte-platform", mcp=mcp)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Main method to generate workunits from Airbyte."""
        logger.info("Starting Airbyte ingestion")

        # Register the Airbyte platform first
        yield self.create_platform_workunit()

        # Use configured workspace_id or first workspace
        workspace_id = self.config.workspace_id
        logger.info(f"Using workspace_id: {workspace_id}")

        # Ingest connections
        if self.config.ingest_connections:
            connections = self.get_connections(workspace_id)
            logger.info(f"Found {len(connections)} connections to ingest")
            for connection in connections:
                # Emit DataFlow (connection as a pipeline)
                flow_workunit = self.create_connection_workunit(connection)
                yield flow_workunit
                self.report.report_workunit(flow_workunit)

                # Emit DataJob with dataset (very important for lineage)
                flow_urn = self._make_flow_urn(connection)
                job_workunit = self.create_datajob_workunit(connection, flow_urn)
                if job_workunit is not None:
                    logger.info(f"Emitting DataJob for connection {job_workunit}")
                    yield job_workunit
                    self.report.report_workunit(job_workunit)

        logger.info(f"Completed Airbyte ingestion. Report: {self.report}")

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "AirbyteSource":
        """Factory method to create AirbyteSource instance."""
        config = AirbyteSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SourceReport:
        """Return the ingestion report."""
        return self.report

    def close(self):
        """Clean up resources."""
        self.session.close()
