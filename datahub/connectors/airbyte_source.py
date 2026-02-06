"""
Custom DataHub connector for Airbyte.
Ingests Airbyte connections, sources, destinations, and jobs into DataHub.
"""

import logging
from typing import Dict, Iterable, List, Optional
import requests

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    DataFlowSnapshotClass,
    DataFlowInfoClass,
    DataPlatformInfoClass,
    PlatformTypeClass,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

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


class AirbyteSource(Source):
    """Custom DataHub source connector for Airbyte."""

    def __init__(self, config: AirbyteSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.ctx = ctx
        self.report = SourceReport()
        self.session = requests.Session()

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

    def create_connection_workunit(self, connection: Dict) -> MetadataWorkUnit:
        """Create a DataFlow workunit for an Airbyte connection."""
        connection_id = connection.get("connectionId")
        connection_name = connection.get("name", f"connection-{connection_id}")

        # Get source and destination info
        source_id = connection.get("sourceId")
        destination_id = connection.get("destinationId")

        # Create DataFlow (represents the connection)
        flow_urn = f"urn:li:dataFlow:(airbyte,{connection_name},{self.config.platform_instance})"
        flow_snapshot = DataFlowSnapshotClass(
            urn=flow_urn,
            aspects=[
                DataFlowInfoClass(
                    name=connection_name,
                    description=f"Airbyte connection: {connection_name}",
                    customProperties={
                        "connection_id": connection_id,
                        "source_id": source_id,
                        "destination_id": destination_id,
                        "status": connection.get("status", "unknown"),
                    },
                )
            ],
        )

        mce = MetadataChangeEvent(proposedSnapshot=flow_snapshot)
        return MetadataWorkUnit(id=f"airbyte-connection-{connection_id}", mce=mce)

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

        # Ingest sources
        if self.config.ingest_sources:
            sources = self.get_sources(workspace_id)
            for source in sources:
                yield self.create_source_workunit(source)
                self.report.report_workunit(self.create_source_workunit(source))

        # Ingest destinations
        if self.config.ingest_destinations:
            destinations = self.get_destinations(workspace_id)
            for destination in destinations:
                yield self.create_destination_workunit(destination)
                self.report.report_workunit(
                    self.create_destination_workunit(destination)
                )

        # Ingest connections
        if self.config.ingest_connections:
            connections = self.get_connections(workspace_id)
            for connection in connections:
                yield self.create_connection_workunit(connection)
                self.report.report_workunit(self.create_connection_workunit(connection))

                # Ingest jobs for each connection
                if self.config.ingest_jobs:
                    connection_id = connection.get("connectionId")
                    jobs = self.get_jobs(connection_id)
                    for job in jobs:
                        try:
                            job_wu = self.create_job_workunit(job, connection_id)
                            yield job_wu
                            self.report.report_workunit(job_wu)
                        except Exception as e:
                            logger.warning(
                                f"Failed to create job workunit for job {job.get('jobId')}: {e}"
                            )
                            # Continue with other jobs even if one fails
                            continue

        logger.info("Completed Airbyte ingestion")

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
