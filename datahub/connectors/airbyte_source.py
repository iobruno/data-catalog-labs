"""
Custom DataHub connector for Airbyte.
Ingests Airbyte connections, sources, destinations, and jobs into DataHub.
"""

import logging
from typing import Dict, Iterable, List, Optional
import requests
from requests.auth import HTTPBasicAuth

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    DatasetPropertiesClass,
    DataFlowSnapshotClass,
    DataFlowInfoClass,
    DataJobSnapshotClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInfoClass,
    PlatformTypeClass,
    OwnershipClass,
    OwnershipTypeClass,
    OwnerClass,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

logger = logging.getLogger(__name__)


class AirbyteSourceConfig(ConfigModel):
    """Configuration for Airbyte source connector."""

    server_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    api_token: Optional[str] = None
    workspace_id: Optional[str] = None
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

    def get_workspaces(self) -> List[Dict]:
        """Fetch all workspaces from Airbyte."""
        try:
            api_base = self.config.server_url
            url = f"{api_base}/workspaces"
            logger.info(f"Fetching workspaces from: {url}")
            logger.debug(f"Using auth headers: {list(self.session.headers.keys())}")
            response = self.session.get(url)

            response.raise_for_status()
            data = response.json()
            # API might return list directly or wrapped in a key
            workspaces = (
                data
                if isinstance(data, list)
                else data.get("workspaces", data.get("data", []))
            )
            logger.info(f"Successfully fetched {len(workspaces)} workspace(s)")
            return workspaces
        except Exception as e:
            logger.error(f"Failed to fetch workspaces: {e}")
            self.report.report_failure("workspaces", f"Error fetching workspaces: {e}")
            return []

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

    def get_sources(self, workspace_id: str) -> List[Dict]:
        """Fetch all sources for a workspace."""
        try:
            api_base = self.config.server_url
            url = f"{api_base}/sources"
            params = {"workspaceIds": workspace_id}
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # API might return list directly or wrapped in a key
            sources = (
                data
                if isinstance(data, list)
                else data.get("sources", data.get("data", []))
            )
            return sources
        except Exception as e:
            logger.error(f"Failed to fetch sources: {e}")
            self.report.report_failure("sources", f"Error fetching sources: {e}")
            return []

    def get_destinations(self, workspace_id: str) -> List[Dict]:
        """Fetch all destinations for a workspace."""
        try:
            api_base = self.config.server_url
            url = f"{api_base}/destinations"
            params = {"workspaceIds": workspace_id}
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # API might return list directly or wrapped in a key
            destinations = (
                data
                if isinstance(data, list)
                else data.get("destinations", data.get("data", []))
            )
            return destinations
        except Exception as e:
            logger.error(f"Failed to fetch destinations: {e}")
            self.report.report_failure(
                "destinations", f"Error fetching destinations: {e}"
            )
            return []

    def get_jobs(self, connection_id: str, limit: int = 10) -> List[Dict]:
        """Fetch recent jobs for a connection."""
        try:
            api_base = self.config.server_url
            url = f"{api_base}/jobs"
            params = {
                "configTypes": "sync",  # Query param format
                "connectionId": connection_id,
                "limit": limit,
            }
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # API might return list directly or wrapped in a key
            jobs = (
                data
                if isinstance(data, list)
                else data.get("jobs", data.get("data", []))
            )
            return jobs
        except Exception as e:
            logger.error(f"Failed to fetch jobs for connection {connection_id}: {e}")
            return []

    def create_connection_workunit(
        self, connection: Dict, workspace_id: str
    ) -> MetadataWorkUnit:
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

    def create_source_workunit(self, source: Dict) -> MetadataWorkUnit:
        """Create a Dataset workunit for an Airbyte source."""
        source_id = source.get("sourceId")
        source_name = source.get("name", f"source-{source_id}")
        source_type = source.get("sourceDefinitionId", "unknown")

        # Determine platform based on source type and name
        platform = self._get_platform_from_source_type(source_type, source_name)

        # Create Dataset (represents the source)
        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{source_name},{self.config.platform_instance})"
        qualified_name = f"{platform}.{source_name}.{self.config.platform_instance}"
        dataset_snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[
                DatasetPropertiesClass(
                    name=source_name,
                    qualifiedName=qualified_name,
                    description=f"Airbyte source: {source_name}",
                    customProperties={
                        k: v for k, v in {
                            "source_id": source_id,
                            "source_type": source_type,
                            "source_definition_id": source.get("sourceDefinitionId"),
                        }.items() if v is not None
                    },
                )
            ],
        )

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return MetadataWorkUnit(id=f"airbyte-source-{source_id}", mce=mce)

    def create_destination_workunit(self, destination: Dict) -> MetadataWorkUnit:
        """Create a Dataset workunit for an Airbyte destination."""
        destination_id = destination.get("destinationId")
        destination_name = destination.get("name", f"destination-{destination_id}")
        destination_type = destination.get("destinationDefinitionId", "unknown")

        # Determine platform based on destination type
        platform = self._get_platform_from_destination_type(destination_type)

        # Create Dataset (represents the destination)
        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{destination_name},{self.config.platform_instance})"
        qualified_name = (
            f"{platform}.{destination_name}.{self.config.platform_instance}"
        )
        dataset_snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[
                DatasetPropertiesClass(
                    name=destination_name,
                    qualifiedName=qualified_name,
                    description=f"Airbyte destination: {destination_name}",
                    customProperties={
                        k: v for k, v in {
                            "destination_id": destination_id,
                            "destination_type": destination_type,
                            "destination_definition_id": destination.get(
                                "destinationDefinitionId"
                            ),
                        }.items() if v is not None
                    },
                )
            ],
        )

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return MetadataWorkUnit(id=f"airbyte-destination-{destination_id}", mce=mce)

    def create_job_workunit(self, job: Dict, connection_id: str) -> MetadataWorkUnit:
        """Create a DataJob workunit for an Airbyte sync job."""
        job_id = job.get("jobId")
        job_type = job.get("configType", "sync")

        # Create DataJob (represents a sync run)
        # DataJob URN format: urn:li:dataJob:(flowUrn,jobId)
        # The flow URN must be a complete DataFlow URN
        # Note: DataHub's URN parser expects the flow URN to be properly formatted
        # We construct the flow URN and embed it in the job URN
        flow_urn = f"urn:li:dataFlow:(airbyte,connection-{connection_id},{self.config.platform_instance})"

        # For DataJob URN, we embed the flow URN directly (no encoding)
        # The format is: urn:li:dataJob:(<flow_urn>,<job_id>)
        job_urn = f"urn:li:dataJob:({flow_urn},{job_id})"

        job_snapshot = DataJobSnapshotClass(
            urn=job_urn,
            aspects=[
                DataJobInfoClass(
                    name=f"sync-{job_id}",
                    type=job_type,
                    description=f"Airbyte sync job: {job_id}",
                    customProperties={
                        "job_id": str(job_id),
                        "connection_id": connection_id,
                        "status": job.get("status", "unknown"),
                        "started_at": str(job.get("startedAt", "")),
                    },
                ),
                DataJobInputOutputClass(
                    inputDatasets=[],
                    outputDatasets=[],
                ),
            ],
        )

        mce = MetadataChangeEvent(proposedSnapshot=job_snapshot)
        return MetadataWorkUnit(id=f"airbyte-job-{job_id}", mce=mce)

    def _get_platform_from_source_type(
        self, source_type: str, source_name: str = ""
    ) -> str:
        """Map Airbyte source type to DataHub platform."""
        # First try to infer from source name (e.g., "rss" in name)
        source_name_lower = source_name.lower()
        if "rss" in source_name_lower:
            return "rss"

        # Common mappings based on source definition ID or type
        platform_map = {
            "rss": "rss",
            "postgres": "postgres",
            "mysql": "mysql",
            "bigquery": "bigquery",
            "s3": "s3",
        }
        # source_type might be a UUID, so try lowercase match
        source_type_lower = str(source_type).lower()
        for key, platform in platform_map.items():
            if key in source_type_lower:
                return platform

        # Fallback to a generic platform that DataHub recognizes
        return "file"  # Use "file" as fallback for unknown sources

    def _get_platform_from_destination_type(self, destination_type: str) -> str:
        """Map Airbyte destination type to DataHub platform."""
        # Common mappings
        platform_map = {
            "bigquery": "bigquery",
            "postgres": "postgres",
            "s3": "s3",
            "snowflake": "snowflake",
        }
        return platform_map.get(destination_type.lower(), "airbyte")

    def create_platform_workunit(self) -> MetadataWorkUnit:
        """Create a workunit to register the Airbyte platform in DataHub."""
        platform_urn = "urn:li:dataPlatform:airbyte"
        mcp = MetadataChangeProposalWrapper(
            entityUrn=platform_urn,
            aspect=DataPlatformInfoClass(
                name="airbyte",
                displayName="Airbyte",
                type=PlatformTypeClass.OTHERS,
                logoUrl="https://airbyte.com/favicon.ico",
                datasetNameDelimiter=".",
            ),
        )
        return MetadataWorkUnit(id="airbyte-platform", mcp=mcp)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Main method to generate workunits from Airbyte."""
        logger.info("Starting Airbyte ingestion")

        # Register the Airbyte platform first
        yield self.create_platform_workunit()

        # Get workspaces
        workspaces = self.get_workspaces()
        if not workspaces:
            logger.warning("No workspaces found")
            return

        # Use configured workspace_id or first workspace
        workspace_id = self.config.workspace_id
        if not workspace_id and workspaces:
            workspace_id = workspaces[0].get("workspaceId")
            logger.info(f"Using workspace: {workspace_id}")

        if not workspace_id:
            logger.error("No workspace ID available")
            return

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
                yield self.create_connection_workunit(connection, workspace_id)
                self.report.report_workunit(
                    self.create_connection_workunit(connection, workspace_id)
                )

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
        config = AirbyteSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SourceReport:
        """Return the ingestion report."""
        return self.report

    def close(self):
        """Clean up resources."""
        self.session.close()
