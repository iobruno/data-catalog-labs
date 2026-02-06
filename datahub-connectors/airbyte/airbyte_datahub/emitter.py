from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.datajob.dataflow import DataFlow
from datahub.api.entities.datajob.datajob import DataJob
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata._urns.urn_defs import DataJobUrn, DatasetUrn
from datahub.metadata.schema_classes import (
    AuditStampClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
)

from airbyte_datahub.config import PipelineConfig, ConnectionConfig


def fetch_airbyte_url(base_url: str, workspace_id: str, connection_id: str) -> str:
    return f"{base_url}/workspaces/{workspace_id}/connections/{connection_id}"


def build_datajob(config: PipelineConfig, conn: ConnectionConfig, flow: DataFlow) -> DataJob:
    job = DataJob(id=conn.id, flow_urn=flow.urn, name=conn.name)
    job.upstream_urns.append(DataJobUrn.from_string(conn.upstream_datajob))
    job.outlets.append(DatasetUrn.from_string(conn.downstream_dataset))
    external_url = fetch_airbyte_url(config.workspace.base_url, config.workspace.id, conn.id)
    job.url = external_url
    return job


def emit_all(config: PipelineConfig, flow: DataFlow, jobs: list[DataJob]) -> None:
    emitter = DataHubRestEmitter(gms_server=config.datahub.server)
    all_mcps = list(flow.generate_mcp()) + [mcp for job in jobs for mcp in job.generate_mcp()]
    for mcp in all_mcps:
        emitter.emit(mcp)
    print(f"Emitted {len(all_mcps)} MCPs to {config.datahub.server}")


def process(config: PipelineConfig) -> None:
    flow = DataFlow(orchestrator="airbyte", id=config.workspace.name, env=config.environment)
    jobs = [build_datajob(config, conn, flow) for conn in config.connections]
    emit_all(config, flow, jobs)
