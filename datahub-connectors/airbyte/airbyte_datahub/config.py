from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class WorkspaceConfig:
    id: str
    name: str
    base_url: str


@dataclass
class DataHubConfig:
    server: str


@dataclass
class ConnectionConfig:
    id: str
    name: str
    upstream_datajob: str
    downstream_dataset: str


@dataclass
class PipelineConfig:
    workspace: WorkspaceConfig
    environment: str
    datahub: DataHubConfig
    connections: list[ConnectionConfig] = field(default_factory=list)


def load_config(path: str | Path) -> PipelineConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)

    return PipelineConfig(
        workspace=WorkspaceConfig(**raw["workspace"]),
        environment=raw["environment"],
        datahub=DataHubConfig(**raw["datahub"]),
        connections=[ConnectionConfig(**c) for c in raw.get("connections", [])],
    )
