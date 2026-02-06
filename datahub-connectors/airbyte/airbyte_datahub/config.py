from pathlib import Path

import yaml
from pydantic import BaseModel


class WorkspaceConfig(BaseModel):
    id: str
    name: str
    base_url: str


class DataHubConfig(BaseModel):
    server: str


class ConnectionConfig(BaseModel):
    id: str
    name: str
    upstream_datajob: str
    downstream_dataset: str


class PipelineConfig(BaseModel):
    workspace: WorkspaceConfig
    environment: str
    datahub: DataHubConfig
    connections: list[ConnectionConfig] = []


def load_config(path: str | Path) -> PipelineConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)

    return PipelineConfig.model_validate(raw)
