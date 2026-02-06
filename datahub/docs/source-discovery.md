# Custom Source Discovery in DataHub

This document explains how DataHub discovers and loads custom source connectors using Python's entry point system.

## Overview

DataHub uses Python's **entry point** mechanism (PEP 621) to automatically discover custom source connectors. This allows you to create custom connectors without modifying DataHub's core codebase.

## How It Works

### 1. Entry Point Definition

Custom sources are registered in `pyproject.toml` under the `datahub.ingestion.source.plugins` entry point group:

```toml
[project.entry-points."datahub.ingestion.source.plugins"]
airbyte = "connectors.airbyte_source:AirbyteSource"
```

**Components:**
- **Entry point group**: `datahub.ingestion.source.plugins` - This is the namespace DataHub uses to discover sources
- **Entry point name**: `airbyte` - This is the identifier used in recipes (e.g., `type: airbyte`)
- **Import path**: `connectors.airbyte_source:AirbyteSource` - The module path and class name

### 2. Registration Process

When DataHub starts, it initializes the source registry:

```python
# In datahub/ingestion/source/source_registry.py
from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.source import Source

source_registry = PluginRegistry[Source]()
source_registry.register_from_entrypoint("datahub.ingestion.source.plugins")
```

The `register_from_entrypoint()` method tells the registry to discover all entry points in the specified group.

The materialization process:
1. Discovers all entry points in the registered groups
2. For the requested source, imports the class using the stored import path
3. Validates the class (ensures it's a subclass of `Source`)
4. Replaces the string path with the actual class in the registry

## Requirements for Custom Sources

To create a custom source that DataHub can discover, you need:

### 1. Entry Point Registration

Add to `pyproject.toml`:

```toml
[project.entry-points."datahub.ingestion.source.plugins"]
your_source_name = "your.module.path:YourSourceClass"
```

### 2. Source Class Implementation

Your source class must:

- Inherit from `datahub.ingestion.api.source.Source`
- Implement the required methods:
  - `get_workunits()` - Generate metadata work units
  - `get_report()` - Return ingestion report
  - `close()` - Clean up resources
- Have a `create()` class method (factory method)
- Use `ConfigModel` for configuration

Example:

```python
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.common import PipelineContext
from datahub.configuration.common import ConfigModel

class YourSourceConfig(ConfigModel):
    server_url: str

class YourSource(Source):
    def __init__(self, config: YourSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
    
    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "YourSource":
        config = YourSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)
    
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Your implementation
        pass
    
    def get_report(self) -> SourceReport:
        return self.report
    
    def close(self):
        pass
```

### 3. Package Installation

The package must be installed (or built into a Docker image) so that setuptools can register the entry points:

```bash
# Local development
uv sync

# Docker build
docker build -t datahub-ingest:latest .
```
