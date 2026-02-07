# Custom Source Discovery in DataHub

This document explains how DataHub discovers and loads custom source connectors using Python's entry point system.

## Overview

DataHub uses Python's **entry point** mechanism to automatically discover custom source connectors. This allows you to create custom connectors without modifying DataHub's core codebase.

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
airbyte = "connectors.airbyte_source:AirbyteSource"
```

**Note:** The entry point name (e.g., `airbyte`) is what you use in your recipe YAML file as `type: airbyte`. The import path points to your source class.

### 2. Source Class Implementation

Use this as an example to follow:

Example (from `connectors/airbyte_source.py`):

```python
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.common import PipelineContext
from datahub.configuration.common import ConfigModel
from typing import Dict, Iterable, Optional
from datahub.ingestion.api.workunit import MetadataWorkUnit

class AirbyteSourceConfig(ConfigModel):
    server_url: str
    workspace_id: str
    api_token: Optional[str] = None

class AirbyteSource(Source):
    """Inherits from Source, this is mandatory for Datahub to register it"""
    def __init__(self, config: AirbyteSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
    
    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "AirbyteSource":
        config = AirbyteSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_data(self):
        """Call an API to get whatever data you want."""
        raise NotImplementedError
    
    def get_example_workunit(self) -> MetadataWorkUnit:
        """Constructs the actual workunit, what datahub wants. You can either generate an MCE or MCP for this.
        MCP = MetadataChangeProposalWrapper
        MCE = MetadataChangeEvent
        """
        data = self.get_data()
        sanitized_name = data.name.replace(" ", "_").replace("/", "_").replace("\\", "_")

        example_urn = f"urn:li:dataFlow:(airbyte,{sanitized_name},airbyte)"
        flow_snapshot = DataFlowSnapshotClass(
            urn=flow_urn,
            aspects=[ # This is the dataflow's metadata.
                DataFlowInfoClass(
                    name=data.connection_name,
                    description=f"Airbyte connection: {data.connection_name}",
                    customProperties={ # get whatever metadata you want from your data object.
                        "connection_id": data.connection_id,
                        "source_id": data.source_id,
                        "destination_id": data.destination_id,
                        "status": data.status,
                    },
                )
            ],
        )
        
        mce = MetadataChangeEvent(proposedSnapshot=flow_snapshot)
        return MetadataWorkUnit(id=f"airbyte-connection-{data.connection_id}", mce=mce)

    
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Main method to generate workunits for Datahub. It generates the actual work units for datahub.
        In here you'd GET the resources from diff endpoints from your actual source,
        and then generate workunits via the MetadataWorkUnit class.

        Each actual definition of a work unit may come from diff schema_classes like these:
           DataFlowSnapshotClass,
           DataFlowInfoClass,
           DataPlatformInfoClass,
           PlatformTypeClass.

        All imported from: from datahub.metadata.schema_classes
        """
        # Generate and yield metadata workunits
        workunit = self.get_example_workunit()
        yield workunit
        self.report.report_workunit(workunit) # add workunit to the report to show on datahub's ingestion UI
    
    def get_report(self) -> SourceReport:
        """This can be left as is, the report attr is going to be ingested data in the get_workunits() function.
        When running, it runs self.report.report_workunit(workunit) to add the workunit to the report.
        It can also run self.report.report_failure(...) to report a particular ingestion failure.
        """
        return self.report
    
    def close(self):
        """Add this is you want to close a particular session like requests.Session()"""
        pass
```
