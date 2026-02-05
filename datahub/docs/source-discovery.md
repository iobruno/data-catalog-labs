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

### 3. Discovery Mechanism

The registry uses Python's `importlib.metadata.entry_points()` to discover all packages that have registered entry points:

```python
def _load_entrypoint(self, entry_point_key: str) -> None:
    for entry_point in entry_points(group=entry_point_key):
        self.register_lazy(entry_point.name, entry_point.value)
```

**What happens:**
1. Python's packaging system (setuptools) reads `pyproject.toml` during package installation
2. Entry points are registered in the package's metadata
3. `entry_points()` scans all installed packages for the specified group
4. Each discovered entry point is registered **lazily** (as a string path, not imported yet)

### 4. Lazy Loading

The registry stores entry points as import paths (strings) rather than importing the classes immediately:

```python
def register_lazy(self, key: str, import_path: str) -> None:
    self._register(key, import_path)  # Stores "connectors.airbyte_source:AirbyteSource"
```

**Benefits:**
- Faster startup time (classes aren't imported until needed)
- Better error handling (import errors only occur when the source is actually used)
- Reduced memory footprint

### 5. Materialization (Class Import)

When a recipe uses the source (e.g., `type: airbyte`), DataHub materializes the entry point:

```python
def get(self, key: str) -> Type[T]:
    self._materialize_entrypoints()  # Loads all lazy entry points
    # ... then looks up "airbyte" and imports the class
```

The materialization process:
1. Discovers all entry points in the registered groups
2. For the requested source, imports the class using the stored import path
3. Validates the class (ensures it's a subclass of `Source`)
4. Replaces the string path with the actual class in the registry

## Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Package Installation                                      │
│    pyproject.toml → setuptools → entry point metadata        │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. DataHub Startup                                          │
│    source_registry.register_from_entrypoint()                │
│    → Registers entry point group for discovery              │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Recipe Parsing                                           │
│    Recipe contains: type: airbyte                           │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Source Lookup                                            │
│    source_registry.get("airbyte")                            │
│    → Triggers materialization                                │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Entry Point Discovery                                    │
│    entry_points(group="datahub.ingestion.source.plugins")   │
│    → Finds "airbyte" = "connectors.airbyte_source:..."      │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Class Import                                             │
│    import_path("connectors.airbyte_source:AirbyteSource")    │
│    → Imports AirbyteSource class                            │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. Source Instantiation                                     │
│    AirbyteSource.create(config_dict, ctx)                    │
│    → Creates and returns source instance                    │
└─────────────────────────────────────────────────────────────┘
```

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

## Benefits of Entry Point System

1. **No Manual Registration**: No need to modify DataHub's core code
2. **Automatic Discovery**: All installed packages with entry points are automatically found
3. **Lazy Loading**: Classes are only imported when actually used
4. **Standard Mechanism**: Uses Python's standard PEP 621 entry points
5. **Isolation**: Custom sources are isolated from DataHub's core codebase

## Troubleshooting

### Source Not Found

If DataHub can't find your source:

1. **Check entry point format**: Ensure it's in `pyproject.toml` under the correct group
2. **Verify package installation**: The package must be installed for entry points to be registered
3. **Check import path**: The module path must be correct and importable
4. **Inspect registry**: Use Python to check if entry points are discovered:
   ```python
   from importlib.metadata import entry_points
   eps = entry_points(group="datahub.ingestion.source.plugins")
   print(list(eps))
   ```

### Import Errors

If you get import errors:

1. **Check module path**: Ensure the module is in Python's path
2. **Verify class name**: The class name after the colon must exist
3. **Check dependencies**: All dependencies must be installed
4. **Docker context**: In Docker, ensure the module is copied into the image

## References

- [PEP 621 - Project metadata](https://peps.python.org/pep-0621/)
- [Python Entry Points](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/)
- [DataHub Source API](https://datahubproject.io/docs/metadata-ingestion/sources/)
