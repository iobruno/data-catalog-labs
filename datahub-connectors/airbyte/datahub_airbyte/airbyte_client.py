from dataclasses import dataclass

from airbyte_api import AirbyteAPI, api, models
from airbyte_api.models import ConnectionResponse, DestinationResponse, WorkspaceResponse
from slugify import slugify
from yarl import URL


@dataclass
class AirbyteConnectionDetails:
    url: str
    workspace: str
    tables: list[str]


class AirbyteClient:
    """Thin wrapper around the Airbyte SDK for fetching connection metadata."""

    def __init__(self, server_url: str, client_id: str, client_secret: str):
        self.server_url = URL(server_url)
        self._client = AirbyteAPI(
            server_url=server_url,
            security=models.Security(
                client_credentials=models.SchemeClientCredentials(
                    client_id=client_id,
                    client_secret=client_secret,
                    token_url=str(self.server_url / "applications/token"),
                )
            ),
        )

    def fetch_connection_by(self, id: str) -> ConnectionResponse | None:
        req = self._client.connections.get_connection(request=api.GetConnectionRequest(id))
        return req.connection_response

    def fetch_destination_by(self, id: str) -> DestinationResponse | None:
        req = self._client.destinations.get_destination(request=api.GetDestinationRequest(id))
        return req.destination_response

    def fetch_workspace_by(self, id: str) -> WorkspaceResponse | None:
        req = self._client.workspaces.get_workspace(request=api.GetWorkspaceRequest(id))
        return req.workspace_response

    def fmt_url(self, workspace_id, conn_id) -> str:
        return f"{self.server_url.origin()}/workspaces/{workspace_id}/connections/{conn_id}"

    def fetch_connection_details(self, conn_id: str) -> AirbyteConnectionDetails:
        """Fetch connection metadata from the Airbyte API.
        Retrieves the connection and its BigQuery destination, then returns:

        * Fully-qualified BigQuery table names built as
          ``{project_id}.{dataset_id}.{prefix}{table_name}`` for each stream.
        * The Airbyte UI URL for the connection.
        """
        conn = self.fetch_connection_by(id=conn_id)
        destination = self.fetch_destination_by(id=conn.destination_id)
        workspace = self.fetch_workspace_by(id=conn.workspace_id)

        project_id = destination.configuration.project_id
        dataset_id = destination.configuration.dataset_id
        prefix = conn.prefix or ""

        bigquery_tables = [
            f"{project_id}.{dataset_id}.{prefix}{stream.name or stream.destination_object_name}"
            for stream in conn.configurations.streams
        ]

        return AirbyteConnectionDetails(
            tables=bigquery_tables,
            url=self.fmt_url(conn.workspace_id, conn_id),
            workspace=slugify(workspace.name),
        )
