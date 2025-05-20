from tap_asana.context import Context
from tap_asana.streams.base import Stream
import asana


class Workspaces(Stream):
    name = "workspaces"
    replication_method = "FULL_TABLE"

    fields = [
        "gid",
        "name",
        "is_organization",
        "resource_type",
        "email_domains"
    ]

    def get_objects(self):
        """Get stream object"""
        opt_fields = ",".join(self.fields)

        # Use WorkspacesApi
        workspaces_api = asana.WorkspacesApi(Context.asana.client)

        # Fetch workspaces using call_api
        workspaces_response = self.call_api(
            workspaces_api,
            "get_workspaces",
            opts={"opt_fields": opt_fields},
        )
        for workspace in workspaces_response["data"]:
            yield workspace


Context.stream_objects["workspaces"] = Workspaces
