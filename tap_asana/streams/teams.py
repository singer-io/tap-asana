from tap_asana.context import Context
from tap_asana.streams.base import Stream
import asana


class Teams(Stream):
    replication_method = "FULL_TABLE"
    name = "teams"

    fields = [
        "gid",
        "resource_type",
        "name",
        "description",
        "html_description",
        "organization",
        "permalink_url",
        "visibility"
    ]

    def get_objects(self):
        """Get stream object"""
        opt_fields = ",".join(self.fields)

        # Use WorkspacesApi, TeamsApi, and UsersApi
        workspaces_api = asana.WorkspacesApi(Context.asana.client)
        teams_api = asana.TeamsApi(Context.asana.client)
        users_api = asana.UsersApi(Context.asana.client)

        # Fetch workspaces using call_api
        workspaces = self.call_api(
            workspaces_api,
            "get_workspaces",
            opts={"opt_fields": "gid,is_organization"},
        )["data"]

        # Iterate over all workspaces
        for workspace in workspaces:
            if workspace.get("is_organization", False):
                # Fetch teams for the current workspace using get_teams_for_workspace
                teams_response = self.call_api(
                    teams_api,
                    "get_teams_for_workspace",
                    workspace_gid=workspace["gid"],
                    opts={"opt_fields": opt_fields},
                    _request_timeout=self.request_timeout,
                )
                for team in teams_response["data"]:
                    # Fetch users for the current team using get_users_for_team
                    users_response = self.call_api(
                        users_api,
                        "get_users_for_team",
                        team_gid=team["gid"],
                        opts={"opt_fields": "gid,name,email"},
                        _request_timeout=self.request_timeout,
                    )
                    team["users"] = users_response["data"]
                    yield team


Context.stream_objects["teams"] = Teams
