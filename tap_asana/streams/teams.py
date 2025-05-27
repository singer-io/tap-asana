import asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream


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
        workspaces = self.fetch_workspaces()

        # Use TeamsApi, and UsersApi
        teams_api = asana.TeamsApi(Context.asana.client)
        users_api = asana.UsersApi(Context.asana.client)

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
