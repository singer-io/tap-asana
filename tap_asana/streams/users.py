import asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Users(Stream):
    replication_method = "FULL_TABLE"
    name = "users"

    fields = [
        "gid",
        "resource_type",
        "name",
        "email",
        "photo",
        "workspaces"
    ]

    
    def get_objects(self):
        """Get stream object"""
        opt_fields = ",".join(self.fields)

        # Use WorkspacesApi and UsersApi
        workspaces_api = asana.WorkspacesApi(Context.asana.client)
        users_api = asana.UsersApi(Context.asana.client)

        # Fetch workspaces using call_api
        workspaces = self.call_api(
            workspaces_api,
            "get_workspaces",
            opts={"opt_fields": "gid"},
        )["data"]

        # Iterate over all workspaces
        for workspace in workspaces:
            # Fetch users for the current workspace
            users_response = self.call_api(
                users_api,
                "get_users",
                opts={"workspace": workspace["gid"], "opt_fields": opt_fields},
                _request_timeout=self.request_timeout,
            )
            for user in users_response["data"]:
                yield user


Context.stream_objects["users"] = Users
