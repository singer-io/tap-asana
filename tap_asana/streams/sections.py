import asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Sections(Stream):
    replication_method = "FULL_TABLE"
    name = "sections"

    fields = [
        "gid",
        "resource_type",
        "name",
        "created_at",
        "project",
        "projects"
    ]

    def get_objects(self):
        """Get stream object"""
        opt_fields = ",".join(self.fields)

        # Use WorkspacesApi, ProjectsApi, and SectionsApi
        workspaces_api = asana.WorkspacesApi(Context.asana.client)
        projects_api = asana.ProjectsApi(Context.asana.client)
        sections_api = asana.SectionsApi(Context.asana.client)

        # Fetch workspaces using call_api
        workspaces = self.call_api(workspaces_api, "get_workspaces")["data"]

        # Iterate over all workspaces
        for workspace in workspaces:
            # Fetch projects for the current workspace
            response = self.call_api(
                projects_api,
                "get_projects",
                opts={"workspace": workspace["gid"], "opt_fields": opt_fields},
                _request_timeout=self.request_timeout,
            )
            project_ids = [project["gid"] for project in response["data"]]

            # Iterate over all project IDs and fetch sections
            for project_id in project_ids:
                section_response = self.call_api(
                    sections_api,
                    "get_sections_for_project",
                    project_gid=project_id,
                    opts={"opt_fields": opt_fields},
                    _request_timeout=self.request_timeout,
                )
                for section in section_response["data"]:
                    yield section


Context.stream_objects["sections"] = Sections
