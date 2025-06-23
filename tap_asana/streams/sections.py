import asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream


# pylint: disable=use-yield-from
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
        workspaces = self.fetch_workspaces()

        # Use SectionsApi to fetch sections
        sections_api = asana.SectionsApi(Context.asana.client)

        # Iterate over all workspaces
        for workspace in workspaces:
            response = self.fetch_projects(workspace_gid=workspace["gid"], opt_fields=opt_fields,
                                           request_timeout=self.request_timeout)
            project_ids = [project["gid"] for project in response]

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
