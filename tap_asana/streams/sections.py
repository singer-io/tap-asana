import singer
from tap_asana.context import Context
from tap_asana.streams.base import Stream


LOGGER = singer.get_logger()

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
        # list of project ids
        project_ids = []

        opt_fields = ",".join(self.fields)

        for workspace in self.call_api("workspaces"):
            for project in self.call_api("projects", workspace=workspace["gid"]):
                project_ids.append(project["gid"])

        num_projects = len(project_ids) // 100 # near 1% of total projects
        koef = 1

        # iterate on all project ids and execute rest of the sync
        for indx, project_id in enumerate(project_ids, 1):
            if (indx == koef * num_projects):
                LOGGER.info(f"Fetching sections progress near: {koef}%)")
                koef += 1
            for section in Context.asana.client.sections.get_sections_for_project(
                project_gid=project_id,
                owner="me",
                opt_fields=opt_fields,
                timeout=self.request_timeout,
            ):
                yield section


Context.stream_objects["sections"] = Sections
