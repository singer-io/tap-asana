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

        projects_total = len(project_ids)
        projects_fraction = projects_total // 100 # near 1% of total projects

        # iterate on all project ids and execute rest of the sync
        LOGGER.info("Fetching sections...")
        for indx, project_id in enumerate(project_ids, 1):
            if (indx % projects_fraction == 0):
                LOGGER.info(f"Fetching done for projects: {indx - 1}/{projects_total}")
            for section in Context.asana.client.sections.get_sections_for_project(
                project_gid=project_id,
                owner="me",
                opt_fields=opt_fields,
                timeout=self.request_timeout,
            ):
                yield section


Context.stream_objects["sections"] = Sections
