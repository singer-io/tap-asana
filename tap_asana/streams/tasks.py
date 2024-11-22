import singer
from tap_asana.context import Context
from tap_asana.streams.base import Stream


LOGGER = singer.get_logger()

class Tasks(Stream):
    name = "tasks"
    replication_key = "modified_at"
    replication_method = "INCREMENTAL"
    fields = [
        "gid",
        "resource_type",
        "name",
        "approval_status",
        "assignee_status",
        "completed",
        "completed_at",
        "completed_by",
        "created_at",
        "dependencies",
        "dependents",
        "due_at",
        "due_on",
        "external",
        "hearted",
        "hearts",
        "html_notes",
        "is_rendered_as_seperator",
        "liked",
        "likes",
        "memberships",
        "memberships.section",
        "memberships.project",
        "modified_at",
        "notes",
        "num_hearts",
        "num_likes",
        "num_subtasks",
        "resource_subtype",
        "start_on",
        "assignee",
        "custom_fields",
        "followers",
        "parent",
        "permalink_url",
        "projects",
        "tags",
        "workspace",
        "start_at",
        "assignee_section"
    ]

    def get_objects(self):
        """Get stream object"""
        # list of project ids
        project_ids = []

        opt_fields = ",".join(self.fields)
        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")

        for workspace in self.call_api("workspaces"):
            for project in self.call_api("projects", workspace=workspace["gid"]):
                project_ids.append(project["gid"])

        projects_total = len(project_ids)
        projects_fraction = projects_total // 100 # near 1% of total projects

        # iterate over all project ids and continue fetching
        LOGGER.info("Fetching tasks...")
        for indx, project_id in enumerate(project_ids, 1):
            if (indx % projects_fraction == 0):
                LOGGER.info(f"Fetching done for projects: {indx - 1}/{projects_total}")
            for task in self.call_api(
                "tasks",
                project=project_id,
                opt_fields=opt_fields,
                opt_expand="memberships",
                modified_since=modified_since,
            ):
                session_bookmark = self.get_updated_session_bookmark(
                    session_bookmark, task[self.replication_key]
                )
                if self.is_bookmark_old(task[self.replication_key]):
                    yield task

        self.update_bookmark(session_bookmark)


Context.stream_objects["tasks"] = Tasks
