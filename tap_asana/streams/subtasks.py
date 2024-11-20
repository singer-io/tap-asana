# pylint:disable=duplicate-code
import singer
from tap_asana.context import Context
from tap_asana.streams.base import Stream


LOGGER = singer.get_logger()

class SubTasks(Stream):
    name = "subtasks"
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
        "is_rendered_as_separator",
        "liked",
        "likes",
        "memberships",
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
        for workspace in self.call_api("workspaces"):
            for project in self.call_api("projects", workspace=workspace["gid"]):
                project_ids.append(project["gid"])

        projects_fraction = len(project_ids) // 100 # near 1% of total projects

        # iterate over all project ids and continue fetching
        LOGGER.info("Fetching subtasks...")
        for indx, project_id in enumerate(project_ids, 1):
            if (indx % projects_fraction == 0):
                LOGGER.info(f"Progress near: {indx / projects_fraction}%)")
            tasks_list = self.call_api("tasks", project=project_id, opt_fields=opt_fields)
            len_tasks_list = len(tasks_list)

            for task in tasks_list:
                for subt in self.fetch_children(task, opt_fields):
                    LOGGER.info(f"subtasks for task: {task}, tasks_list_len: {len_tasks_list}%)")
                    session_bookmark = self.get_updated_session_bookmark(
                        session_bookmark, subt[self.replication_key]
                    )
                    if self.is_bookmark_old(subt[self.replication_key]):
                        yield subt
        self.update_bookmark(session_bookmark)

    def fetch_children(self, p_task, opt_fields):
        subtasks_children = []
        resource = getattr(Context.asana.client, "tasks")
        subtasks = list(resource.get_subtasks_for_task(p_task.get("gid"), opt_fields=opt_fields))
        for s_task in subtasks:
            subtasks_children.extend(self.fetch_children(s_task, opt_fields))
        return subtasks + subtasks_children


Context.stream_objects["subtasks"] = SubTasks
