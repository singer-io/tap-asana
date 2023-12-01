# pylint:disable=duplicate-code
from tap_asana.context import Context
from tap_asana.streams.base import Stream
import singer
import logging

LOGGER = singer.get_logger()
LOGGER.setLevel(logging.DEBUG)

class SubTasks(Stream):
    name = "subtasks"
    replication_method = "FULL_TABLE"
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
        for workspace in self.call_api("workspaces"):
            for project in self.call_api("projects", workspace=workspace["gid"]):
                    project_ids.append(project["gid"])

        for indx, project_id in enumerate(project_ids, 1):
            LOGGER.info("Fetching Subtasks for project: %s/%s",indx, len(project_ids))
            tasks_list = self.call_api("tasks",project=project_id,opt_fields=opt_fields)
            for task in tasks_list:
                yield from self.fetch_children(task, opt_fields)

    def fetch_children(self, p_task, opt_fields):
        subtasks_children = []
        resource = getattr(Context.asana.client, "tasks")
        subtasks  = list(resource.get_subtasks_for_task(p_task.get("gid"), opt_fields=opt_fields))
        for s_task in subtasks:
            subtasks_children.extend(self.fetch_children(s_task, opt_fields))
        return subtasks + subtasks_children


Context.stream_objects["subtasks"] = SubTasks
