import asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream


# pylint:disable=duplicate-code
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
        opt_fields = ",".join(self.fields)
        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
        workspaces = self.fetch_workspaces()

        # Use TasksApi to fetch tasks
        tasks_api = asana.TasksApi(Context.asana.client)

        for workspace in workspaces:
            response = self.fetch_projects(workspace_gid=workspace["gid"], opt_fields="gid",
                                           request_timeout=self.request_timeout)
            project_ids = [project["gid"] for project in response]

            # Iterate over all project IDs and fetch tasks
            for project_id in project_ids:
                task_response = self.call_api(
                    tasks_api,
                    "get_tasks",
                    opts={
                        "project": project_id,
                        "opt_fields": opt_fields,
                        "modified_since": modified_since,
                    },
                    _request_timeout=self.request_timeout,
                )
                for task in task_response["data"]:
                    session_bookmark = self.get_updated_session_bookmark(
                        session_bookmark, task[self.replication_key]
                    )
                    if self.is_bookmark_old(task[self.replication_key]):
                        yield task

        # Update the bookmark after processing all tasks
        self.update_bookmark(session_bookmark)


Context.stream_objects["tasks"] = Tasks
