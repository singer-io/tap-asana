# pylint:disable=duplicate-code
import asana
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
        opt_fields = ",".join(self.fields)
        bookmark = self.get_bookmark()
        session_bookmark = bookmark

        # Use WorkspacesApi, ProjectsApi, and TasksApi
        workspaces_api = asana.WorkspacesApi(Context.asana.client)
        projects_api = asana.ProjectsApi(Context.asana.client)
        tasks_api = asana.TasksApi(Context.asana.client)

        # Fetch workspaces using call_api
        workspaces = self.call_api(workspaces_api, "get_workspaces")["data"]

        # Iterate over all workspaces
        for workspace in workspaces:
            # Fetch projects for the current workspace
            projects_response = self.call_api(
                projects_api,
                "get_projects",
                opts={"workspace": workspace["gid"], "opt_fields": "gid"},
                _request_timeout=self.request_timeout,
            )
            project_ids = [project["gid"] for project in projects_response["data"]]

            # Iterate over all project IDs and fetch tasks
            for indx, project_id in enumerate(project_ids, 1):
                LOGGER.info("Fetching Subtasks for project: %s/%s", indx, len(project_ids))
                tasks_response = self.call_api(
                    tasks_api,
                    "get_tasks",
                    opts={"project": project_id, "opt_fields": opt_fields},
                    _request_timeout=self.request_timeout,
                )
                for task in tasks_response["data"]:
                    # Fetch subtasks recursively
                    for subt in self.fetch_children(task, opt_fields):
                        session_bookmark = self.get_updated_session_bookmark(
                            session_bookmark, subt[self.replication_key]
                        )
                        if self.is_bookmark_old(subt[self.replication_key]):
                            yield subt

        # Update the bookmark after processing all subtasks
        self.update_bookmark(session_bookmark)

    def fetch_children(self, p_task, opt_fields):
        subtasks_children = []
        resource = asana.TasksApi(Context.asana.client) 
        subtasks = list(resource.get_subtasks_for_task(task_gid=p_task.get("gid"), opts={"opt_fields": opt_fields}))
        for s_task in subtasks:
            subtasks_children.extend(self.fetch_children(s_task, opt_fields))
        return subtasks + subtasks_children


Context.stream_objects["subtasks"] = SubTasks
