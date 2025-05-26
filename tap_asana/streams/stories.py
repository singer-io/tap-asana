import asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream

class Stories(Stream):
    name = "stories"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"

    fields = [
        "gid",
        "resource_type",
        "created_at",
        "created_by",
        "resource_subtype",
        "text",
        "html_text",
        "is_pinned",
        "assignee",
        "dependency",
        "duplicate_of",
        "duplicated_from",
        "follower",
        "hearted",
        "hearts",
        "is_edited",
        "liked",
        "likes",
        "new_approval_status",
        "new_dates",
        "new_enum_value",
        "old_date_value",
        "new_date_value",
        "old_people_value",
        "new_people_value",
        "new_name",
        "new_number_value",
        "new_resource_subtype",
        "new_section",
        "new_text_value",
        "num_hearts",
        "num_likes",
        "old_approval_status",
        "old_dates",
        "old_enum_value",
        "old_name",
        "old_number_value",
        "old_resource_subtype",
        "old_section",
        "old_text_value",
        "preview",
        "project",
        "source",
        "story",
        "tag",
        "target",
        "task",
        "sticker_name",
        "custom_field",
        "is_editable",
        "new_multi_enum_values",
        "old_multi_enum_values",
        "type"
    ]

    def get_objects(self):
        """Get stream object"""
        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        opt_fields = ",".join(self.fields)

        # Use WorkspacesApi, ProjectsApi, TasksApi, and StoriesApi
        workspaces_api = asana.WorkspacesApi(Context.asana.client)
        projects_api = asana.ProjectsApi(Context.asana.client)
        tasks_api = asana.TasksApi(Context.asana.client)
        stories_api = asana.StoriesApi(Context.asana.client)

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

            # Iterate over all project IDs and fetch tasks
            for project_id in project_ids:
                task_response = self.call_api(
                    tasks_api,
                    "get_tasks",
                    opts={"project": project_id},
                    _request_timeout=self.request_timeout,
                )
                for task in task_response["data"]:
                    task_gid = task.get("gid")

                    # Fetch stories for the current task
                    story_response = self.call_api(
                        stories_api,
                        "get_stories_for_task",
                        task_gid=task_gid,
                        opts={"opt_fields": opt_fields},
                        _request_timeout=self.request_timeout,
                    )
                    for story in story_response["data"]:
                        session_bookmark = self.get_updated_session_bookmark(
                            session_bookmark, story[self.replication_key]
                        )
                        if self.is_bookmark_old(story[self.replication_key]):
                            yield story

        # Update the bookmark after processing all stories
        self.update_bookmark(session_bookmark)


Context.stream_objects["stories"] = Stories
