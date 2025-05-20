from tap_asana.context import Context
from tap_asana.streams.base import Stream
import asana


class Projects(Stream):
  name = "projects"
  replication_key = "modified_at"
  replication_method = 'INCREMENTAL'
  fields = [
    "name",
    "gid",
    "owner",
    "current_status",
    "custom_fields",
    "default_view",
    "due_date",
    "due_on",
    "html_notes",
    "is_template",
    "created_at",
    "modified_at",
    "start_on",
    "archived",
    "public",
    "members",
    "followers",
    "color",
    "notes",
    "icon",
    "permalink_url",
    "workspace",
    "team",
    "resource_type",
    "current_status_update",
    "custom_field_settings",
    "completed",
    "completed_at",
    "completed_by",
    "created_from_template",
    "project_brief"
  ]

  def get_objects(self):
    """Get stream object"""
    opt_fields = ",".join(self.fields)
    bookmark = self.get_bookmark()
    session_bookmark = bookmark

    # Use WorkspacesApi to fetch workspaces
    workspaces_api = asana.WorkspacesApi(Context.asana.client)
    projects_api = asana.ProjectsApi(Context.asana.client)

    # Fetch workspaces using call_api
    workspaces = self.call_api(workspaces_api, "get_workspaces")["data"]

    for workspace in workspaces:
        # Paginate through projects for each workspace
        offset = None
        while True:
            response = self.call_api(
                projects_api,
                "get_projects",
                opts={"workspace" : workspace["gid"],  "opt_fields":opt_fields }    
            )

            for project in response["data"]:
                session_bookmark = self.get_updated_session_bookmark(session_bookmark, project[self.replication_key])
                if self.is_bookmark_old(project[self.replication_key]):
                    yield project

            # Check if there are more pages
            offset = response.get("offset")
            if not offset:
                break

    # Update the bookmark after processing all projects
    self.update_bookmark(session_bookmark)


Context.stream_objects["projects"] = Projects
