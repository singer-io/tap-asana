
from tap_asana.context import Context
from tap_asana.streams.base import Stream


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
    "resource_type"]

  def get_objects(self):
    opt_fields = ",".join(self.fields)
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    for workspace in self.call_api("workspaces"):
      for project in self.call_api("projects", workspace=workspace["gid"], opt_fields=opt_fields):
        session_bookmark = self.get_updated_session_bookmark(session_bookmark, project[self.replication_key])
        if self.is_bookmark_old(project[self.replication_key]):
          yield project
    self.update_bookmark(session_bookmark)


Context.stream_objects["projects"] = Projects
