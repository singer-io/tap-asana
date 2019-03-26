
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Projects(Stream):
  name = "projects"
  replication_key = "modified_at"
  fields = [
    "name",
    "id",
    "owner",
    "current_status",
    "due_date",
    "created_at",
    "modified_at",
    "archived",
    "public",
    "members",
    "followers",
    "color",
    "notes",
    "workspace",
    "team"
  ]


  def get_objects(self):
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    opt_fields = ",".join(self.fields)
    for workspace in Context.asana.client.workspaces.find_all():
      for project in Context.asana.client.projects.find_all(workspace=workspace["gid"], opt_fields=opt_fields):
        if utils.strptime_with_tz(project[self.replication_key]) > session_bookmark:
          session_bookmark = utils.strptime_with_tz(project[self.replication_key])
        yield project
    self.update_bookmark(project[self.replication_key])


Context.stream_objects["projects"] = Projects
