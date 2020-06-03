
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Tasks(Stream):
  name = 'tasks'
  replication_key = "modified_at"
  replication_method = 'INCREMENTAL'
  fields = [
    "gid",
    "assignee",
    "assignee_status",
    "created_at",
    "completed",
    "completed_at",
    "custom_fields",
    "tags",
    "due_on",
    "due_at",
    "start_on",
    "external",
    "followers",
    "hearted",
    "hearts",
    "modified_at",
    "name",
    "notes",
    "num_hearts",
    "projects",
    "parent",
    "workspace",
    "memberships"
  ]


  def get_objects(self):
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
    opt_fields = ",".join(self.fields)
    for workspace in self.call_api("workspaces"):
      for project in self.call_api("projects", workspace=workspace["gid"]):
        for task in self.call_api("tasks", project=project["gid"], opt_fields=opt_fields, modified_since=modified_since): 
          session_bookmark = self.get_updated_session_bookmark(session_bookmark, task[self.replication_key])
          if self.is_bookmark_old(task[self.replication_key]):
            yield task
    self.update_bookmark(session_bookmark)


Context.stream_objects['tasks'] = Tasks
