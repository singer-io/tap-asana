
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Tags(Stream):
  name = 'tags'
  replication_key = "created_at"
  fields = [
    "id",
    "created_at",
    "followers",
    "name",
    "color",
    "notes",
    "workspace"
  ]  


  def get_objects(self):
    opt_fields = ",".join(self.fields)
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    for workspace in self.call_api("workspaces"):
      for tag in self.call_api("tags", workspace=workspace["gid"], opt_fields=opt_fields):
        if utils.strptime_with_tz(tag[self.replication_key]) > session_bookmark:
          session_bookmark = utils.strptime_with_tz(tag[self.replication_key])
        yield tag
    self.update_bookmark(tag[self.replication_key])


Context.stream_objects['tags'] = Tags
