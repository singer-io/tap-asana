
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Users(Stream):
  replication_method = 'FULL_TABLE'
  name = 'users'

  fields = [
    "gid",
    "resource_type",
    "name",
    "email",
    "photo",
    "workspaces"
  ]


  def get_objects(self):
    opt_fields = ",".join(self.fields)
    for workspace in self.call_api("workspaces"):
      for user in self.call_api("users", workspace=workspace["gid"], opt_fields=opt_fields):
        yield user


Context.stream_objects['users'] = Users
