
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Workspaces(Stream):
  name = 'workspaces'
  replication_method = 'FULL_TABLE'

  fields = [
    "gid",
    "name",
    "is_organization",
    "resource_type",
    "email_domains"
  ]

  def get_objects(self):
    opt_fields = ",".join(self.fields)
    for workspace in self.call_api("workspaces", opt_fields=opt_fields):
      yield workspace


Context.stream_objects['workspaces'] = Workspaces
