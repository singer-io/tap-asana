
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Teams(Stream):
  replication_method = 'FULL_TABLE'
  name = 'teams'

  fields = [
    "gid",
    "resource_type",
    "name",
    "description",
    "html_description",
    "organization",
    "permalink_url"
  ]


  def get_objects(self):
    opt_fields = ",".join(self.fields)
    for workspace in self.call_api("workspaces", opt_fields="gid,is_organization"):
      if workspace.get('is_organization', False):
        for team in Context.asana.client.teams.find_by_organization(organization=workspace["gid"], opt_fields=opt_fields, timeout=self.request_timeout):
          users = []
          for user in Context.asana.client.teams.users(team=team["gid"], timeout=self.request_timeout):
            users.append(user)
          team['users'] = users
          yield team


Context.stream_objects['teams'] = Teams
