
from tap_asana.context import Context
from tap_asana.streams.base import Stream, asana_error_handling, REQUEST_TIMEOUT

@asana_error_handling
def find_team_by_organization(organization, opt_fields):
  teams = list(Context.asana.client.teams.find_by_organization(organization=organization,
                                                               opt_fields=opt_fields,
                                                               timeout=float(Context.config.get('request_timeout') or REQUEST_TIMEOUT)))
  return teams

@asana_error_handling
def get_users_for_teams(team):
  users = list(Context.asana.client.teams.users(team=team, timeout=float(Context.config.get('request_timeout') or REQUEST_TIMEOUT)))
  return users

class Teams(Stream):
  name = 'teams'
  replication_method = 'FULL_TABLE'
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
      if workspace.get('is_organization', False) == True:
        for team in find_team_by_organization(workspace["gid"], opt_fields):
          users = []
          for user in get_users_for_teams(team["gid"]):
            users.append(user)
          team['users'] = users
          yield team


Context.stream_objects['teams'] = Teams
