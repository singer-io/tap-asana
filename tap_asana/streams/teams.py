
from tap_asana.context import Context
from tap_asana.streams.base import Stream, asana_error_handling, REQUEST_TIMEOUT

@asana_error_handling
def find_team_by_organization(organization, opt_fields):
  # Set request timeout to config param `request_timeout` value.
  config_request_timeout = Context.config.get('request_timeout')
  # If value is 0,"0","" or not passed then it set default to 300 seconds.
  if config_request_timeout and float(config_request_timeout):
    request_timeout = float(config_request_timeout)
  else:
    request_timeout = REQUEST_TIMEOUT

  # Get and return a list of teams for provided organization
  teams = list(Context.asana.client.teams.find_by_organization(organization=organization,
                                                               opt_fields=opt_fields,
                                                               timeout=request_timeout))
  return teams

@asana_error_handling
def get_users_for_teams(team):
  # Set request timeout to config param `request_timeout` value.
  config_request_timeout = Context.config.get('request_timeout')
  # If value is 0,"0","" or not passed then it set default to 300 seconds.
  if config_request_timeout and float(config_request_timeout):
    request_timeout = float(config_request_timeout)
  else:
    request_timeout = REQUEST_TIMEOUT

  # Get and return a list of users for provided team
  users = list(Context.asana.client.teams.users(team=team, timeout=request_timeout))
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
