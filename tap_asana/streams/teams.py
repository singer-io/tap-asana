
from tap_asana.context import Context
from tap_asana.streams.base import Stream, asana_error_handling

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

  @asana_error_handling
  def find_team_by_organization(self, organization, opt_fields):

    # Get and return a list of teams for provided organization
    teams = list(Context.asana.client.teams.find_by_organization(organization=organization,
                                                                opt_fields=opt_fields,
                                                                timeout=self.request_timeout))
    return teams

  @asana_error_handling
  def get_users_for_teams(self, team):

    # Get and return a list of users for provided team
    users = list(Context.asana.client.teams.users(team=team, timeout=self.request_timeout))
    return users

  def get_objects(self):
    opt_fields = ",".join(self.fields)
    for workspace in self.call_api("workspaces", opt_fields="gid,is_organization"):
      if workspace.get('is_organization', False) == True:
        for team in self.find_team_by_organization(workspace["gid"], opt_fields):
          users = []
          for user in self.get_users_for_teams(team["gid"]):
            users.append(user)
          team['users'] = users
          yield team


Context.stream_objects['teams'] = Teams
