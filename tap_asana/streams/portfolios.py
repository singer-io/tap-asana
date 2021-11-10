
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream, asana_error_handling

class Portfolios(Stream):
  name = "portfolios"
  replication_method = 'FULL_TABLE'

  fields = [
    "gid",
    "resource_type",
    "name",
    "color",
    "created_at",
    "created_by",
    "custom_field_settings",
    "is_template",
    "due_on",
    "members",
    "owner",
    "permalink_url",
    "start_on",
    "workspace",
    "portfolio_items"
  ]

  @asana_error_handling
  def get_items_for_portfolio(self, portfolio_gid):

    # Get and return a list of portfolio items for provided portfolio_gid
    portfolio_items = list(Context.asana.client.portfolios.get_items_for_portfolio(portfolio_gid=portfolio_gid, timeout=self.request_timeout))
    return portfolio_items

  @asana_error_handling
  def get_portfolies_for_workspace(self, workspace_id, owner, opt_fields):

    # Get and return a list of portfolios for provided workspace
    portfolios = list(Context.asana.client.portfolios.get_portfolios(workspace=workspace_id,
                                                                    owner=owner,
                                                                    opt_fields=opt_fields,
                                                                    timeout=self.request_timeout))
    return portfolios

  def get_objects(self):
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    opt_fields = ",".join(self.fields)
    for workspace in self.call_api("workspaces"):
      # NOTE: Currently, API users can only get a list of portfolios that they themselves own; owner="me"
      for portfolio in self.get_portfolies_for_workspace(workspace["gid"], "me", opt_fields):
        # portfolio_items are typically the projects in a portfolio
        portfolio_items = []
        for portfolio_item in self.get_items_for_portfolio(portfolio["gid"]):
          portfolio_items.append(portfolio_item)
        portfolio['portfolio_items'] = portfolio_items
        yield portfolio


Context.stream_objects["portfolios"] = Portfolios
