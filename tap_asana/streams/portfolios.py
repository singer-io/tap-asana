
from tap_asana.context import Context
from tap_asana.streams.base import Stream


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


  def get_objects(self):
    opt_fields = ",".join(self.fields)
    for workspace in self.call_api("workspaces"):
      # NOTE: Currently, API users can only get a list of portfolios that they themselves own; owner="me"
      for portfolio in Context.asana.client.portfolios.get_portfolios(workspace=workspace["gid"], owner="me", opt_fields=opt_fields, timeout=self.request_timeout):
        # portfolio_items are typically the projects in a portfolio
        portfolio_items = []
        for portfolio_item in Context.asana.client.portfolios.get_items_for_portfolio(portfolio_gid=portfolio["gid"], timeout=self.request_timeout):
          portfolio_items.append(portfolio_item)
        portfolio['portfolio_items'] = portfolio_items
        yield portfolio


Context.stream_objects["portfolios"] = Portfolios
