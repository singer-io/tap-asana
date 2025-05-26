import asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Portfolios(Stream):
    name = "portfolios"
    replication_method = "FULL_TABLE"

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
        "portfolio_items",
        "current_status_update",
        "custom_fields",
        "public"
    ]

    def get_objects(self):
        """Get stream object"""
        opt_fields = ",".join(self.fields)

        # Use WorkspacesApi to fetch workspaces
        workspaces_api = asana.WorkspacesApi(Context.asana.client)
        portfolios_api = asana.PortfoliosApi(Context.asana.client)

        # Fetch workspaces using call_api
        workspaces = self.call_api(workspaces_api, "get_workspaces")["data"]

        for workspace in workspaces:
            # Paginate through portfolios for each workspace
            offset = None
            while True:
                # Fetch portfolios for the current workspace using call_api
                response = self.call_api(
                    portfolios_api,
                    "get_portfolios",
                    workspace=workspace["gid"],  # Workspace ID
                    opts={"owner": "me", "opt_fields": opt_fields},
                    _request_timeout=self.request_timeout,
                )


                for portfolio in response["data"]:
                    # Fetch detailed portfolio information using get_portfolio
                    portfolio_details = self.call_api(
                        portfolios_api,
                        "get_portfolio",
                        portfolio_gid=portfolio["gid"],
                        opts={"opt_fields": opt_fields},
                        _request_timeout=self.request_timeout,
                    )

                    # Add detailed portfolio information to the portfolio object
                    portfolio.update(portfolio_details)
                    yield portfolio

                # Check if there are more pages
                offset = response.get("offset")
                if not offset:
                    break


Context.stream_objects["portfolios"] = Portfolios
