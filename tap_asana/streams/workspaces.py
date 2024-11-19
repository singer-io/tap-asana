import singer
from tap_asana.context import Context
from tap_asana.streams.base import Stream


LOGGER = singer.get_logger()

class Workspaces(Stream):
    name = "workspaces"
    replication_method = "FULL_TABLE"

    fields = [
        "gid",
        "name",
        "is_organization",
        "resource_type",
        "email_domains"
    ]

    def get_objects(self):
        """Get stream object"""
        opt_fields = ",".join(self.fields)
        LOGGER.info("Fetching workspaces)")
        for workspace in self.call_api("workspaces", opt_fields=opt_fields):
            yield workspace


Context.stream_objects["workspaces"] = Workspaces
