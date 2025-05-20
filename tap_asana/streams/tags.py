from tap_asana.context import Context
from tap_asana.streams.base import Stream
import asana


class Tags(Stream):
    name = "tags"
    replication_key = "created_at"
    replication_method = "INCREMENTAL"
    fields = [
        "gid",
        "resource_type",
        "created_at",
        "followers",
        "name",
        "color",
        "notes",
        "permalink_url",
        "workspace",
    ]

    def z_get_objects(self):
        """Get stream object"""
        bookmark = self.get_bookmark()
        opt_fields = ",".join(self.fields)
        session_bookmark = bookmark
        for workspace in self.call_api("workspaces"):
            for tag in self.call_api(
                "tags", workspace=workspace["gid"], opt_fields=opt_fields
            ):
                session_bookmark = self.get_updated_session_bookmark(
                    session_bookmark, tag[self.replication_key]
                )
                if self.is_bookmark_old(tag[self.replication_key]):
                    yield tag
        self.update_bookmark(session_bookmark)

    def get_objects(self):
        """Get stream object"""
        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        opt_fields = ",".join(self.fields)

        # Use WorkspacesApi and TagsApi
        workspaces_api = asana.WorkspacesApi(Context.asana.client)
        tags_api = asana.TagsApi(Context.asana.client)

        # Fetch workspaces using call_api
        workspaces = self.call_api(workspaces_api, "get_workspaces")["data"]

        # Iterate over all workspaces
        for workspace in workspaces:
            # Fetch tags for the current workspace
            tag_response = self.call_api(
                tags_api,
                "get_tags",
                opts={"workspace": workspace["gid"], "opt_fields": opt_fields},
            )
            for tag in tag_response["data"]:
                session_bookmark = self.get_updated_session_bookmark(
                    session_bookmark, tag[self.replication_key]
                )
                if self.is_bookmark_old(tag[self.replication_key]):
                    yield tag

        # Update the bookmark after processing all tags
        self.update_bookmark(session_bookmark)


Context.stream_objects["tags"] = Tags
