import asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream


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

    def get_objects(self):
        """Get stream object"""
        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        opt_fields = ",".join(self.fields)
        workspaces = self.fetch_workspaces()

        # Use TagsApi to fetch tags
        tags_api = asana.TagsApi(Context.asana.client)

        # Iterate over all workspaces
        for workspace in workspaces:
            # Fetch tags for the current workspace
            tag_response = self.call_api(
                tags_api,
                "get_tags",
                opts={"workspace": workspace["gid"], "opt_fields": opt_fields},
                _request_timeout=self.request_timeout,
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
