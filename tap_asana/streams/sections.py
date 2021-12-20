
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Sections(Stream):
  name = 'sections'
  replication_method = 'FULL_TABLE'

  fields = [
    "gid",
    "resource_type",
    "name",
    "created_at",
    "project",
    "projects"
  ]

  def get_objects(self):
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
    opt_fields = ",".join(self.fields)

    # list of project ids
    project_ids = []

    for workspace in self.call_api("workspaces"):
      for project in self.call_api("projects", workspace=workspace["gid"]):
        project_ids.append(project["gid"])

    # iterate on all project ids and execute rest of the sync
    for project_id in project_ids:
      for section in Context.asana.client.sections.get_sections_for_project(project_gid=project_id, owner="me", opt_fields=opt_fields):
        yield section


Context.stream_objects['sections'] = Sections
