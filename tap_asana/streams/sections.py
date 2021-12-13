
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

  # send list of project ids
  def get_project_ids(self):
    for workspace in self.call_api("workspaces"):
      for project in self.call_api("projects", workspace=workspace["gid"]):
        yield project["gid"]

  def get_objects(self):
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
    opt_fields = ",".join(self.fields)

    # iterate on all project ids and execute rest of the sync
    for project_id in self.get_project_ids():
      for section in Context.asana.client.sections.get_sections_for_project(project_gid=project_id, owner="me", opt_fields=opt_fields):
        yield section


Context.stream_objects['sections'] = Sections
