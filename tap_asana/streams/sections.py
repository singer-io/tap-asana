
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream, asana_error_handling

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

  @asana_error_handling
  def get_sections_for_projects(self, project_gid, owner, opt_fields):

    # Get and return a list sections for provided project
    sections = list(Context.asana.client.sections.get_sections_for_project(project_gid=project_gid,
                                                                          owner=owner,
                                                                          opt_fields=opt_fields,
                                                                          timeout=self.request_timeout))
    return sections

  def get_objects(self):
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
    opt_fields = ",".join(self.fields)
    for workspace in self.call_api("workspaces"):
      for project in self.call_api("projects", workspace=workspace["gid"]):
        for section in self.get_sections_for_projects(project["gid"], "me", opt_fields):
          yield section


Context.stream_objects['sections'] = Sections
