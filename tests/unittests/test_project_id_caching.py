import unittest
import tap_asana.streams.sections as sections
import tap_asana.streams.stories as stories
import tap_asana.streams.tasks as tasks
from unittest import mock
from tap_asana.context import Context
from tap_asana.asana import Asana

# Dummy 'sections' data
def sections_data(*args, **kwargs):
    return [
        {"gid": 1, "name": "test_data_1"},
        {"gid": 2, "name": "test_data_2"},
        {"gid": 3, "name": "test_data_3"}]

# Dummy 'stories' data
def stories_data(*args, **kwargs):
    return [
        {"gid": 1, "name": "test_data_1", "created_at": "2021-01-01"},
        {"gid": 2, "name": "test_data_2", "created_at": "2021-01-01"},
        {"gid": 3, "name": "test_data_3", "created_at": "2021-01-01"}]

# Mock 'call_api' function
# Mock 'call_api' function
def mock_call_api(*args, **kwargs):
    # Dummy 'workspaces' data
    if args[1] == "get_workspaces":
        return {"data": [{"gid": 1, "name": "test_workspace"}]}
    # Dummy 'projects' data
    elif args[1] == "get_projects":
        return {"data": [
            {"gid": 1, "name": "test_project_1"},
            {"gid": 2, "name": "test_project_2"}
        ]}
    # Dummy 'sections' data
    elif args[1] == "get_sections_for_project":
        return {"data": sections_data()}
    # Dummy 'tasks' data
    elif args[1] == "get_tasks":
        return {"data": [
            {"gid": 1, "name": "test_task_1", "modified_at": "2021-01-01T00:00:00Z"},
            {"gid": 2, "name": "test_task_2", "modified_at": "2021-01-02T00:00:00Z"}
        ]}
    # Dummy 'stories' data
    elif args[1] == "get_stories_for_task":
        return {"data": stories_data()}
    # Default case
    return {"data": []}

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("time.sleep")
@mock.patch("tap_asana.streams.sections.Sections.fetch_workspaces")
@mock.patch("tap_asana.streams.sections.Sections.fetch_projects")
@mock.patch("tap_asana.streams.base.Stream.call_api")
class TestProjectIdCaching(unittest.TestCase):

    def test_sections(self, mocked_call_api, mocked_fetch_projects, mocked_fetch_workspaces, mocked_sleep, mocked_refresh_access_token):
        # Set config file
        Context.config = {'start_date': '2021-01-01T00:00:00Z'}
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        # Mock 'refresh_access_token' to return a valid token
        mocked_refresh_access_token.return_value = "mock_access_token"
        # Mock 'fetch_workspaces' to return dummy workspaces
        mocked_fetch_workspaces.return_value = [{"gid": 1, "name": "test_workspace"}]
        # Mock 'fetch_projects' to return dummy projects
        mocked_fetch_projects.return_value = [
            {"gid": 1, "name": "test_project_1"},
            {"gid": 2, "name": "test_project_2"}
        ]
        # Mock 'call_api' to return dummy sections
        mocked_call_api.side_effect = mock_call_api

        # Call function
        section = sections.Sections().get_objects()
        # 'get_objects' is generator, convert it to list
        list_section = list(section)

        # Collect expected data
        expected_data = []

        # Workspaces -> projects -> sections
        # As we have created dummy responses: 1 workspace, 2 projects, 3 sections
        # Hence we will get total 6 sections (3 for each project)
        for i in range(2):
            for d in sections_data():
                expected_data.append(d)

        # Verify the data we expected is returned
        self.assertEquals(list_section, expected_data)
        self.assertEquals(len(list_section), 6)

    # Verify the working if 'tasks' stream after caching Project IDs
    @mock.patch("tap_asana.streams.tasks.Tasks.get_objects")
    @mock.patch("tap_asana.streams.base.Stream.get_updated_session_bookmark")
    @mock.patch("tap_asana.streams.base.Stream.update_bookmark")
    def test_tasks(
        self,
        mocked_update_bookmark,
        mocked_get_updated_session_bookmark,
        mocked_get_objects,
        mocked_call_api,
        mocked_fetch_projects,
        mocked_fetch_workspaces,
        mocked_sleep,
        mocked_refresh_access_token,
    ):
        # Set config file
        Context.config = {'start_date': '2021-01-01T00:00:00Z'}
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')

        # Mock 'refresh_access_token' to return a valid token
        mocked_refresh_access_token.return_value = "mock_access_token"
        # Mock 'fetch_workspaces' to return dummy workspaces
        mocked_fetch_workspaces.return_value = [{"gid": 1, "name": "test_workspace"}]
        # Mock 'fetch_projects' to return dummy projects
        mocked_fetch_projects.return_value = [
            {"gid": 1, "name": "test_project_1"},
            {"gid": 2, "name": "test_project_2"}
        ]
        # Mock 'call_api' to return dummy tasks
        mocked_call_api.side_effect = mock_call_api
        # Mock 'get_updated_session_bookmark' to return a dummy bookmark
        mocked_get_updated_session_bookmark.return_value = "dummy_bookmark"
        # Mock 'update_bookmark' to do nothing
        mocked_update_bookmark.return_value = None
        # Mock 'Tasks.get_objects' to return dummy tasks
        mocked_get_objects.return_value = [
            {"gid": 1, "name": "test_task_1", "modified_at": "2021-01-01T00:00:00Z"},
            {"gid": 2, "name": "test_task_2", "modified_at": "2021-01-02T00:00:00Z"}
        ]

        # Call function
        task = tasks.Tasks().get_objects()
        # 'get_objects' is generator, convert it to list
        list_task = list(task)

        # Collect expected data
        expected_data = mocked_get_objects.return_value

        # Verify the data we expected is returned
        self.assertEquals(list_task, expected_data)
        self.assertEquals(len(list_task), len(expected_data))

    # Verify the working if 'stories' stream after caching Project IDs
    @mock.patch("tap_asana.streams.stories.Stories.get_objects")
    @mock.patch("tap_asana.streams.base.Stream.is_bookmark_old")
    @mock.patch("tap_asana.streams.base.Stream.get_updated_session_bookmark")
    @mock.patch("tap_asana.streams.base.Stream.update_bookmark")
    def test_stories(
        self,
        mocked_update_bookmark,
        mocked_get_updated_session_bookmark,
        mocked_is_bookmark_old,
        mocked_get_objects,
        mocked_call_api,
        mocked_fetch_projects,
        mocked_fetch_workspaces,
        mocked_sleep,
        mocked_refresh_access_token,
    ):
        # Set config file
        Context.config = {'start_date': '2021-01-01T00:00:00Z'}
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')

        # Mock 'refresh_access_token' to return a valid token
        mocked_refresh_access_token.return_value = "mock_access_token"
        # Mock 'call_api' function to return dummy stories
        mocked_call_api.side_effect = mock_call_api
        # Mock 'is_bookmark_old' to always return True
        mocked_is_bookmark_old.return_value = True
        # Mock 'get_updated_session_bookmark' to return a dummy bookmark
        mocked_get_updated_session_bookmark.return_value = "dummy_bookmark"
        # Mock 'update_bookmark' to do nothing
        mocked_update_bookmark.return_value = None
        # Mock 'Stories.get_objects' to return dummy stories
        mocked_get_objects.return_value = [
            {"gid": 1, "name": "test_data_1", "created_at": "2021-01-01"},
            {"gid": 2, "name": "test_data_2", "created_at": "2021-01-01"},
            {"gid": 3, "name": "test_data_3", "created_at": "2021-01-01"},
            {"gid": 1, "name": "test_data_1", "created_at": "2021-01-01"},
            {"gid": 2, "name": "test_data_2", "created_at": "2021-01-01"},
            {"gid": 3, "name": "test_data_3", "created_at": "2021-01-01"},
            {"gid": 1, "name": "test_data_1", "created_at": "2021-01-01"},
            {"gid": 2, "name": "test_data_2", "created_at": "2021-01-01"},
            {"gid": 3, "name": "test_data_3", "created_at": "2021-01-01"},
            {"gid": 1, "name": "test_data_1", "created_at": "2021-01-01"},
            {"gid": 2, "name": "test_data_2", "created_at": "2021-01-01"},
            {"gid": 3, "name": "test_data_3", "created_at": "2021-01-01"},
        ]

        # Call function
        story = stories.Stories().get_objects()
        # 'get_objects' is generator, convert it to list
        list_story = list(story)

        # Collect expected data
        expected_data = mocked_get_objects.return_value

        # Verify the data we expected is returned
        self.assertEquals(list_story, expected_data)
        self.assertEquals(len(list_story), len(expected_data))
