import unittest
from unittest import mock

import tap_asana.streams.sections as sections
import tap_asana.streams.stories as stories
import tap_asana.streams.tasks as tasks
from tap_asana.asana import Asana
from tap_asana.context import Context


# dummy 'sections' data
def sections_data(*args, **kwargs):
    return [
        {"gid": 1, "name": "test_data_1"},
        {"gid": 2, "name": "test_data_2"},
        {"gid": 3, "name": "test_data_3"},
    ]


# dummy 'stories' data
def stories_data(*args, **kwargs):
    return [
        {"gid": 1, "name": "test_data_1", "created_at": "2021-01-01"},
        {"gid": 2, "name": "test_data_2", "created_at": "2021-01-01"},
        {"gid": 3, "name": "test_data_3", "created_at": "2021-01-01"},
    ]


# mock 'call_api' function
def mock_call_api(*args, **kwargs):
    # # dummy 'workspaces' data
    if args[0] == "workspaces":
        return [{"gid": 1, "name": "test_workspace"}]
    # # dummy 'projects' data
    elif args[0] == "projects":
        return [
            {"gid": 1, "name": "test_project_1"},
            {"gid": 2, "name": "test_project_2"},
        ]
    # # dummy 'tasks' data
    elif args[0] == "tasks":
        return [
            {"gid": 1, "name": "test_task_1", "modified_at": "2021-01-01"},
            {"gid": 2, "name": "test_task_2", "modified_at": "2021-01-01"},
        ]


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("time.sleep")
@mock.patch("tap_asana.streams.base.Stream.call_api")
class TestProjectIdCaching(unittest.TestCase):

    # verify the working if 'sections' stream after caching Project IDs
    def test_sections(self, mocked_call_api, mocked_sleep, mocked_refresh_access_token):
        # set config file
        Context.config = {"start_date": "2021-01-01T00:00:00Z"}
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # mock 'call_api' function
        mocked_call_api.side_effect = mock_call_api
        # mock and return 'sections' data
        Context.asana.client.sections.get_sections_for_project = sections_data

        # call function
        section = sections.Sections().get_objects()
        # 'get_objects' is generator, convert it to list
        list_section = list(section)

        # collect expected data
        expected_data = []

        # workspaces -> projects -> sections
        # as we have created dummy responses: 1 workspace, 2 projects, 3 sections
        # hence we will get total 6 sections (3 for each project)
        for i in range(2):
            for d in sections_data():
                expected_data.append(d)

        # verify the data we expected is returned
        self.assertEqual(list_section, expected_data)
        self.assertEqual(len(list_section), 6)

    # verify the working if 'tasks' stream after caching Project IDs
    @mock.patch("tap_asana.streams.base.Stream.get_updated_session_bookmark")
    @mock.patch("tap_asana.streams.base.Stream.update_bookmark")
    def test_tasks(
        self,
        mocked_update_bookmark,
        mocked_get_updated_session_bookmark,
        mocked_call_api,
        mocked_sleep,
        mocked_refresh_access_token,
    ):
        # set config file
        Context.config = {"start_date": "2021-01-01T00:00:00Z"}
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # mock 'call_api' function
        mocked_call_api.side_effect = mock_call_api

        # call function
        task = tasks.Tasks().get_objects()
        # 'get_objects' is generator, convert it to list
        list_task = list(task)

        # collect expected data
        expected_data = []

        # workspaces -> projects -> tasks
        # as we have created dummy responses: 1 workspace, 2 projects, 2 tasks
        # hence we will get total 4 tasks (2 for each project)
        for i in range(2):
            for d in mock_call_api("tasks"):
                expected_data.append(d)

        # verify the data we expected is returned
        self.assertEqual(list_task, expected_data)
        self.assertEqual(len(list_task), 4)

    # verify the working if 'stories' stream after caching Project IDs
    @mock.patch("tap_asana.streams.base.Stream.is_bookmark_old")
    def test_stories(
        self,
        mocked_is_bookmark_old,
        mocked_call_api,
        mocked_sleep,
        mocked_refresh_access_token,
    ):
        # set config file
        Context.config = {"start_date": "2021-01-01T00:00:00Z"}
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # mock 'call_api' function
        mocked_call_api.side_effect = mock_call_api
        # mock and return 'stories' data
        Context.asana.client.stories.get_stories_for_task = stories_data
        mocked_is_bookmark_old.return_value = True

        # call function
        task = stories.Stories().get_objects()
        # 'get_objects' is generator, convert it to list
        list_task = list(task)

        # collect expected data
        expected_data = []

        # workspaces -> projects -> tasks -> stories
        # as we have created dummy responses: 1 workspace, 2 projects, 2 tasks, 3 stories
        # hence we will get total 12 stories (2 tasks for each project, 3 stories for each task)
        for i in range(4):
            for d in stories_data():
                expected_data.append(d)

        self.assertEqual(list_task, expected_data)
        self.assertEqual(len(list_task), 12)
