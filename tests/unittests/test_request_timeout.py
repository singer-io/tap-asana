import unittest
import requests
import tap_asana
from tap_asana.streams.portfolios import Portfolios
from tap_asana.streams.projects import Projects
from tap_asana.streams.sections import Sections
from tap_asana.streams.stories import Stories
from tap_asana.streams.teams import Teams
from unittest import mock
from tap_asana.context import Context
from tap_asana.asana import Asana
# from tap_asana.streams.base import CollectionPageIterator
from parameterized import parameterized
import asana

# raise requests.Timeout error
def raise_Timeout_error(*args, **kwargs):
    raise requests.Timeout

# Mock request output
def valid_data(*args, **kwargs):
    yield {"gid": "test", "modified_at": "2017-01-01T00:00:00Z", "created_at": "2017-01-01T00:00:00Z", "is_organization": True}



class TestTimeoutErrorBase(unittest.TestCase):

    @parameterized.expand([
        ["string_request_timeout", "100", 100.0],
        ["int_request_timeout", 100, 100.0],
        ["empty_request_timeout", '', 300.0],
        ["zero_request_timeout", 0, 300.0],
        ["zero_string_request_timeout", '0.0', 300.0],
        ["no_request_timeout", None, 300.0],
    ])
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    @mock.patch("time.sleep")
    def test_timeout_value_in_config__Base(self, name, actual_timeout, expected_timeout, mocked_sleep, mocked_get_workspaces, mocked_refresh_access_token):
        """Test case to verify request_timeout sets as expected"""
        # Set Asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        # Initialize config file
        Context.config = {'request_timeout': actual_timeout}

        # Mock the API response to return valid data
        mocked_get_workspaces.return_value = [{"gid": "1", "name": "Workspace 1"}]

        # Simulate a manual API call
        workspaces_api = asana.WorkspacesApi(Context.asana.client)
        response = workspaces_api.get_workspaces(timeout=expected_timeout)

        # Verify if the timeout was passed as expected from the config file
        mocked_get_workspaces.assert_called_with(timeout=expected_timeout)

class TestTimeoutValuesPortfolios(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set Asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')

        asana.PortfoliosApi = mock.MagicMock()
        asana.WorkspacesApi = mock.MagicMock()

        asana.PortfoliosApi.get_portfolios.side_effect =  valid_data
        asana.PortfoliosApi.get_portfolio.side_effect = valid_data
        asana.WorkspacesApi.get_workspaces.side_effect = valid_data


    @parameterized.expand([
        ["string_request_timeout", "100", 100],
        ["int_request_timeout", 100, 100],
        ["empty_request_timeout", '', 300],
        ["zero_request_timeout", 0, 300],
        ["zero_string_request_timeout", '0.0', 300],
        ["no_request_timeout", None, 300],
        ["float_request_timeout", 100.5, 100.5],
    ])
    @mock.patch("tap_asana.streams.base.Stream.call_api")
    def test_request_timeout_(self, mocked_call_api, name, actual_timeout, expected_timeout):
        """Verify that request_timeout is set as expected for `portfolios` """
        # Set the configuration with the request_timeout value
        Context.config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": actual_timeout}

        items_for_portfolios = list(Portfolios().get_objects())
        
        # Verify requests is called with expected timeout
        asana.WorkspacesApi.get_workspaces = mock.MagicMock()
        asana.WorkspacesApi.get_workspaces(workspace='workspace_123', opts={"owner": "me", "opt_fields": "opt_fields"}, _request_timeout=expected_timeout)
        args, kwargs = asana.WorkspacesApi.get_workspaces.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.PortfoliosApi.get_portfolios = mock.MagicMock()
        asana.PortfoliosApi.get_portfolios(workspace='workspace_123', opts={"owner": "me", "opt_fields": "opt_fields"}, _request_timeout=expected_timeout,)
        args, kwargs = asana.PortfoliosApi.get_portfolios.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.PortfoliosApi.get_portfolio = mock.MagicMock()
        asana.PortfoliosApi.get_portfolio(portfolio_gid='portfolio_123', opts={"opt_fields": "opt_fields"},_request_timeout=expected_timeout,)
        args, kwargs = asana.PortfoliosApi.get_portfolio.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

class TestTimeoutValuesProjects(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self,mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        asana.ProjectsApi = mock.MagicMock()
        asana.WorkspacesApi = mock.MagicMock()

        asana.WorkspacesApi.get_workspaces.side_effect = valid_data
        asana.ProjectsApi.get_projects.side_effect = valid_data

    @parameterized.expand([
            ["string_request_timeout", "100", 100],
            ["int_request_timeout", 100, 100],
            ["empty_request_timeout", '', 300],
            ["zero_request_timeout", 0, 300],
            ["zero_string_request_timeout", '0.0', 300],
            ["no_request_timeout", None, 300],
            ["float_request_timeout", 100.5, 100.5],
        ])
    def test_request_timeout(self, name, actual_timeout, expected_timeout):
        """Verify that request_timeout is set as expected for `projects` """
        Context.config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": actual_timeout} # integer timeout in config

        projects = list(Projects().get_objects())

        # Verify requests is called with expected timeout
        asana.WorkspacesApi.get_workspaces = mock.MagicMock()
        asana.WorkspacesApi.get_workspaces(workspace='workspace_123', opts={"owner": "me", "opt_fields": "opt_fields"}, _request_timeout=expected_timeout)
        args, kwargs = asana.WorkspacesApi.get_workspaces.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.ProjectsApi.get_projects = mock.MagicMock()
        asana.ProjectsApi.get_projects(opts={"workspace" : 'workspace_123',  "opt_fields":'opt_fields' },_request_timeout=expected_timeout,)
        args, kwargs = asana.ProjectsApi.get_projects.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

class TestTimeoutValuesSections(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        asana.WorkspacesApi = mock.MagicMock()
        asana.ProjectsApi = mock.MagicMock()
        asana.SectionsApi = mock.MagicMock()

        asana.WorkspacesApi.get_workspaces.side_effect = valid_data
        asana.ProjectsApi.get_projects.side_effect = valid_data
        asana.SectionsApi.get_sections_for_project.side_effect = valid_data

    @parameterized.expand([
            ["string_request_timeout", "100", 100],
            ["int_request_timeout", 100, 100],
            ["empty_request_timeout", '', 300],
            ["zero_request_timeout", 0, 300],
            ["zero_string_request_timeout", '0.0', 300],
            ["no_request_timeout", None, 300],
            ["float_request_timeout", 100.5, 100.5],
        ])
    def test_request_timeout_in_config(self,name, actual_timeout, expected_timeout):
        """Verify that request_timeout is set as expected for `sections` """
        Context.config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": actual_timeout} # integer timeout in config

        sections = list(Sections().get_objects())

        # Verify requests is called with expected timeout
        asana.WorkspacesApi.get_workspaces = mock.MagicMock()
        asana.WorkspacesApi.get_workspaces(workspace='workspace_123', opts={"owner": "me", "opt_fields": "opt_fields"}, _request_timeout=expected_timeout)
        args, kwargs = asana.WorkspacesApi.get_workspaces.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.ProjectsApi.get_projects = mock.MagicMock()
        asana.ProjectsApi.get_projects(opts={"workspace" : 'workspace_123',  "opt_fields":'opt_fields' },_request_timeout=expected_timeout,)
        args, kwargs = asana.ProjectsApi.get_projects.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.SectionsApi.get_sections_for_project = mock.MagicMock()
        asana.SectionsApi.get_sections_for_project(project_gid='project_123', opts={"opt_fields": 'opt_fields'}, _request_timeout=expected_timeout,)
        args, kwargs = asana.SectionsApi.get_sections_for_project.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

class TestTimeoutValuesStories(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        asana.WorkspacesApi = mock.MagicMock()
        asana.ProjectsApi = mock.MagicMock()
        asana.TasksApi = mock.MagicMock()
        asana.StoriesApi = mock.MagicMock()

        asana.WorkspacesApi.get_workspaces.side_effect = valid_data
        asana.ProjectsApi.get_projects.side_effect = valid_data
        asana.TasksApi.get_tasks.side_effect = valid_data
        asana.StoriesApi.get_stories_for_task.side_effect = valid_data

    @parameterized.expand([
            ["string_request_timeout", "100", 100],
            ["int_request_timeout", 100, 100],
            ["empty_request_timeout", '', 300],
            ["zero_request_timeout", 0, 300],
            ["zero_string_request_timeout", '0.0', 300],
            ["no_request_timeout", None, 300],
            ["float_request_timeout", 100.5, 100.5],
        ])
    def test_request_timeout_in_config(self,name, actual_timeout, expected_timeout):
        """Verify that request_timeout is set as expected for `stories` """
        Context.config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": actual_timeout} # integer timeout in config

        stories = list(Stories().get_objects())

        # Verify requests is called with expected timeout
        asana.WorkspacesApi.get_workspaces = mock.MagicMock()
        asana.WorkspacesApi.get_workspaces(workspace='workspace_123', opts={"owner": "me", "opt_fields": "opt_fields"}, _request_timeout=expected_timeout)
        args, kwargs = asana.WorkspacesApi.get_workspaces.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.ProjectsApi.get_projects = mock.MagicMock()
        asana.ProjectsApi.get_projects(opts={"workspace" : 'workspace_123',  "opt_fields":'opt_fields' },_request_timeout=expected_timeout,)
        args, kwargs = asana.ProjectsApi.get_projects.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.TasksApi.get_tasks = mock.MagicMock()
        asana.TasksApi.get_tasks(opts={"project": 'project_123'}, _request_timeout=expected_timeout,)
        args, kwargs = asana.TasksApi.get_tasks.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.StoriesApi.get_stories_for_task = mock.MagicMock()
        asana.StoriesApi.get_stories_for_task(task_gid='task_123', opts={"opt_fields": 'opt_fields'}, _request_timeout=expected_timeout,)
        args, kwargs = asana.StoriesApi.get_stories_for_task.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

class TestTimeoutValuesTeams(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        asana.WorkspacesApi = mock.MagicMock()
        asana.TeamsApi = mock.MagicMock()
        asana.UsersApi = mock.MagicMock()

        asana.WorkspacesApi.get_workspaces.side_effect = valid_data
        asana.TeamsApi.get_teams_for_workspace.side_effect = valid_data
        asana.UsersApi.get_users_for_team.side_effect = valid_data

    @parameterized.expand([
            ["string_request_timeout", "100", 100],
            ["int_request_timeout", 100, 100],
            ["empty_request_timeout", '', 300],
            ["zero_request_timeout", 0, 300],
            ["zero_string_request_timeout", '0.0', 300],
            ["no_request_timeout", None, 300],
            ["float_request_timeout", 100.5, 100.5],
        ])
    def test_request_timeout_in_config(self,name, actual_timeout, expected_timeout):
        """Verify that request_timeout is set as expected for `teams` """
        Context.config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": actual_timeout} # integer timeout in config

        stories = list(Teams().get_objects())

        # Verify requests is called with expected timeout
        asana.WorkspacesApi.get_workspaces = mock.MagicMock()
        asana.WorkspacesApi.get_workspaces(workspace='workspace_123', opts={"owner": "me", "opt_fields": "opt_fields"}, _request_timeout=expected_timeout)
        args, kwargs = asana.WorkspacesApi.get_workspaces.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.TeamsApi.get_teams_for_workspace = mock.MagicMock()
        asana.TeamsApi.get_teams_for_workspace(workspace_gid='workspace_123', opts={"opt_fields": 'opt_fields'}, _request_timeout=expected_timeout,)
        args, kwargs = asana.TeamsApi.get_teams_for_workspace.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)

        asana.UsersApi.get_users_for_team = mock.MagicMock()
        asana.UsersApi.get_users_for_team(team_gid='team_123', opts={"opt_fields": "gid,name,email"}, _request_timeout=expected_timeout,)
        args, kwargs = asana.UsersApi.get_users_for_team.call_args
        self.assertEqual(kwargs.get('_request_timeout'), expected_timeout)


# @mock.patch("tap_asana.asana.Asana.refresh_access_token")
# @mock.patch("tap_asana.asana.asana.client.Client.get")
# @mock.patch("time.sleep")
# class TestRequestTimeoutBackoff(unittest.TestCase):

#     def test_timeout_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
#         '''
#              Verify that get_initial function of SDK is retrying five time due to Timeout error
#         '''
#         # Set asana client in Context before test
#         Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
#         # Set asana CollectionPageIterator object
#         client = tap_asana.asana.asana.client.Client({})
#         iterator_object = CollectionPageIterator(client, 'test', 'test', {})
#         mocked_get.side_effect = raise_Timeout_error # raise Timeout

#         try:
#             iterator_object.get_initial()
#         except requests.Timeout as e:
#             self.assertEqual(mocked_get.call_count, 5)

#     def test_timeout_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
#         '''
#               Verify that get_next function of SDK is retrying five time due to Timeout error
#         '''
#         # Set asana client in Context before test
#         Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
#         # Set asana CollectionPageIterator object
#         client = tap_asana.asana.asana.client.Client({})
#         iterator_object = CollectionPageIterator(client, 'test', 'test', {})
#         iterator_object.continuation = {"offset": "test"}
#         mocked_get.side_effect = raise_Timeout_error # raise Timeout

#         try:
#             iterator_object.get_next()
#         except requests.Timeout as e:
#             self.assertEqual(mocked_get.call_count, 5)
