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
from tap_asana.streams.base import CollectionPageIterator
from parameterized import parameterized

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
    @mock.patch("tap_asana.streams.base.getattr")
    @mock.patch("time.sleep")
    def test_timeout_value_in_config__Base(self, name, actual_timeout, expected_timeout, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        """Test case to verify request_timeout sets as expected"""
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        # Initialize config file
        Context.config = {'request_timeout': actual_timeout}
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = valid_data

        workspaces = Context.stream_objects.get('workspaces')().call_api("workspaces")

        # Verify if the timeout was passed as expected from the config file
        mocked_find_all.find_all.assert_called_with(timeout=expected_timeout)

class TestTimeoutValuesPortfolios(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.portfolios = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.portfolios.get_portfolios.side_effect = valid_data
        Context.asana.client.portfolios.get_items_for_portfolio.side_effect = valid_data

    @parameterized.expand([
            ["string_request_timeout", "100", 100],
            ["int_request_timeout", 100, 100],
            ["empty_request_timeout", '', 300],
            ["zero_request_timeout", 0, 300],
            ["zero_string_request_timeout", '0.0', 300],
            ["no_request_timeout", None, 300],
            ["float_request_timeout", 100.5, 100.5],
        ])
    def test_request_timeout_(self, name, actual_timeout, expected_timeout):
        """Verify that request_timeout is set as expected for `portfolios` """
        Context.config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": actual_timeout} # integer timeout in config

        items_for_portfolios = list(Portfolios().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.portfolios.get_portfolios.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.portfolios.get_items_for_portfolio.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)

class TestTimeoutValuesProjects(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self,mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.projects = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.projects.find_all.side_effect = valid_data

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
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)

class TestTimeoutValuesSections(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.projects = mock.MagicMock()
        Context.asana.client.sections = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.projects.find_all.side_effect = valid_data
        Context.asana.client.sections.get_sections_for_project.side_effect = valid_data

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
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.sections.get_sections_for_project.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)

class TestTimeoutValuesStories(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.projects = mock.MagicMock()
        Context.asana.client.tasks = mock.MagicMock()
        Context.asana.client.stories = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.projects.find_all.side_effect = valid_data
        Context.asana.client.tasks.find_all.side_effect = valid_data
        Context.asana.client.stories.get_stories_for_task.side_effect = valid_data

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
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.tasks.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.stories.get_stories_for_task.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)

class TestTimeoutValuesTeams(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.teams = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.teams.find_by_organization.side_effect = valid_data
        Context.asana.client.teams.users.side_effect = valid_data

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
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.teams.find_by_organization.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)
        args, kwargs = Context.asana.client.teams.users.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("tap_asana.asana.asana.client.Client.get")
@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):

    def test_timeout_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
             Verify that get_initial function of SDK is retrying five time due to Timeout error
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        mocked_get.side_effect = raise_Timeout_error # raise Timeout

        try:
            iterator_object.get_initial()
        except requests.Timeout as e:
            self.assertEqual(mocked_get.call_count, 5)

    def test_timeout_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
              Verify that get_next function of SDK is retrying five time due to Timeout error
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        iterator_object.continuation = {"offset": "test"}
        mocked_get.side_effect = raise_Timeout_error # raise Timeout

        try:
            iterator_object.get_next()
        except requests.Timeout as e:
            self.assertEqual(mocked_get.call_count, 5)
