import unittest
from unittest import mock

import requests

import tap_asana
from tap_asana.asana import Asana
from tap_asana.context import Context
from tap_asana.streams.base import CollectionPageIterator
from tap_asana.streams.portfolios import Portfolios
from tap_asana.streams.projects import Projects
from tap_asana.streams.sections import Sections
from tap_asana.streams.stories import Stories
from tap_asana.streams.teams import Teams


# raise requests.Timeout error
def raise_Timeout_error(*args, **kwargs):
    raise requests.Timeout


# Mock request output
def valid_data(*args, **kwargs):
    yield {
        "gid": "test",
        "modified_at": "2017-01-01T00:00:00Z",
        "created_at": "2017-01-01T00:00:00Z",
        "is_organization": True,
    }


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("tap_asana.streams.base.getattr")
@mock.patch("time.sleep")
class TestTimeoutErrorBase(unittest.TestCase):
    def test_timeout_value_not_in_config__Base(self, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # initialize config file
        Context.config = {}
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = valid_data

        Context.stream_objects.get("workspaces")().call_api("workspaces")

        # verify if the default timeout was passed as, timeout was not passed in the config
        mocked_find_all.find_all.assert_called_with(timeout=300.0)

    def test_timeout_value_in_config__Base(self, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # initialize config file
        Context.config = {"request_timeout": 100}
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = valid_data

        Context.stream_objects.get("workspaces")().call_api("workspaces")

        # verify if the timeout was passed as expected from the config file
        mocked_find_all.find_all.assert_called_with(timeout=100.0)

    def test_timeout_string_value_in_config__Base(self, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # initialize config file
        Context.config = {"request_timeout": "100"}
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = valid_data

        Context.stream_objects.get("workspaces")().call_api("workspaces")

        # verify if the timeout was passed as expected from the config file
        mocked_find_all.find_all.assert_called_with(timeout=100.0)

    def test_timeout_empty_value_in_config__Base(self, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # initialize config file
        Context.config = {"request_timeout": ""}
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = valid_data

        Context.stream_objects.get("workspaces")().call_api("workspaces")

        # verify if the timeout was passed as expected from the config file
        mocked_find_all.find_all.assert_called_with(timeout=300.0)

    def test_timeout_0_value_in_config__Base(self, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # initialize config file
        Context.config = {"request_timeout": 0.0}
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = valid_data

        Context.stream_objects.get("workspaces")().call_api("workspaces")

        # verify if the timeout was passed as expected from the config file
        mocked_find_all.find_all.assert_called_with(timeout=300.0)

    def test_timeout_string_0_value_in_config__Base(self, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana("test", "test", "test", "test", "test")
        # initialize config file
        Context.config = {"request_timeout": "0.0"}
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = valid_data

        Context.stream_objects.get("workspaces")().call_api("workspaces")

        # verify if the timeout was passed as expected from the config file
        mocked_find_all.find_all.assert_called_with(timeout=300.0)


class TestTimeoutValuesPortfolios(unittest.TestCase):
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana("test", "test", "test", "test", "test")
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.portfolios = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.portfolios.get_portfolios.side_effect = valid_data
        Context.asana.client.portfolios.get_items_for_portfolio.side_effect = valid_data

    def test_timeout_value_not_in_config(self):
        """Verify that if request_timeout is not provided in config then
        default value is used."""
        # initialize config file without request_timeout
        Context.config = {"start_date": "2017-01-01T00:00:00Z"}  # No timeout in config

        list(Portfolios().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.portfolios.get_portfolios.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.portfolios.get_items_for_portfolio.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_integer_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(integer value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100,
        }  # integer timeout in config

        list(Portfolios().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.portfolios.get_portfolios.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.portfolios.get_items_for_portfolio.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_float_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(float value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100.5,
        }  # float timeout in config

        list(Portfolios().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.portfolios.get_portfolios.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.portfolios.get_items_for_portfolio.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)

    def test_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(string value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "100",
        }  # string format timeout in config

        list(Portfolios().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.portfolios.get_portfolios.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.portfolios.get_items_for_portfolio.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_empty_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with empty
        string then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "",
        }  # empty string in config

        list(Portfolios().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.portfolios.get_portfolios.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.portfolios.get_items_for_portfolio.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero value
        then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 0.0,
        }  # zero value in config

        list(Portfolios().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.portfolios.get_portfolios.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.portfolios.get_items_for_portfolio.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero in
        string format then default value is used."""
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Portfolios().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.portfolios.get_portfolios.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.portfolios.get_items_for_portfolio.call_args
        self.assertEqual(kwargs.get("timeout"), 300)


class TestTimeoutValuesProjects(unittest.TestCase):
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana("test", "test", "test", "test", "test")
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.projects = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.projects.find_all.side_effect = valid_data

    def test_timeout_value_not_in_config(self):
        """Verify that if request_timeout is not provided in config then
        default value is used."""
        # initialize config file without request_timeout
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Projects().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_integer_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(integer value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100,
        }  # integer timeout in config

        list(Projects().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_float_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(float value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100.5,
        }  # float timeout in config

        list(Projects().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)

    def test_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(string value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "100",
        }  # string format timeout in config

        list(Projects().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_empty_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with empty
        string then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "",
        }  # empty string in config

        list(Projects().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero value
        then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 0.0,
        }  # zero value in config

        list(Projects().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero in
        string format then default value is used."""
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Projects().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)


class TestTimeoutValuesSections(unittest.TestCase):
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana("test", "test", "test", "test", "test")
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.projects = mock.MagicMock()
        Context.asana.client.sections = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.projects.find_all.side_effect = valid_data
        Context.asana.client.sections.get_sections_for_project.side_effect = valid_data

    def test_timeout_value_not_in_config(self):
        """Verify that if request_timeout is not provided in config then
        default value is used."""
        # initialize config file without request_timeout
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Sections().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.sections.get_sections_for_project.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_integer_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(integer value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100,
        }  # integer timeout in config

        list(Sections().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.sections.get_sections_for_project.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_float_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(float value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100.5,
        }  # float timeout in config

        list(Sections().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.sections.get_sections_for_project.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)

    def test_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(string value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "100",
        }  # string format timeout in config

        list(Sections().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.sections.get_sections_for_project.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_empty_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with empty
        string then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "",
        }  # empty string in config

        list(Sections().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.sections.get_sections_for_project.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero value
        then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 0.0,
        }  # zero value in config

        list(Sections().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.sections.get_sections_for_project.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero in
        string format then default value is used."""
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Sections().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.sections.get_sections_for_project.call_args
        self.assertEqual(kwargs.get("timeout"), 300)


class TestTimeoutValuesStories(unittest.TestCase):
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana("test", "test", "test", "test", "test")
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.projects = mock.MagicMock()
        Context.asana.client.tasks = mock.MagicMock()
        Context.asana.client.stories = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.projects.find_all.side_effect = valid_data
        Context.asana.client.tasks.find_all.side_effect = valid_data
        Context.asana.client.stories.get_stories_for_task.side_effect = valid_data

    def test_timeout_value_not_in_config(self):
        """Verify that if request_timeout is not provided in config then
        default value is used."""
        # initialize config file without request_timeout
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Stories().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.tasks.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.stories.get_stories_for_task.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_integer_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(integer value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100,
        }  # integer timeout in config

        list(Stories().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.tasks.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.stories.get_stories_for_task.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_float_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(float value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100.5,
        }  # float timeout in config

        list(Stories().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.tasks.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.stories.get_stories_for_task.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)

    def test_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(string value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "100",
        }  # string format timeout in config

        list(Stories().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.tasks.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.stories.get_stories_for_task.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_empty_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with empty
        string then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "",
        }  # empty string in config

        list(Stories().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.tasks.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.stories.get_stories_for_task.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero value
        then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 0.0,
        }  # zero value in config

        list(Stories().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.tasks.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.stories.get_stories_for_task.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero in
        string format then default value is used."""
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Stories().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.projects.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.tasks.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.stories.get_stories_for_task.call_args
        self.assertEqual(kwargs.get("timeout"), 300)


class TestTimeoutValuesTeams(unittest.TestCase):
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_access_token):
        # Set asana client in Context before test with mocked requests
        Context.asana = Asana("test", "test", "test", "test", "test")
        Context.asana.client.workspaces = mock.MagicMock()
        Context.asana.client.teams = mock.MagicMock()
        Context.asana.client.workspaces.find_all.side_effect = valid_data
        Context.asana.client.teams.find_by_organization.side_effect = valid_data
        Context.asana.client.teams.users.side_effect = valid_data

    def test_timeout_value_not_in_config(self):
        """Verify that if request_timeout is not provided in config then
        default value is used."""
        # initialize config file without request_timeout
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Teams().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.teams.find_by_organization.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.teams.users.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_integer_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(integer value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100,
        }  # integer timeout in config

        list(Teams().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.teams.find_by_organization.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.teams.users.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_float_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(float value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100.5,
        }  # float timeout in config

        list(Teams().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.teams.find_by_organization.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)
        args, kwargs = Context.asana.client.teams.users.call_args
        self.assertEqual(kwargs.get("timeout"), 100.5)

    def test_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config(string value)
        then it should be use."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "100",
        }  # string format timeout in config

        list(Teams().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.teams.find_by_organization.call_args
        self.assertEqual(kwargs.get("timeout"), 100)
        args, kwargs = Context.asana.client.teams.users.call_args
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_empty_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with empty
        string then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "",
        }  # empty string in config

        list(Teams().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.teams.find_by_organization.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.teams.users.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero value
        then default value is used."""
        Context.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 0.0,
        }  # zero value in config

        list(Teams().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.teams.find_by_organization.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.teams.users.call_args
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_string_request_timeout_in_config(self):
        """Verify that if request_timeout is provided in config with zero in
        string format then default value is used."""
        Context.config = {"start_date": "2018-04-11T00:00:00Z"}  # No timeout in config

        list(Teams().get_objects())

        # Verify requests is called with expected timeout
        args, kwargs = Context.asana.client.workspaces.find_all.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.teams.find_by_organization.call_args
        self.assertEqual(kwargs.get("timeout"), 300)
        args, kwargs = Context.asana.client.teams.users.call_args
        self.assertEqual(kwargs.get("timeout"), 300)


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("tap_asana.asana.asana.client.Client.get")
@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):
    def test_timeout_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """Verify that get_initial function of SDK is retrying five time due to
        Timeout error."""
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana("test", "test", "test", "test", "test")
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, "test", "test", {})
        mocked_get.side_effect = raise_Timeout_error  # raise Timeout

        try:
            iterator_object.get_initial()
        except requests.Timeout:
            self.assertEqual(mocked_get.call_count, 5)

    def test_timeout_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """Verify that get_next function of SDK is retrying five time due to
        Timeout error."""
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana("test", "test", "test", "test", "test")
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, "test", "test", {})
        iterator_object.continuation = {"offset": "test"}
        mocked_get.side_effect = raise_Timeout_error  # raise Timeout

        try:
            iterator_object.get_next()
        except requests.Timeout:
            self.assertEqual(mocked_get.call_count, 5)
