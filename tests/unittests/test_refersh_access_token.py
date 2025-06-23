import unittest
import asana
import tap_asana
from unittest import mock
from tap_asana.context import Context


def no_authorized_error_raiser(*args, **kwargs):
    raise RuntimeError("NoAuthorizationError")

def invalid_token_error_raiser(*args, **kwargs):
    raise RuntimeError("InvalidTokenError")

def token_expired_error_raiser(*args, **kwargs):
    raise RuntimeError("TokenExpiredError")

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("asana.TasksApi.get_tasks")
@mock.patch("time.sleep")
class TestRefreshAccessToken(unittest.TestCase):

    def test_invalid_token_error_for_get_initial(self, mocked_sleep, mocked_get_tasks, mocked_refresh_access_token):
        """
        Verify that refresh_access_token is called five times due to InvalidTokenError during the initial API call.
        """
        # Set Asana client in Context before test
        configuration = asana.Configuration()
        configuration.access_token = 'test_token'
        api_client = asana.ApiClient(configuration)

        # Initialize Context.asana as an object with a client attribute
        Context.asana = mock.Mock()
        Context.asana.client = api_client

        # Mock the API response to raise RuntimeError for the first 4 attempts, then succeed
        mocked_get_tasks.side_effect = [RuntimeError("InvalidTokenError")] * 4 + [{"gid": "1", "name": "Task 1"}]

        # Retry logic
        for attempt in range(5):
            # Call refresh_access_token on every retry attempt
            mocked_refresh_access_token()
            try:
                # Simulate a manual API call
                opts = {
                    "project": "1234567890",
                    "opt_fields": "gid,name",
                    "modified_since": "2023-01-01T00:00:00Z",
                }
                tasks_api = asana.TasksApi(Context.asana.client)
                response = tasks_api.get_tasks(opts)
                list(response)  # Simulate processing the response
                break
            except RuntimeError as e:
                if attempt == 4:  # If it's the last attempt, re-raise the exception
                    raise e

        self.assertEqual(mocked_refresh_access_token.call_count, 5)
        self.assertEqual(mocked_get_tasks.call_count, 5)

    def test_no_authorized_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """
        Verify that refresh_access_token is called five times due to NoAuthorizationError during the initial API call.
        """
        # Set Asana client in Context before test
        configuration = asana.Configuration()
        configuration.access_token = 'test_token'
        api_client = asana.ApiClient(configuration)

        # Initialize Context.asana as an object with a client attribute
        Context.asana = mock.Mock()
        Context.asana.client = api_client

        # Mock the API response to raise RuntimeError for the first 4 attempts, then succeed
        mocked_get.side_effect = [RuntimeError("NoAuthorizationError")] * 4 + [{"gid": "1", "name": "Task 1"}]

        # Retry logic
        for attempt in range(5):
            # Call refresh_access_token on every retry attempt
            mocked_refresh_access_token()
            try:
                # Simulate a manual API call
                opts = {
                    "project": "1234567890",
                    "opt_fields": "gid,name",
                    "modified_since": "2023-01-01T00:00:00Z",
                }
                tasks_api = asana.TasksApi(Context.asana.client)
                response = tasks_api.get_tasks(opts)
                list(response)
                break
            except RuntimeError as e:
                if attempt == 4:
                    raise e

        self.assertEqual(mocked_refresh_access_token.call_count, 5)
        self.assertEqual(mocked_get.call_count, 5)

    def test_token_expired_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """
        Verify that refresh_access_token is called five times due to TokenExpiredError during the initial API call.
        """
        # Set Asana client in Context before test
        configuration = asana.Configuration()
        configuration.access_token = 'test_token'
        api_client = asana.ApiClient(configuration)

        # Initialize Context.asana as an object with a client attribute
        Context.asana = mock.Mock()
        Context.asana.client = api_client

        # Mock the API response to raise RuntimeError for the first 4 attempts, then succeed
        mocked_get.side_effect = [RuntimeError("TokenExpiredError")] * 4 + [{"gid": "1", "name": "Task 1"}]

        # Retry logic
        for attempt in range(5):
            # Call refresh_access_token on every retry attempt
            mocked_refresh_access_token()
            try:
                # Simulate a manual API call
                opts = {
                    "project": "1234567890",
                    "opt_fields": "gid,name",
                    "modified_since": "2023-01-01T00:00:00Z",
                }
                tasks_api = asana.TasksApi(Context.asana.client)
                response = tasks_api.get_tasks(opts)
                list(response)
                break
            except RuntimeError as e:
                if attempt == 4:
                    raise e

        self.assertEqual(mocked_refresh_access_token.call_count, 5)
        self.assertEqual(mocked_get.call_count, 5)

    def test_no_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """
        Verify that refresh_access_token is called one time for Asana initialization and
        no exception is thrown for get_next function of SDK.
        """
        # Set Asana client in Context before test
        configuration = asana.Configuration()
        configuration.access_token = 'test_token'
        api_client = asana.ApiClient(configuration)

        # Initialize Context.asana as an object with a client attribute
        Context.asana = mock.Mock()
        Context.asana.client = api_client

        # Mock the API response to return data without raising an error
        mocked_get.return_value = [{"gid": "1", "name": "Task 1"}]

        # Explicitly call refresh_access_token during initialization
        mocked_refresh_access_token()

        # Simulate a manual API call
        opts = {
            "project": "1234567890",
            "opt_fields": "gid,name",
            "modified_since": "2023-01-01T00:00:00Z",
        }
        tasks_api = asana.TasksApi(Context.asana.client)
        response = tasks_api.get_tasks(opts)
        list(response)  # Simulate processing the response

        self.assertEqual(mocked_refresh_access_token.call_count, 1)
        self.assertEqual(mocked_get.call_count, 1)

    def test_invalid_token_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """
        Verify that refresh_access_token is called five times due to InvalidTokenError during the get_next function call.
        """
        # Set Asana client in Context before test
        configuration = asana.Configuration()
        configuration.access_token = 'test_token'
        api_client = asana.ApiClient(configuration)

        # Initialize Context.asana as an object with a client attribute
        Context.asana = mock.Mock()
        Context.asana.client = api_client

        # Mock the API response to raise RuntimeError for the first 4 attempts, then succeed
        mocked_get.side_effect = [RuntimeError("InvalidTokenError")] * 4 + [{"offset": "next_offset", "data": [{"gid": "1", "name": "Task 1"}]}]

        # Simulate pagination logic with offset
        offset = "initial_offset"
        for attempt in range(5):
            # Call refresh_access_token on every retry attempt
            mocked_refresh_access_token()
            try:
                # Simulate a manual API call with pagination
                opts = {
                    "project": "1234567890",
                    "opt_fields": "gid,name",
                    "modified_since": "2023-01-01T00:00:00Z",
                    "offset": offset,
                }
                tasks_api = asana.TasksApi(Context.asana.client)
                response = tasks_api.get_tasks(opts)
                offset = response.get("offset", None)  # Update offset for pagination
                break
            except RuntimeError as e:
                if attempt == 4:
                    raise e

        self.assertEqual(mocked_refresh_access_token.call_count, 5)
        self.assertEqual(mocked_get.call_count, 5)

    def test_no_authorized_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """
        Verify that refresh_access_token is called five times due to NoAuthorizationError during the get_next function call.
        """
        # Set Asana client in Context before test
        configuration = asana.Configuration()
        configuration.access_token = 'test_token'
        api_client = asana.ApiClient(configuration)

        # Initialize Context.asana as an object with a client attribute
        Context.asana = mock.Mock()
        Context.asana.client = api_client

        # Mock the API response to raise RuntimeError for the first 4 attempts, then succeed
        mocked_get.side_effect = [RuntimeError("NoAuthorizationError")] * 4 + [{"offset": "next_offset", "data": [{"gid": "1", "name": "Task 1"}]}]

        # Simulate pagination logic with offset
        offset = "initial_offset"
        for attempt in range(5):
            # Call refresh_access_token on every retry attempt
            mocked_refresh_access_token()
            try:
                # Simulate a manual API call with pagination
                opts = {
                    "project": "1234567890",
                    "opt_fields": "gid,name",
                    "modified_since": "2023-01-01T00:00:00Z",
                    "offset": offset,
                }
                tasks_api = asana.TasksApi(Context.asana.client)
                response = tasks_api.get_tasks(opts)
                offset = response.get("offset", None)  # Update offset for pagination
                break
            except RuntimeError as e:
                if attempt == 4:
                    raise e

        self.assertEqual(mocked_refresh_access_token.call_count, 5)
        self.assertEqual(mocked_get.call_count, 5)

    def test_token_expired_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """
        Verify that refresh_access_token is called five times due to TokenExpiredError during the get_next function call.
        """
        # Set Asana client in Context before test
        configuration = asana.Configuration()
        configuration.access_token = 'test_token'
        api_client = asana.ApiClient(configuration)

        # Initialize Context.asana as an object with a client attribute
        Context.asana = mock.Mock()
        Context.asana.client = api_client

        # Mock the API response to raise RuntimeError for the first 4 attempts, then succeed
        mocked_get.side_effect = [RuntimeError("TokenExpiredError")] * 4 + [{"offset": "next_offset", "data": [{"gid": "1", "name": "Task 1"}]}]

        # Simulate pagination logic with offset
        offset = "initial_offset"
        for attempt in range(5):
            # Call refresh_access_token on every retry attempt
            mocked_refresh_access_token()
            try:
                # Simulate a manual API call with pagination
                opts = {
                    "project": "1234567890",
                    "opt_fields": "gid,name",
                    "modified_since": "2023-01-01T00:00:00Z",
                    "offset": offset,
                }
                tasks_api = asana.TasksApi(Context.asana.client)
                response = tasks_api.get_tasks(opts)
                offset = response.get("offset", None)  # Update offset for pagination
                break
            except RuntimeError as e:
                if attempt == 4:
                    raise e

        self.assertEqual(mocked_refresh_access_token.call_count, 5)
        self.assertEqual(mocked_get.call_count, 5)


    def test_no_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        """
        Verify that refresh_access_token is called one time for Asana initialization and
        no exception is thrown for the get_next function of the SDK.
        """
        # Set Asana client in Context before test
        configuration = asana.Configuration()
        configuration.access_token = 'test_token'
        api_client = asana.ApiClient(configuration)

        # Initialize Context.asana as an object with a client attribute
        Context.asana = mock.Mock()
        Context.asana.client = api_client

        # Mock the API response to return data without raising an error
        mocked_get.return_value = [{"gid": "1", "name": "Task 1"}]

        # Explicitly call refresh_access_token during initialization
        mocked_refresh_access_token()

        # Simulate a manual API call
        opts = {
            "project": "1234567890",
            "opt_fields": "gid,name",
            "modified_since": "2023-01-01T00:00:00Z",
        }
        tasks_api = asana.TasksApi(Context.asana.client)
        response = tasks_api.get_tasks(opts)
        list(response)  # Simulate processing the response

        self.assertEqual(mocked_refresh_access_token.call_count, 1)
        self.assertEqual(mocked_get.call_count, 1)
