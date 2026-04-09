import unittest
import asana
from unittest import mock
from tap_asana.context import Context
from tap_asana.streams.base import Stream, NoAuthorizationError, MAX_RETRIES


class TestCallApiTokenRefresh(unittest.TestCase):
    """Tests for the 401 token-refresh retry logic via invalid_token_handler / asana_error_handling."""

    def _make_401(self):
        exc = asana.rest.ApiException(status=401, reason="Unauthorized")
        exc.status = 401
        return exc

    def setUp(self):
        configuration = asana.Configuration()
        configuration.access_token = "old_token"
        self.api_client = asana.ApiClient(configuration)
        Context.asana = mock.Mock()
        Context.asana.client = self.api_client
        Context.asana.refresh_access_token.return_value = "new_token"

    def tearDown(self):
        if hasattr(Context, "asana") and hasattr(Context.asana, "client") and Context.asana.client:
            try:
                Context.asana.client.pool.close()
                Context.asana.client.pool.join()
            except Exception:
                pass

    @mock.patch("asana.TasksApi.get_tasks")
    def test_refreshes_token_on_401_and_retries_successfully(self, mocked_get_tasks):
        """On 401, backoff calls invalid_token_handler which patches the ApiClient
        in-place with the new token. The retry then succeeds."""
        success_response = [{"gid": "1", "name": "Task 1"}]
        mocked_get_tasks.side_effect = [self._make_401(), iter(success_response)]

        stream = Stream()
        tasks_api = asana.TasksApi(Context.asana.client)
        result = stream.call_api(tasks_api, "get_tasks", opts={})

        # Token was refreshed exactly once (one backoff before the successful retry)
        Context.asana.refresh_access_token.assert_called_once()
        # ApiClient was patched in-place with the new token
        self.assertEqual(Context.asana.client.configuration.access_token, "new_token")
        self.assertEqual(mocked_get_tasks.call_count, 2)
        self.assertEqual(result["data"], success_response)

    @mock.patch("asana.TasksApi.get_tasks")
    def test_client_token_not_updated_when_refresh_returns_none(self, mocked_get_tasks):
        """When refresh_access_token returns None, the ApiClient token must NOT be
        changed and NoAuthorizationError must eventually be raised."""
        mocked_get_tasks.side_effect = [self._make_401()] * MAX_RETRIES
        Context.asana.refresh_access_token.return_value = None

        stream = Stream()
        tasks_api = asana.TasksApi(Context.asana.client)
        with self.assertRaises(NoAuthorizationError):
            stream.call_api(tasks_api, "get_tasks", opts={})

        # ApiClient token must remain unchanged since refresh returned None
        self.assertEqual(Context.asana.client.configuration.access_token, "old_token")
        self.assertGreaterEqual(Context.asana.refresh_access_token.call_count, 1)

    @mock.patch("asana.TasksApi.get_tasks")
    def test_raises_no_authorization_error_after_exhausting_retries(self, mocked_get_tasks):
        """When every retry returns a 401 (even after token refresh),
        NoAuthorizationError is raised after MAX_RETRIES attempts."""
        mocked_get_tasks.side_effect = [self._make_401()] * MAX_RETRIES

        stream = Stream()
        tasks_api = asana.TasksApi(Context.asana.client)
        with self.assertRaises(NoAuthorizationError):
            stream.call_api(tasks_api, "get_tasks", opts={})

        # on_backoff fires MAX_RETRIES-1 times (after each attempt except the last)
        self.assertEqual(Context.asana.refresh_access_token.call_count, MAX_RETRIES - 1)
        self.assertEqual(mocked_get_tasks.call_count, MAX_RETRIES)
