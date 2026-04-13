import unittest
import asana
from unittest import mock
from tap_asana.asana import Asana
from tap_asana.context import Context
from tap_asana.streams.base import Stream, NoAuthorizationError, MAX_RETRIES


def _post_response(status_code, body=None):
    """Helper to build a mocked requests.post response."""
    resp = mock.Mock()
    resp.status_code = status_code
    resp.json.return_value = body or {}
    return resp


@mock.patch("time.sleep")  # prevent backoff from incurring real sleep delays
class TestCallApiTokenRefresh(unittest.TestCase):
    """
    End-to-end tests for the 401 token-refresh flow, using a real Asana instance
    and mocked HTTP calls - matching the exact scenario from the reported error.

    Reported error scenario:
        The bearer token expired mid-sync. The tap raised NoAuthorizationError
        and terminated because refresh_access_token() was called but the new token
        was never applied to the live ApiClient, so every retry still used the
        expired token.

    Fix:
        refresh_access_token() now also writes the new token directly into
        self._client.configuration.access_token, so the shared ApiClient used by
        all stream API instances immediately picks up the refreshed credentials.
    """

    def _make_401(self):
        exc = asana.rest.ApiException(status=401, reason="Unauthorized")
        exc.status = 401
        return exc

    def setUp(self):
        # Simulate the tap starting with an access token that is about to expire.
        self.asana_instance = Asana("client_id", "client_secret", "redirect_uri", "refresh_tok", "old_token")
        Context.asana = self.asana_instance

    def tearDown(self):
        if self.asana_instance.client:
            try:
                self.asana_instance.client.pool.close()
                self.asana_instance.client.pool.join()
            except Exception:
                pass

    @mock.patch("tap_asana.asana.requests.post")
    @mock.patch("asana.TasksApi.get_tasks")
    def test_refreshes_token_on_401_and_retries_successfully(self, mocked_get_tasks, mocked_post, mocked_sleep):
        """
        Reproduces the reported error scenario:
          1. First API call returns 401 (token expired mid-sync).
          2. Backoff calls invalid_token_handler -> refresh_access_token().
          3. refresh_access_token() fetches a new token AND updates the live ApiClient.
          4. Retry succeeds because the ApiClient now carries the new token.
        """
        mocked_post.return_value = _post_response(200, {"access_token": "new_token"})

        success_response = [{"gid": "1", "name": "Task 1"}]
        mocked_get_tasks.side_effect = [self._make_401(), iter(success_response)]

        stream = Stream()
        tasks_api = asana.TasksApi(Context.asana.client)
        result = stream.call_api(tasks_api, "get_tasks", opts={})

        # Token endpoint was hit exactly once (one backoff before the successful retry)
        mocked_post.assert_called_once()
        # The live ApiClient must now carry the refreshed token
        self.assertEqual(Context.asana.client.configuration.access_token, "new_token")
        self.assertEqual(Context.asana.access_token, "new_token")
        # First call failed (401), second call succeeded
        self.assertEqual(mocked_get_tasks.call_count, 2)
        self.assertEqual(result["data"], success_response)

    @mock.patch("tap_asana.asana.requests.post")
    @mock.patch("asana.TasksApi.get_tasks")
    def test_client_token_not_updated_when_refresh_fails(self, mocked_get_tasks, mocked_post, mocked_sleep):
        """
        When the token endpoint returns a non-200 (refresh fails), the ApiClient
        token must remain unchanged and NoAuthorizationError must eventually be raised.
        """
        mocked_post.return_value = _post_response(401)
        mocked_get_tasks.side_effect = [self._make_401()] * MAX_RETRIES

        stream = Stream()
        tasks_api = asana.TasksApi(Context.asana.client)
        with self.assertRaises(NoAuthorizationError):
            stream.call_api(tasks_api, "get_tasks", opts={})

        # Refresh was attempted on each backoff
        self.assertGreaterEqual(mocked_post.call_count, 1)
        # ApiClient token must remain the original expired token
        self.assertEqual(Context.asana.client.configuration.access_token, "old_token")
        self.assertEqual(Context.asana.access_token, "old_token")

    @mock.patch("tap_asana.asana.requests.post")
    @mock.patch("asana.TasksApi.get_tasks")
    def test_raises_no_authorization_error_after_exhausting_retries(self, mocked_get_tasks, mocked_post, mocked_sleep):
        """
        When every API call returns 401 even after a successful token refresh
        (e.g. the new token is also rejected), NoAuthorizationError is raised
        after MAX_RETRIES attempts.
        """
        mocked_post.return_value = _post_response(200, {"access_token": "new_token"})
        mocked_get_tasks.side_effect = [self._make_401()] * MAX_RETRIES

        stream = Stream()
        tasks_api = asana.TasksApi(Context.asana.client)
        with self.assertRaises(NoAuthorizationError):
            stream.call_api(tasks_api, "get_tasks", opts={})

        # on_backoff fires MAX_RETRIES-1 times (once after each attempt except the last)
        self.assertEqual(mocked_post.call_count, MAX_RETRIES - 1)
        self.assertEqual(mocked_get_tasks.call_count, MAX_RETRIES)
