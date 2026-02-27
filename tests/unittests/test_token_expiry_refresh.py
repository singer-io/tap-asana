import unittest
from unittest import mock
import asana
from tap_asana.streams.base import (
    Stream,
    InvalidTokenError,
    NoAuthorizationError,
    MAX_RETRIES,
)
from tap_asana.context import Context
from tap_asana.asana import Asana


class TestAccessTokenExpiryAndRefresh(unittest.TestCase):
    """
    Tests that when an access token expires (412/401), the retry logic
    refreshes the token and retries the API call successfully.
    """

    def setUp(self):
        """Set up the Asana client and mock context."""
        Context.asana = Asana("test", "test", "test", "test", "test")
        Context.config = {"start_date": "2021-01-01T00:00:00Z", "request_timeout": 300}

    def tearDown(self):
        """Close the Asana client to suppress ApiClient.__del__ warnings."""
        if hasattr(Context, "asana") and hasattr(Context.asana, "client") and Context.asana.client:
            try:
                Context.asana.client.pool.close()
                Context.asana.client.pool.join()
            except Exception:
                pass

    # ---------------------------------------------------------------
    # call_api: token expires then refresh succeeds
    # ---------------------------------------------------------------
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_token_expires_then_refresh_succeeds(
        self, mocked_get_workspaces, mocked_refresh
    ):
        """
        Simulate token expiry (412) on first 2 calls, then a successful
        response. Verify refresh_access_token is called on each retry and
        the final result is returned correctly.
        """
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=412),
            asana.rest.ApiException(status=412),
            [{"gid": "1", "name": "My Workspace"}],
        ]

        stream = Stream()
        result = stream.call_api(
            asana.WorkspacesApi(Context.asana.client), "get_workspaces"
        )

        # Should return data from the successful 3rd attempt
        self.assertEqual(result["data"], [{"gid": "1", "name": "My Workspace"}])
        # API was called 3 times (2 failures + 1 success)
        self.assertEqual(mocked_get_workspaces.call_count, 3)
        # refresh_access_token was called on each of the 2 retries
        self.assertEqual(mocked_refresh.call_count, 2)

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_401_then_refresh_succeeds(
        self, mocked_get_workspaces, mocked_refresh
    ):
        """
        Simulate 401 (NoAuthorization) on first call, then success.
        Verify refresh is called and result is correct.
        """
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=401),
            [{"gid": "1", "name": "My Workspace"}],
        ]

        stream = Stream()
        result = stream.call_api(
            asana.WorkspacesApi(Context.asana.client), "get_workspaces"
        )

        self.assertEqual(result["data"], [{"gid": "1", "name": "My Workspace"}])
        self.assertEqual(mocked_get_workspaces.call_count, 2)
        self.assertEqual(mocked_refresh.call_count, 1)

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_token_expires_all_retries_exhausted(
        self, mocked_get_workspaces, mocked_refresh
    ):
        """
        Simulate token expiry (412) on every attempt. Verify it retries
        MAX_RETRIES times and then raises InvalidTokenError.
        """
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=412)
        ] * MAX_RETRIES

        stream = Stream()
        with self.assertRaises(InvalidTokenError):
            stream.call_api(
                asana.WorkspacesApi(Context.asana.client), "get_workspaces"
            )

        self.assertEqual(mocked_get_workspaces.call_count, MAX_RETRIES)
        # refresh is called on every retry except the last (which gives up)
        self.assertEqual(mocked_refresh.call_count, MAX_RETRIES - 1)

    # ---------------------------------------------------------------
    # fetch_workspaces: token expires then refresh succeeds
    # ---------------------------------------------------------------
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_fetch_workspaces_token_expires_then_refresh_succeeds(
        self, mocked_get_workspaces, mocked_refresh
    ):
        """
        Simulate token expiry during fetch_workspaces, refresh succeeds,
        and workspaces are returned on retry.
        """
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=412),
            [{"gid": "100", "name": "Workspace A"}],
        ]

        stream = Stream()
        result = stream.fetch_workspaces()

        self.assertEqual(result, [{"gid": "100", "name": "Workspace A"}])
        self.assertEqual(mocked_get_workspaces.call_count, 2)
        self.assertEqual(mocked_refresh.call_count, 1)

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_fetch_workspaces_401_then_refresh_succeeds(
        self, mocked_get_workspaces, mocked_refresh
    ):
        """
        Simulate 401 during fetch_workspaces, refresh succeeds on retry.
        """
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=401),
            asana.rest.ApiException(status=401),
            [{"gid": "100", "name": "Workspace A"}],
        ]

        stream = Stream()
        result = stream.fetch_workspaces()

        self.assertEqual(result, [{"gid": "100", "name": "Workspace A"}])
        self.assertEqual(mocked_get_workspaces.call_count, 3)
        self.assertEqual(mocked_refresh.call_count, 2)

    # ---------------------------------------------------------------
    # fetch_projects: token expires then refresh succeeds
    # ---------------------------------------------------------------
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.ProjectsApi.get_projects")
    def test_fetch_projects_token_expires_then_refresh_succeeds(
        self, mocked_get_projects, mocked_refresh
    ):
        """
        Simulate token expiry during fetch_projects, refresh succeeds,
        and projects are returned on retry.
        """
        mocked_get_projects.side_effect = [
            asana.rest.ApiException(status=412),
            [{"gid": "200", "name": "Project X"}],
        ]

        stream = Stream()
        result = stream.fetch_projects(
            workspace_gid="123", opt_fields="name", request_timeout=300
        )

        self.assertEqual(result, [{"gid": "200", "name": "Project X"}])
        self.assertEqual(mocked_get_projects.call_count, 2)
        self.assertEqual(mocked_refresh.call_count, 1)

    # ---------------------------------------------------------------
    # Mixed: 412 then 401 then success
    # ---------------------------------------------------------------
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_mixed_token_errors_then_success(
        self, mocked_get_workspaces, mocked_refresh
    ):
        """
        Simulate a 412 (InvalidToken), then 401 (NoAuthorization), then
        success. Both errors trigger token refresh.
        """
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=412),
            asana.rest.ApiException(status=401),
            [{"gid": "1", "name": "Workspace"}],
        ]

        stream = Stream()
        result = stream.call_api(
            asana.WorkspacesApi(Context.asana.client), "get_workspaces"
        )

        self.assertEqual(result["data"], [{"gid": "1", "name": "Workspace"}])
        self.assertEqual(mocked_get_workspaces.call_count, 3)
        # refresh called once for the 412, once for the 401.
        # Note: backoff treats these as the same decorator group, so
        # the handler fires on each retry.
        self.assertEqual(mocked_refresh.call_count, 2)

    # ---------------------------------------------------------------
    # Verify refresh_access_token is actually invoked (not just mocked)
    # ---------------------------------------------------------------
    @mock.patch("requests.post")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_actually_calls_refresh_endpoint(
        self, mocked_get_workspaces, mocked_post
    ):
        """
        Verify that when a 412 occurs, the real refresh_access_token method
        is called which POSTs to the Asana OAuth token endpoint.
        """
        # First call: token expired, second call: success
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=412),
            [{"gid": "1", "name": "Workspace"}],
        ]

        # Mock the HTTP POST for token refresh to return a new token
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_test_token",
            "token_type": "bearer",
            "expires_in": 3600,
        }
        mocked_post.return_value = mock_response

        stream = Stream()
        result = stream.call_api(
            asana.WorkspacesApi(Context.asana.client), "get_workspaces"
        )

        self.assertEqual(result["data"], [{"gid": "1", "name": "Workspace"}])

        # Verify the refresh endpoint was actually called
        mocked_post.assert_called()
        call_args = mocked_post.call_args
        self.assertEqual(call_args[0][0], "https://app.asana.com/-/oauth_token")
        self.assertEqual(call_args[1]["data"]["grant_type"], "refresh_token")

    # ---------------------------------------------------------------
    # No token error - no refresh called
    # ---------------------------------------------------------------
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_no_token_error_no_refresh(
        self, mocked_get_workspaces, mocked_refresh
    ):
        """
        When the API call succeeds on the first try, refresh_access_token
        should NOT be called.
        """
        mocked_get_workspaces.return_value = [{"gid": "1", "name": "Workspace"}]

        stream = Stream()
        result = stream.call_api(
            asana.WorkspacesApi(Context.asana.client), "get_workspaces"
        )

        self.assertEqual(result["data"], [{"gid": "1", "name": "Workspace"}])
        self.assertEqual(mocked_get_workspaces.call_count, 1)
        mocked_refresh.assert_not_called()


if __name__ == "__main__":
    unittest.main()
