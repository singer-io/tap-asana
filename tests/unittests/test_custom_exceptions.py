import unittest
from unittest import mock
from tap_asana.streams.base import (
    Stream,
    InvalidTokenError,
    NoAuthorizationError,
    MAX_RETRIES,
)
from tap_asana.context import Context
from tap_asana.asana import Asana
import asana


class TestStreamExceptions(unittest.TestCase):
    def setUp(self):
        """Set up the Asana client and mock context."""
        Context.asana = Asana("test", "test", "test", "test", "test")

    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_invalid_token_error(self, mocked_get_workspaces):
        """Test call_api raises InvalidTokenError."""
        mocked_get_workspaces.side_effect = asana.rest.ApiException(status=412)

        stream = Stream()
        with self.assertRaises(InvalidTokenError):
            stream.call_api(asana.WorkspacesApi(Context.asana.client), "get_workspaces")

    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_no_authorization_error(self, mocked_get_workspaces):
        """Test call_api raises NoAuthorizationError."""
        mocked_get_workspaces.side_effect = asana.rest.ApiException(status=401)

        stream = Stream()
        with self.assertRaises(NoAuthorizationError):
            stream.call_api(asana.WorkspacesApi(Context.asana.client), "get_workspaces")

    @mock.patch("asana.ProjectsApi.get_projects")
    def test_fetch_projects_invalid_token_error(self, mocked_get_projects):
        """Test fetch_projects raises InvalidTokenError."""
        mocked_get_projects.side_effect = asana.rest.ApiException(status=412)

        stream = Stream()
        with self.assertRaises(InvalidTokenError):
            stream.fetch_projects(
                workspace_gid="123", opt_fields="name", request_timeout=300
            )

    @mock.patch("asana.ProjectsApi.get_projects")
    def test_fetch_projects_no_authorization_error(self, mocked_get_projects):
        """Test fetch_projects raises NoAuthorizationError."""
        mocked_get_projects.side_effect = asana.rest.ApiException(status=401)

        stream = Stream()
        with self.assertRaises(NoAuthorizationError):
            stream.fetch_projects(
                workspace_gid="123", opt_fields="name", request_timeout=300
            )

    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_fetch_workspaces_invalid_token_error(self, mocked_get_workspaces):
        """Test fetch_workspaces raises InvalidTokenError."""
        mocked_get_workspaces.side_effect = asana.rest.ApiException(status=412)

        stream = Stream()
        with self.assertRaises(InvalidTokenError):
            stream.fetch_workspaces()

    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_fetch_workspaces_no_authorization_error(self, mocked_get_workspaces):
        """Test fetch_workspaces raises NoAuthorizationError."""
        mocked_get_workspaces.side_effect = asana.rest.ApiException(status=401)

        stream = Stream()
        with self.assertRaises(NoAuthorizationError):
            stream.fetch_workspaces()


class TestRetryBehavior(unittest.TestCase):
    def setUp(self):
        """Set up the Asana client and mock context."""
        Context.asana = Asana("test", "test", "test", "test", "test")

    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_retry_invalid_token_error(self, mocked_get_workspaces):
        """Test call_api retries on InvalidTokenError."""
        # Raise InvalidTokenError for all retries
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=412)
        ] * MAX_RETRIES

        stream = Stream()
        with self.assertRaises(InvalidTokenError):
            stream.call_api(asana.WorkspacesApi(Context.asana.client), "get_workspaces")

        # Verify the method was retried MAX_RETRIES times
        self.assertEqual(mocked_get_workspaces.call_count, MAX_RETRIES)

    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_retry_no_authorization_error(self, mocked_get_workspaces):
        """Test call_api retries on NoAuthorizationError."""
        # Raise NoAuthorizationError for all retries
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=401)
        ] * MAX_RETRIES

        stream = Stream()
        with self.assertRaises(NoAuthorizationError):
            stream.call_api(asana.WorkspacesApi(Context.asana.client), "get_workspaces")

        # Verify the method was retried MAX_RETRIES times
        self.assertEqual(mocked_get_workspaces.call_count, MAX_RETRIES)

    @mock.patch("asana.WorkspacesApi.get_workspaces")
    def test_call_api_success_before_max_retries(self, mocked_get_workspaces):
        """Test call_api succeeds before reaching MAX_RETRIES."""
        # Raise InvalidTokenError for the first 2 calls, then succeed
        mocked_get_workspaces.side_effect = [
            asana.rest.ApiException(status=412),
            asana.rest.ApiException(status=412),
            [{"gid": "123", "name": "Workspace 1"}],
        ]

        stream = Stream()
        result = stream.call_api(
            asana.WorkspacesApi(Context.asana.client), "get_workspaces"
        )

        # Verify the method succeeded and returned the correct result
        self.assertEqual(result["data"], [{"gid": "123", "name": "Workspace 1"}])

        # Verify the method was retried 3 times (2 failures + 1 success)
        self.assertEqual(mocked_get_workspaces.call_count, 3)
