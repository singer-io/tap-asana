import unittest
from unittest import mock

import asana
import requests

from tap_asana.asana import Asana
from tap_asana.context import Context
from tap_asana.streams.base import InvalidTokenError, MAX_RETRIES, NoAuthorizationError
from tap_asana.streams.subtasks import SubTasks


class TestSubTasksErrorHandling(unittest.TestCase):
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, mocked_refresh_access_token):
        Context.asana = Asana("test", "test", "test", "test", "test")

    def tearDown(self):
        if hasattr(Context, "asana") and hasattr(Context.asana, "client") and Context.asana.client:
            try:
                Context.asana.client.pool.close()
                Context.asana.client.pool.join()
            except Exception:
                pass

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("tap_asana.streams.subtasks.asana.TasksApi")
    def test_fetch_children_retries_on_timeout(self, mocked_tasks_api, _):
        mocked_tasks_api.return_value.get_subtasks_for_task.side_effect = [requests.Timeout(), []]

        stream = SubTasks()
        result = stream.fetch_children({"gid": "task_1"}, "gid")

        self.assertEqual(result, [])
        self.assertEqual(mocked_tasks_api.return_value.get_subtasks_for_task.call_count, 2)

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("tap_asana.streams.subtasks.asana.TasksApi")
    def test_fetch_children_retries_and_refreshes_token_on_401(
        self, mocked_tasks_api, mocked_refresh_token, _
    ):
        """
        Verify the fix for the production failure:
        When the OAuth bearer token expires mid-sync, asana.rest.ApiException(401)
        is raised by the Asana page iterator.  The fix in asana_error_handling's
        wrapper catches it, converts it to NoAuthorizationError, which triggers
        the backoff decorator to call refresh_access_token() before each retry.
        After MAX_RETRIES exhausted, NoAuthorizationError (not the raw
        ApiException) is raised.
        """
        mocked_tasks_api.return_value.get_subtasks_for_task.side_effect = (
            asana.rest.ApiException(status=401, reason="Unauthorized")
        )
        mocked_refresh_token.return_value = "new_access_token"

        stream = SubTasks()

        # After the fix: NoAuthorizationError is raised (not raw ApiException)
        # meaning the wrapper correctly converted and the backoff triggered retries.
        with self.assertRaises(NoAuthorizationError):
            stream.fetch_children({"gid": "task_1"}, "gid")

        # Backoff retried MAX_RETRIES times total
        self.assertEqual(
            mocked_tasks_api.return_value.get_subtasks_for_task.call_count,
            MAX_RETRIES,
        )

        # refresh_access_token was called on each backoff (MAX_RETRIES - 1 times)
        self.assertEqual(
            mocked_refresh_token.call_count,
            MAX_RETRIES - 1,
            "refresh_access_token should be called on each backoff retry",
        )

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    @mock.patch("tap_asana.streams.subtasks.asana.TasksApi")
    def test_fetch_children_retries_and_refreshes_token_on_412(
        self, mocked_tasks_api, mocked_refresh_token, _
    ):
        """
        Verify that ApiException(412) raised by the SDK iterator inside
        fetch_children is also converted to InvalidTokenError by the wrapper,
        triggering the same backoff/refresh behaviour as the 401 case.
        """
        mocked_tasks_api.return_value.get_subtasks_for_task.side_effect = (
            asana.rest.ApiException(status=412, reason="Precondition Failed")
        )
        mocked_refresh_token.return_value = "new_access_token"

        stream = SubTasks()

        # InvalidTokenError (not raw ApiException) should be raised after retries
        with self.assertRaises(InvalidTokenError):
            stream.fetch_children({"gid": "task_1"}, "gid")

        # Backoff retried MAX_RETRIES times total
        self.assertEqual(
            mocked_tasks_api.return_value.get_subtasks_for_task.call_count,
            MAX_RETRIES,
        )

        # refresh_access_token was called on each backoff (MAX_RETRIES - 1 times)
        self.assertEqual(
            mocked_refresh_token.call_count,
            MAX_RETRIES - 1,
            "refresh_access_token should be called on each backoff retry",
        )
