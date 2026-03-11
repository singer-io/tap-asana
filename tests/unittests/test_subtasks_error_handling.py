import unittest
from unittest import mock

import requests

from tap_asana.asana import Asana
from tap_asana.context import Context
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
    @mock.patch("asana.TasksApi.get_subtasks_for_task")
    def test_fetch_children_retries_on_timeout(self, mocked_get_subtasks, _):
        mocked_get_subtasks.side_effect = [requests.Timeout(), []]

        stream = SubTasks()
        result = stream.fetch_children({"gid": "task_1"}, "gid")

        self.assertEqual(result, [])
        self.assertEqual(mocked_get_subtasks.call_count, 2)
