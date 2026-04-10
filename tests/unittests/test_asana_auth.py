import unittest
from unittest import mock

from tap_asana.asana import Asana


class TestAsanaAuth(unittest.TestCase):
    def tearDown(self):
        if hasattr(self, "asana_client") and self.asana_client.client:
            try:
                self.asana_client.client.pool.close()
                self.asana_client.client.pool.join()
            except Exception:
                pass

    @mock.patch("tap_asana.asana.requests.post")
    def test_refresh_access_token_success_updates_client_and_token(self, mocked_post):
        response = mock.Mock()
        response.status_code = 200
        response.json.return_value = {"access_token": "new_token"}
        mocked_post.return_value = response

        self.asana_client = Asana("id", "secret", "uri", "refresh", "old_token")

        token = self.asana_client.refresh_access_token()

        self.assertEqual(token, "new_token")
        self.assertEqual(self.asana_client.access_token, "new_token")
        self.assertIsNotNone(self.asana_client.client)
        # The live ApiClient must also reflect the new token immediately
        self.assertEqual(self.asana_client.client.configuration.access_token, "new_token")

    @mock.patch("tap_asana.asana.requests.post")
    def test_refresh_access_token_missing_access_token_returns_none(self, mocked_post):
        response = mock.Mock()
        response.status_code = 200
        response.json.return_value = {"token_type": "bearer"}
        mocked_post.return_value = response

        self.asana_client = Asana("id", "secret", "uri", "refresh", "old_token")

        token = self.asana_client.refresh_access_token()

        self.assertIsNone(token)
        self.assertEqual(self.asana_client.access_token, "old_token")

    @mock.patch("tap_asana.asana.requests.post")
    def test_refresh_access_token_non_200_returns_none(self, mocked_post):
        response = mock.Mock()
        response.status_code = 401
        response.text = '{"errors":[{"message":"expired"}]}'
        mocked_post.return_value = response

        self.asana_client = Asana("id", "secret", "uri", "refresh", "old_token")

        token = self.asana_client.refresh_access_token()

        self.assertIsNone(token)
        self.assertEqual(self.asana_client.access_token, "old_token")
