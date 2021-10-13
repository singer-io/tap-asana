import unittest
from unittest import mock
from tap_asana.streams.base import Stream
from tap_asana import context

@mock.patch("tap_asana.streams.base.LOGGER.warn")
class TestMaximunPageSize(unittest.TestCase):

    # test case to verify that if page size greater 100 is passed in config then the tap gives warning
    def test_page_size_greater_than_100(self, mocked_logger_warn):
        # setting "results_per_page" in config file
        context.Context.config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "redirect_uri": "test_redirect_uri",
            "refresh_token": "test_refresh_token",
            "results_per_page": 104
        }
        # initializing Stream class
        Stream()

        # verify if the LOGGER.warn was called and verify the message, as the "results_per_page" is greater than 100
        self.assertEquals(mocked_logger_warn.call_count, 1)
        mocked_logger_warn.assert_called_with("The page size cannot be greater than 100, hence setting to maximum page size: 100.")

    # test case to verify that there should be no warning is case of page size less than 100
    def test_page_size_not_greater_than_100(self, mocked_logger_warn):
        # setting "results_per_page" in config file
        context.Context.config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "redirect_uri": "test_redirect_uri",
            "refresh_token": "test_refresh_token",
            "results_per_page": 99
        }
        # initializing Stream class
        Stream()

        # verify if the LOGGER.warn was not called, as the "results_per_page" is less than 100
        self.assertEquals(mocked_logger_warn.call_count, 0)
