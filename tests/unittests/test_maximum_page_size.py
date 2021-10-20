import unittest
from unittest import mock
import tap_asana

class Args:
    def __init__(self, config):
        self.config = config
        self.state = {}
        self.discover = True

def get_args(config):
    return Args(config)

@mock.patch("tap_asana.streams.base.LOGGER.warning")
@mock.patch("tap_asana.discover")
@mock.patch("tap_asana.Asana")
@mock.patch("singer.utils.parse_args")
class TestMaximunPageSize(unittest.TestCase):

    # test case to verify that if page size greater 100 is passed in config then the tap gives warning
    def test_page_size_greater_than_100(self, mocked_args, mocked_asana, mocked_discover, mocked_logger_warning):
        # mock 'parse_args' and return dummy data
        mocked_args.return_value = get_args({
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "redirect_uri": "test_redirect_uri",
            "refresh_token": "test_refresh_token",
            "start_date" : "2021-01-01T00:00:00Z",
            "results_per_page": 105  # setting "results_per_page" in config file
        })
        mocked_discover.return_value = {}

        # function call
        tap_asana.main()

        # verify if the LOGGER.warn was called and verify the message, as the "results_per_page" is greater than 100
        self.assertEquals(mocked_logger_warning.call_count, 1)
        mocked_logger_warning.assert_called_with("The page size cannot be greater than 100, hence setting to maximum page size: 100.")

    # test case to verify that there should be no warning is case of page size less than 100
    def test_page_size_not_greater_than_100(self, mocked_args, mocked_asana, mocked_discover, mocked_logger_warning):
        # mock 'parse_args' and return dummy data
        mocked_args.return_value = get_args({
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "redirect_uri": "test_redirect_uri",
            "refresh_token": "test_refresh_token",
            "start_date" : "2021-01-01T00:00:00Z",
            "results_per_page": 90  # setting "results_per_page" in config file
        })
        mocked_discover.return_value = {}

        # function call
        tap_asana.main()

        # verify if the LOGGER.warn was not called, as the "results_per_page" is less than 100
        self.assertEquals(mocked_logger_warning.call_count, 0)
