import unittest
from tap_asana.streams.base import InvalidTokenError, NoAuthorizationError

class TestCustomExceptions(unittest.TestCase):
    def test_invalid_token_error(self):
        """Test InvalidTokenError exception."""
        response_mock = {"message": "Invalid token"}
        exception = InvalidTokenError(response=response_mock)

        self.assertEqual(str(exception), "Invalid Token Error")
        self.assertEqual(exception.status_code, 412)
        self.assertEqual(exception.response, response_mock)

    def test_no_authorization_error(self):
        """Test NoAuthorizationError exception."""
        response_mock = {"message": "Unauthorized access"}
        exception = NoAuthorizationError(response=response_mock)

        self.assertEqual(str(exception), "No Authorization Error")
        self.assertEqual(exception.status_code, 401)
        self.assertEqual(exception.response, response_mock)
