import unittest
import asana
import oauthlib
import tap_asana
from unittest import mock
from tap_asana.context import Context
from tap_asana.streams.base import CollectionPageIterator


def no_authorized_error_raiser(*args, **kwargs):
    raise asana.error.NoAuthorizationError

def invalid_token_error_raiser(*args, **kwargs):
    raise asana.error.InvalidTokenError

def token_expired_error_raiser(*args, **kwargs):
    raise oauthlib.oauth2.rfc6749.errors.TokenExpiredError

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("tap_asana.asana.asana.client.Client.get")
@mock.patch("time.sleep")
class TestRefreshAccessToken(unittest.TestCase):

    def test_invalid_token_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
             Verify that refresh_access_token is called five time due to InvalidTokenError error of get_initial function of SDK
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        mocked_get.side_effect = invalid_token_error_raiser # Raise InvalidTokenError
        
        try:
            iterator_object.get_initial()
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 5)
            self.assertEqual(mocked_get.call_count, 5)

    def test_no_authorized_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called five time due to NoAuthorizationError error of get_initial function of SDK
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        mocked_get.side_effect = no_authorized_error_raiser # Raise NoAuthorizationError
        
        try:
            iterator_object.get_initial()
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 5)
            self.assertEqual(mocked_get.call_count, 5)

    def test_token_expired_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called five time due to TokenExpiredError error of get_initial function of SDK
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        mocked_get.side_effect = token_expired_error_raiser # Raise TokenExpiredError
        
        try:
            iterator_object.get_initial()
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 5)
            self.assertEqual(mocked_get.call_count, 5)

    def test_no_error_for_get_initial(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown for get_intial function of SDK
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        mocked_get.side_effect = "data"

        # Call get_initial function
        iterator_object.get_initial()

        self.assertEqual(mocked_refresh_access_token.call_count, 1)
        self.assertEqual(mocked_get.call_count, 1)

    def test_invalid_token_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
             Verify that refresh_access_token is called five time due to InvalidTokenError error of get_next function of SDK
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        iterator_object.continuation = {"offset": "test"}
        mocked_get.side_effect = invalid_token_error_raiser # Raise InvalidTokenError
        
        try:
            iterator_object.get_next()
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 5)
            self.assertEqual(mocked_get.call_count, 5)

    def test_no_authorized_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called five time due to NoAuthorizationError error of get_next function of SDK
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        iterator_object.continuation = {"offset": "test"}
        mocked_get.side_effect = no_authorized_error_raiser # Raise NoAuthorizationError
        
        try:
            iterator_object.get_next()
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 5)
            self.assertEqual(mocked_get.call_count, 5)

    def test_token_expired_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called five time due to TokenExpiredError error of get_next function of SDK
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        iterator_object.continuation = {"offset": "test"}
        mocked_get.side_effect = token_expired_error_raiser # Raise TokenExpiredError
        
        try:
            iterator_object.get_next()
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 5)
            self.assertEqual(mocked_get.call_count, 5)

    def test_no_error_for_get_next(self, mocked_sleep, mocked_get, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown for get_next function of SDK
        '''
        # Set asana client in Context before test
        Context.asana = tap_asana.Asana('test', 'test', 'test', 'test', 'test')
        # Set asana CollectionPageIterator object
        client = tap_asana.asana.asana.client.Client({})
        iterator_object = CollectionPageIterator(client, 'test', 'test', {})
        iterator_object.continuation = {"offset": "test"}
        mocked_get.side_effect = "data"

        # Call get_next function
        iterator_object.get_next()

        self.assertEqual(mocked_refresh_access_token.call_count, 1)
        self.assertEqual(mocked_get.call_count, 1)
