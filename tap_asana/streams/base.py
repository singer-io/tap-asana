import math
import functools
import sys

import requests
import backoff
import simplejson
import singer
from asana.error import NoAuthorizationError, RetryableAsanaError, InvalidTokenError, RateLimitEnforcedError
from asana.page_iterator import CollectionPageIterator
from oauthlib.oauth2 import TokenExpiredError
from singer import utils
from tap_asana.context import Context


LOGGER = singer.get_logger()

# Setting default timeout as 300 second
REQUEST_TIMEOUT = 300

# Retry the request in the factor of 2 ie. 2, 4, 8, ...
FACTOR = 2

RESULTS_PER_PAGE = 250

# We've observed 500 errors returned if this is too large (30 days was too
# large for a customer)
DATE_WINDOW_SIZE = 1

# We will retry a 500 error a maximum of 5 times before giving up
MAX_RETRIES = 5


def is_not_status_code_fn(status_code):
    def gen_fn(exc):
        if getattr(exc, 'code', None) and exc.code not in status_code:
            return True
        # Retry other errors up to the max
        return False
    return gen_fn


def leaky_bucket_handler(details):
    LOGGER.info("Received 429 -- sleeping for %s seconds",
                details['wait'])


def retry_handler(details):
    LOGGER.info("Received 500 or retryable error -- Retry %s/%s",
                details['tries'], MAX_RETRIES)


#pylint: disable=unused-argument
def retry_after_wait_gen(**kwargs):
    # This is called in an except block so we can retrieve the exception
    # and check it.
    exc_info = sys.exc_info()
    resp = exc_info[1].response
    # Retry-After is an undocumented header. But honoring
    # it was proven to work in our spikes.
    sleep_time_str = resp.headers.get('Retry-After')
    yield math.floor(float(sleep_time_str))


def invalid_token_handler(details):
    LOGGER.info("Received invalid or expired token error, refreshing access token")
    Context.asana.refresh_access_token()


def asana_error_handling(fnc):
    @backoff.on_exception(backoff.expo,
                          requests.Timeout,
                          max_tries=MAX_RETRIES,
                          factor=FACTOR)
    @backoff.on_exception(backoff.expo,
                          (InvalidTokenError,
                          NoAuthorizationError,
                          TokenExpiredError),
                          on_backoff=invalid_token_handler,
                          max_tries=MAX_RETRIES)
    @backoff.on_exception(backoff.expo,
                          (simplejson.scanner.JSONDecodeError,
                          RetryableAsanaError),
                          giveup=is_not_status_code_fn(range(500, 599)),
                          on_backoff=retry_handler,
                          max_tries=MAX_RETRIES)
    @backoff.on_exception(retry_after_wait_gen,
                          RateLimitEnforcedError,
                          giveup=is_not_status_code_fn([429]),
                          on_backoff=leaky_bucket_handler,
                          # No jitter as we want a constant value
                          jitter=None)
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)
    return wrapper

# Added decorator over functions of asana SDK as functions from SDK returns generator and
# tap is yielding data from that function so backoff is not working over tap functions.
# Decorator can be put above get_objects() functions of every stream file but
# it has multiple for loops so it's expensive to backoff everything.
CollectionPageIterator.get_initial = asana_error_handling(CollectionPageIterator.get_initial)
CollectionPageIterator.get_next = asana_error_handling(CollectionPageIterator.get_next)

class Stream():
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_method = None
    replication_key = None
    key_properties = ['gid']
    # Controls which SDK object we use to call the API by default.

    def __init__(self):
        # Set request timeout to config param `request_timeout` value.
        config_request_timeout = Context.config.get('request_timeout')
        # If value is 0, "0", "" or not passed then it sets default to 300 seconds.
        if config_request_timeout and float(config_request_timeout):
            self.request_timeout = float(config_request_timeout)
        else:
            self.request_timeout = REQUEST_TIMEOUT

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_to_utc(bookmark)


    def is_bookmark_old(self, value):
        bookmark = self.get_bookmark()
        return utils.strptime_to_utc(value) >= bookmark


    def update_bookmark(self, value):
        # NOTE: Bookmarking can never be updated to not get the most
        # recent thing it saw the next time you run, because the querying
        # only allows greater than or equal semantics.

        try:
            value = value.strftime("%Y-%m-%dT%H:%M:%S.%f")
        except TypeError:
            pass

        if self.is_bookmark_old(value):
            singer.write_bookmark(
                Context.state,
                # name is overridden by some substreams
                self.name,
                self.replication_key,
                value
            )
            singer.write_state(Context.state)


    @staticmethod
    def get_updated_session_bookmark(session_bookmark, value):
        try:
            session_bookmark = utils.strptime_with_tz(session_bookmark)
        except TypeError:
            pass

        try:
            value = utils.strptime_with_tz(value)
        except TypeError:
            pass

        if value > session_bookmark:
            return value
        return session_bookmark


    # As we added timeout, we need to pass it in the query param
    # hence removed the condition: 'if query_params', as
    # there will be atleast 1 param: 'timeout'
    @asana_error_handling
    def call_api(self, resource, **query_params):
        api_function = getattr(Context.asana.client, resource)
        query_params['timeout'] = self.request_timeout
        return api_function.find_all(**query_params)

    def sync(self):
        """Yield's processed SDK object dicts to the caller.
        """
        for obj in self.get_objects():
            yield obj
