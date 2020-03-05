import math
import functools
import datetime
import sys
import backoff
import simplejson
import singer
import time
from singer.messages import StateMessage
from tap_asana.asana import Asana
from asana.error import AsanaError, NoAuthorizationError, RetryableAsanaError, InvalidTokenError, RateLimitEnforcedError
from singer import utils
from tap_asana.context import Context


LOGGER = singer.get_logger()

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
    LOGGER.info("Received InvalidTokenError, refreshing access token")
    Context.asana.refresh_access_token()


def asana_error_handling(fnc):
    @backoff.on_exception(backoff.expo,
                          InvalidTokenError,
                          on_backoff=invalid_token_handler)
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


class Stream():
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_method = None
    replication_key = None
    key_properties = ['gid']
    # Controls which SDK object we use to call the API by default.
    # 

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


    def get_updated_session_bookmark(self, session_bookmark, value):
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


    @asana_error_handling
    def call_api(self, resource, **query_params):
        fn = getattr(Context.asana.client, resource)
        if query_params:
            return fn.find_all(**query_params)
        return fn.find_all()

    def sync(self):
        """Yield's processed SDK object dicts to the caller.
        """
        for obj in self.get_objects():
            yield obj
