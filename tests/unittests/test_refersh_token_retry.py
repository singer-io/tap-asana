import unittest
import tap_asana
import asana
import oauthlib
import tap_asana.streams.portfolios as portfolios
import tap_asana.streams.sections as sections
import tap_asana.streams.stories as stories
import tap_asana.streams.teams as teams
from unittest import mock
from tap_asana.context import Context
from tap_asana.asana import Asana
from oauthlib.oauth2 import TokenExpiredError


WORKSPACE_OBJECT = Context.stream_objects['workspaces']()

def no_authorized_error_raiser(*args, **kwargs):
    raise asana.error.NoAuthorizationError

def invalid_token_error_raiser(*args, **kwargs):
    raise asana.error.InvalidTokenError

def token_expired_error_raiser(*args, **kwargs):
    raise oauthlib.oauth2.rfc6749.errors.TokenExpiredError

def valid_data(*args, **kwargs):
    return "data"

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("tap_asana.streams.base.getattr")
class TestCallAPIRefreshAccessToken(unittest.TestCase):

    def test_invalid_token_error(self, mocked_get_attr, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to InvalidTokenError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = invalid_token_error_raiser # raise InvalidTokenError

        try:
            workspaces = WORKSPACE_OBJECT.call_api("workspaces")
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)
            self.assertEqual(mocked_get_attr.call_count, 2)

    def test_no_authorized_error(self, mocked_get_attr, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to NoAuthorizationError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = no_authorized_error_raiser # raise NoAuthorizationError

        try:
            workspaces = WORKSPACE_OBJECT.call_api("workspaces")
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)
            self.assertEqual(mocked_get_attr.call_count, 2)

    def test_token_expired_error(self, mocked_get_attr, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to TokenExpiredError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = token_expired_error_raiser # raise TokenExpiredError

        try:
            workspaces = WORKSPACE_OBJECT.call_api("workspaces")
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)
            self.assertEqual(mocked_get_attr.call_count, 2)

    def test_no_error(self, mocked_get_attr, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = "data"

        workspaces = WORKSPACE_OBJECT.call_api("workspaces")
        self.assertEqual(mocked_refresh_access_token.call_count, 1)
        self.assertEqual(mocked_get_attr.call_count, 1)


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
class TestGetItemForPortfolio(unittest.TestCase):

    def test_invalid_token_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to InvalidTokenError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios.get_items_for_portfolio = invalid_token_error_raiser # raise InvalidTokenError

        try:
            portfolio_items = portfolios.get_items_for_portfolio('test')
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_authorized_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to NoAuthorizationError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios.get_items_for_portfolio = no_authorized_error_raiser # raise NoAuthorizationError

        try:
            portfolio_items = portfolios.get_items_for_portfolio('test')
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_token_expired_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to TokenExpiredError error
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios.get_items_for_portfolio = token_expired_error_raiser # raise TokenExpiredError

        try:
            portfolio_items = portfolios.get_items_for_portfolio('test')
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios.get_items_for_portfolio = valid_data

        portfolio_items = portfolios.get_items_for_portfolio('test')
        self.assertEqual(mocked_refresh_access_token.call_count, 1)


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
class TestGetPortfoliosForWorkspaces(unittest.TestCase):

    def test_invalid_token_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to InvalidTokenError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios.get_portfolios = invalid_token_error_raiser # raise InvalidTokenError

        try:
            portfolio = portfolios.get_portfolies_for_workspace('test', 'test', 'test')
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_authorized_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to NoAuthorizationError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios.get_portfolios = no_authorized_error_raiser # raise NoAuthorizationError

        try:
            portfolio = portfolios.get_portfolies_for_workspace('test', 'test', 'test')
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_token_expired_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to TokenExpiredError error
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios.get_portfolios = token_expired_error_raiser # raise TokenExpiredError

        try:
            portfolio = portfolios.get_portfolies_for_workspace('test', 'test', 'test')
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios.get_portfolios = valid_data

        portfolio = portfolios.get_portfolies_for_workspace('test', 'test', 'test')
        self.assertEqual(mocked_refresh_access_token.call_count, 1)


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
class TestGetSectionsForProject(unittest.TestCase):

    def test_invalid_token_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to InvalidTokenError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.sections.get_sections_for_project = invalid_token_error_raiser # raise InvalidTokenError

        try:
            section = sections.get_sections_for_projects('test', 'test', 'test')
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_authorized_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to NoAuthorizationError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.sections.get_sections_for_project = no_authorized_error_raiser # raise NoAuthorizationError

        try:
            section = sections.get_sections_for_projects('test', 'test', 'test')
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_token_expired_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to TokenExpiredError error
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.sections.get_sections_for_project = token_expired_error_raiser # raise TokenExpiredError

        try:
            section = sections.get_sections_for_projects('test', 'test', 'test')
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.sections.get_sections_for_project = valid_data

        section = sections.get_sections_for_projects('test', 'test', 'test')
        self.assertEqual(mocked_refresh_access_token.call_count, 1)


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
class TestGetStoriesForTask(unittest.TestCase):

    def test_invalid_token_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to InvalidTokenError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.stories.get_stories_for_task = invalid_token_error_raiser # raise InvalidTokenError

        try:
            story = stories.get_stories_for_tasks('test', 'test')
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_authorized_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to NoAuthorizationError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.stories.get_stories_for_task = no_authorized_error_raiser # raise NoAuthorizationError

        try:
            story = stories.get_stories_for_tasks('test', 'test')
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_token_expired_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to TokenExpiredError error
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.stories.get_stories_for_task = token_expired_error_raiser # raise TokenExpiredError

        try:
            story = stories.get_stories_for_tasks('test', 'test')
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.stories.get_stories_for_task = valid_data

        story = stories.get_stories_for_tasks('test', 'test')
        self.assertEqual(mocked_refresh_access_token.call_count, 1)


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
class TestFindTeamByOrganization(unittest.TestCase):

    def test_invalid_token_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to InvalidTokenError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams.find_by_organization = invalid_token_error_raiser # raise InvalidTokenError

        try:
            team = teams.find_team_by_organization('test', 'test')
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_authorized_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to NoAuthorizationError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams.find_by_organization = no_authorized_error_raiser # raise NoAuthorizationError

        try:
            team = teams.find_team_by_organization('test', 'test')
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_token_expired_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to TokenExpiredError error
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams.find_by_organization = token_expired_error_raiser # raise TokenExpiredError

        try:
            team = teams.find_team_by_organization('test', 'test')
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams.find_by_organization = valid_data

        team = teams.find_team_by_organization('test', 'test')
        self.assertEqual(mocked_refresh_access_token.call_count, 1)


@mock.patch("tap_asana.asana.Asana.refresh_access_token")
class TestGetUsersForTeams(unittest.TestCase):

    def test_invalid_token_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to InvalidTokenError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams.users = invalid_token_error_raiser # raise InvalidTokenError

        try:
            team = teams.get_users_for_teams('test')
        except asana.error.InvalidTokenError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_authorized_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to NoAuthorizationError error
        '''
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams.users = no_authorized_error_raiser # raise NoAuthorizationError

        try:
            team = teams.get_users_for_teams('test')
        except asana.error.NoAuthorizationError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_token_expired_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called two time each for Asana initialization and
            refresh access_token due to TokenExpiredError error
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams.users = token_expired_error_raiser # raise TokenExpiredError

        try:
            team = teams.get_users_for_teams('test')
        except oauthlib.oauth2.TokenExpiredError as e:
            self.assertEqual(mocked_refresh_access_token.call_count, 2)

    def test_no_error(self, mocked_refresh_access_token):
        '''
            Verify that refresh_access_token is called one time for Asana initialization and
            no exception is thrown
        '''
         # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams.users = valid_data

        team = teams.get_users_for_teams('test')
        self.assertEqual(mocked_refresh_access_token.call_count, 1)
