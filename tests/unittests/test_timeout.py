import unittest

import requests
import asana
import oauthlib
import tap_asana.streams.portfolios as portfolios
import tap_asana.streams.sections as sections
import tap_asana.streams.stories as stories
import tap_asana.streams.teams as teams
from unittest import mock
from tap_asana.context import Context
from tap_asana.asana import Asana

WORKSPACE_OBJECT = Context.stream_objects['workspaces']()

# raise requests.Timeout error
def raise_Timeout_error(*args, **kwargs):
    raise requests.Timeout

# yield dummy data
def valid_data(*args, **kwargs):
    yield {"key1": "value1", "key2": "value2"}

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("tap_asana.streams.base.getattr")
@mock.patch("time.sleep")
class TestTimeoutErrorBase(unittest.TestCase):

    def test_timeout_error__Base(self, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = raise_Timeout_error

        try:
            workspaces = WORKSPACE_OBJECT.call_api("workspaces")
        except requests.Timeout:
            pass

        self.assertEqual(mocked_get_attr.call_count, 5)

    def test_no_timeout_error__Base(self, mocked_sleep, mocked_get_attr, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        mocked_find_all = mocked_get_attr.return_value
        mocked_find_all.find_all.side_effect = valid_data

        workspaces = WORKSPACE_OBJECT.call_api("workspaces")

        self.assertEqual(workspaces, list(valid_data()))

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("time.sleep")
class TestTimeoutErrorPortfolios(unittest.TestCase):

    def test_timeout_error__get_items_for_portfolio(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios = mock.MagicMock()
        Context.asana.client.portfolios.get_items_for_portfolio.side_effect = raise_Timeout_error

        try:
            items_for_portfolios = portfolios.get_items_for_portfolio('test')
        except requests.Timeout:
            pass

        self.assertEqual(Context.asana.client.portfolios.get_items_for_portfolio.call_count, 5)

    def test_no_timeout_error__get_items_for_portfolio(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios = mock.MagicMock()
        Context.asana.client.portfolios.get_items_for_portfolio.side_effect = valid_data

        items_for_portfolios = portfolios.get_items_for_portfolio('test')

        self.assertEqual(items_for_portfolios, list(valid_data()))

    def test_timeout_error__get_portfolies_for_workspace(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios = mock.MagicMock()
        Context.asana.client.portfolios.get_portfolios.side_effect = raise_Timeout_error

        try:
            portfolios_for_workspace = portfolios.get_portfolies_for_workspace('test', 'test', ['test'])
        except requests.Timeout:
            pass

        self.assertEqual(Context.asana.client.portfolios.get_portfolios.call_count, 5)

    def test_no_timeout_error__get_portfolies_for_workspace(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.portfolios = mock.MagicMock()
        Context.asana.client.portfolios.get_portfolios.side_effect = valid_data

        portfolios_for_workspace = portfolios.get_portfolies_for_workspace('test', 'test', ['test'])

        self.assertEqual(portfolios_for_workspace, list(valid_data()))

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("time.sleep")
class TestTimeoutErrorSections(unittest.TestCase):

    def test_timeout_error__get_sections_for_projects(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.sections = mock.MagicMock()
        Context.asana.client.sections.get_sections_for_project.side_effect = raise_Timeout_error

        try:
            sections_for_projects = sections.get_sections_for_projects('test', 'test', ['test'])
        except requests.Timeout:
            pass

        self.assertEqual(Context.asana.client.sections.get_sections_for_project.call_count, 5)

    def test_no_timeout_error__get_sections_for_projects(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.sections = mock.MagicMock()
        Context.asana.client.sections.get_sections_for_project.side_effect = valid_data

        sections_for_projects = sections.get_sections_for_projects('test', 'test', ['test'])

        self.assertEqual(sections_for_projects, list(valid_data()))

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("time.sleep")
class TestTimeoutErrorStories(unittest.TestCase):

    def test_timeout_error__get_stories_for_tasks(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.stories = mock.MagicMock()
        Context.asana.client.stories.get_stories_for_task.side_effect = raise_Timeout_error

        try:
            stories_for_tasks = stories.get_stories_for_tasks('test', ['test'])
        except requests.Timeout:
            pass

        self.assertEqual(Context.asana.client.stories.get_stories_for_task.call_count, 5)

    def test_no_timeout_error__get_stories_for_tasks(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.stories = mock.MagicMock()
        Context.asana.client.stories.get_stories_for_task.side_effect = valid_data

        stories_for_tasks = stories.get_stories_for_tasks('test', ['test'])

        self.assertEqual(stories_for_tasks, list(valid_data()))

@mock.patch("tap_asana.asana.Asana.refresh_access_token")
@mock.patch("time.sleep")
class TestTimeoutErrorTeams(unittest.TestCase):

    def test_timeout_error__find_team_by_organization(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams = mock.MagicMock()
        Context.asana.client.teams.find_by_organization.side_effect = raise_Timeout_error

        try:
            team_by_organization = teams.find_team_by_organization('test', ['test'])
        except requests.Timeout:
            pass

        self.assertEqual(Context.asana.client.teams.find_by_organization.call_count, 5)

    def test_no_timeout_error__find_team_by_organization(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams = mock.MagicMock()
        Context.asana.client.teams.find_by_organization.side_effect = valid_data

        team_by_organization = teams.find_team_by_organization('test', ['test'])

        self.assertEqual(team_by_organization, list(valid_data()))

    def test_timeout_error__get_users_for_teams(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams = mock.MagicMock()
        Context.asana.client.teams.users.side_effect = raise_Timeout_error

        try:
            users_for_team = teams.get_users_for_teams('test')
        except requests.Timeout:
            pass

        self.assertEqual(Context.asana.client.teams.users.call_count, 5)

    def test_no_timeout_error__get_users_for_teams(self, mocked_sleep, mocked_refresh_access_token):
        # Set asana client in Context before test
        Context.asana = Asana('test', 'test', 'test', 'test', 'test')
        Context.asana.client.teams = mock.MagicMock()
        Context.asana.client.teams.users.side_effect = valid_data

        users_for_team = teams.get_users_for_teams('test')

        self.assertEqual(users_for_team, list(valid_data()))
