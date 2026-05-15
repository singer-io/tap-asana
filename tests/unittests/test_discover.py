"""Unit tests for the refactored discover-mode helpers in tap_asana/__init__.py."""

import unittest
from unittest import mock

import asana

from tap_asana.asana import Asana
from tap_asana.context import Context
from tap_asana.streams.portfolios import Portfolios
import tap_asana


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_api_exception(status):
    """Return an asana.rest.ApiException with the given HTTP status."""
    exc = asana.rest.ApiException(status=status, reason="test")
    exc.status = status
    return exc


# ---------------------------------------------------------------------------
# _probe_workspaces
# ---------------------------------------------------------------------------

class TestProbeWorkspaces(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, _):
        Context.config = {"start_date": "2020-01-01T00:00:00Z"}
        Context.asana = Asana("test", "test", "test", "test", "test")

    def tearDown(self):
        if hasattr(Context, "asana") and hasattr(Context.asana, "client") and Context.asana.client:
            try:
                Context.asana.client.pool.close()
                Context.asana.client.pool.join()
            except Exception:
                pass

    @mock.patch("tap_asana.streams.base.Stream.fetch_workspaces")
    def test_returns_workspace_list(self, mocked_fetch):
        """_probe_workspaces() should return the list returned by fetch_workspaces."""
        mocked_fetch.return_value = [{"gid": "ws1"}]
        result = tap_asana._probe_workspaces()
        self.assertEqual(result, [{"gid": "ws1"}])

    @mock.patch("tap_asana.streams.base.Stream.fetch_workspaces")
    def test_raises_runtime_error_on_402(self, mocked_fetch):
        """_probe_workspaces() should raise RuntimeError containing the HTTP status when 402 is returned."""
        mocked_fetch.side_effect = _make_api_exception(402)
        with self.assertRaises(RuntimeError) as ctx:
            tap_asana._probe_workspaces()
        self.assertIn("HTTP-error-code: 402", str(ctx.exception))

    @mock.patch("tap_asana.streams.base.Stream.fetch_workspaces")
    def test_raises_runtime_error_on_403(self, mocked_fetch):
        """_probe_workspaces() should raise RuntimeError containing the HTTP status when 403 is returned."""
        mocked_fetch.side_effect = _make_api_exception(403)
        with self.assertRaises(RuntimeError) as ctx:
            tap_asana._probe_workspaces()
        self.assertIn("HTTP-error-code: 403", str(ctx.exception))

    @mock.patch("tap_asana.streams.base.Stream.fetch_workspaces")
    def test_re_raises_other_api_errors(self, mocked_fetch):
        """_probe_workspaces() should re-raise ApiException for status codes other than 402/403 (e.g. 500)."""
        mocked_fetch.side_effect = _make_api_exception(500)
        with self.assertRaises(asana.rest.ApiException):
            tap_asana._probe_workspaces()


# ---------------------------------------------------------------------------
# _handle_excluded_streams
# ---------------------------------------------------------------------------

class TestHandleExcludedStreams(unittest.TestCase):

    def test_raises_when_no_accessible_streams_remain(self):
        """_handle_excluded_streams() should raise RuntimeError when the streams list is empty (all excluded)."""
        inaccessible_streams = [("portfolios", None)]
        with self.assertRaises(RuntimeError) as ctx:
            tap_asana._handle_excluded_streams(inaccessible_streams, streams=[])
        self.assertIn("lack of permissions", str(ctx.exception))

    def test_warns_when_some_streams_still_accessible(self):
        """_handle_excluded_streams() should log a warning naming the excluded streams when others remain accessible."""
        inaccessible_streams = [("portfolios", None)]
        remaining = [{"stream": "tasks"}]
        with mock.patch.object(tap_asana.LOGGER, "warning") as mocked_warn:
            tap_asana._handle_excluded_streams(inaccessible_streams, remaining)
        mocked_warn.assert_called_once()
        self.assertIn("portfolios", mocked_warn.call_args[0][1])


# ---------------------------------------------------------------------------
# Portfolios.check_access
# ---------------------------------------------------------------------------

class TestPortfoliosCheckAccess(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, _):
        Context.config = {"start_date": "2020-01-01T00:00:00Z"}
        Context.asana = Asana("test", "test", "test", "test", "test")

    def tearDown(self):
        if hasattr(Context, "asana") and hasattr(Context.asana, "client") and Context.asana.client:
            try:
                Context.asana.client.pool.close()
                Context.asana.client.pool.join()
            except Exception:
                pass

    def test_returns_true_when_workspaces_empty(self):
        """check_access() should return True without calling the API when the workspace list is empty."""
        stream = Portfolios()
        self.assertTrue(stream.check_access([]))

    @mock.patch("asana.PortfoliosApi.get_portfolios")
    def test_returns_true_when_api_succeeds(self, mocked_get):
        """check_access() should return True when the portfolios API probe succeeds."""
        mocked_get.return_value = iter([])
        stream = Portfolios()
        result = stream.check_access([{"gid": "ws1"}])
        self.assertTrue(result)

    @mock.patch("asana.PortfoliosApi.get_portfolios")
    def test_returns_false_on_402(self, mocked_get):
        """check_access() should return False when the API returns HTTP 402 (payment required / non-premium workspace)."""
        mocked_get.side_effect = _make_api_exception(402)
        stream = Portfolios()
        result = stream.check_access([{"gid": "ws1"}])
        self.assertFalse(result)

    @mock.patch("asana.PortfoliosApi.get_portfolios")
    def test_returns_false_on_403(self, mocked_get):
        """check_access() should return False when the API returns HTTP 403 (forbidden)."""
        mocked_get.side_effect = _make_api_exception(403)
        stream = Portfolios()
        result = stream.check_access([{"gid": "ws1"}])
        self.assertFalse(result)

    @mock.patch("asana.PortfoliosApi.get_portfolios")
    def test_re_raises_unexpected_api_error(self, mocked_get):
        """check_access() should re-raise ApiException for status codes other than 402/403."""
        mocked_get.side_effect = _make_api_exception(500)
        stream = Portfolios()
        with self.assertRaises(asana.rest.ApiException):
            stream.check_access([{"gid": "ws1"}])

    @mock.patch("asana.PortfoliosApi.get_portfolios")
    def test_probes_with_first_workspace_only(self, mocked_get):
        """check_access() should only probe the first workspace in the list, not iterate all of them."""
        mocked_get.return_value = iter([])
        stream = Portfolios()
        stream.check_access([{"gid": "ws1"}, {"gid": "ws2"}])
        call_kwargs = mocked_get.call_args[1]
        self.assertEqual(call_kwargs.get("workspace"), "ws1")


# ---------------------------------------------------------------------------
# discover() — stream exclusion behaviour
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    @mock.patch("tap_asana.asana.Asana.refresh_access_token")
    def setUp(self, _):
        Context.config = {"start_date": "2020-01-01T00:00:00Z"}
        Context.state = {}
        Context.asana = Asana("test", "test", "test", "test", "test")

    def tearDown(self):
        if hasattr(Context, "asana") and hasattr(Context.asana, "client") and Context.asana.client:
            try:
                Context.asana.client.pool.close()
                Context.asana.client.pool.join()
            except Exception:
                pass

    @mock.patch("tap_asana._probe_workspaces", return_value=[{"gid": "ws1"}])
    @mock.patch("tap_asana.load_schemas", return_value={
        "tasks": {"properties": {"gid": {"type": "string"}}},
    })
    def test_accessible_stream_included_in_catalog(self, _schemas, _ws):
        """discover() should include streams whose check_access() returns True (default) in the catalog."""
        catalog = tap_asana.discover()
        stream_ids = [s["tap_stream_id"] for s in catalog["streams"]]
        self.assertIn("tasks", stream_ids)

    @mock.patch("tap_asana._probe_workspaces", return_value=[{"gid": "ws1"}])
    @mock.patch("tap_asana.load_schemas", return_value={
        "portfolios": {"properties": {"gid": {"type": "string"}}},
        "tasks": {"properties": {"gid": {"type": "string"}}},
    })
    @mock.patch("tap_asana.streams.portfolios.Portfolios.check_access", return_value=False)
    def test_inaccessible_stream_excluded_from_catalog(self, _chk, _schemas, _ws):
        """discover() should omit streams whose check_access() returns False and keep remaining ones."""
        catalog = tap_asana.discover()
        stream_ids = [s["tap_stream_id"] for s in catalog["streams"]]
        self.assertNotIn("portfolios", stream_ids)
        self.assertIn("tasks", stream_ids)

    @mock.patch("tap_asana._probe_workspaces", return_value=[{"gid": "ws1"}])
    @mock.patch("tap_asana.load_schemas", return_value={
        "portfolios": {"properties": {"gid": {"type": "string"}}},
    })
    @mock.patch("tap_asana.streams.portfolios.Portfolios.check_access", return_value=False)
    def test_raises_when_all_streams_inaccessible(self, _chk, _schemas, _ws):
        """discover() should raise RuntimeError when every stream is excluded by check_access()."""
        with self.assertRaises(RuntimeError) as ctx:
            tap_asana.discover()
        self.assertIn("lack of permissions", str(ctx.exception))

    @mock.patch("tap_asana._probe_workspaces", return_value=[{"gid": "ws1"}])
    @mock.patch("tap_asana.load_schemas", return_value={
        "tasks": {"properties": {"gid": {"type": "string"}}},
    })
    def test_catalog_entry_has_required_keys(self, _schemas, _ws):
        """Each catalog entry produced by discover() must contain stream, tap_stream_id, schema, metadata, and key_properties."""
        catalog = tap_asana.discover()
        entry = catalog["streams"][0]
        for key in ("stream", "tap_stream_id", "schema", "metadata", "key_properties"):
            self.assertIn(key, entry)

    @mock.patch("tap_asana._probe_workspaces", return_value=[{"gid": "ws1"}])
    @mock.patch("tap_asana.load_schemas", return_value={
        "tasks": {"properties": {"gid": {"type": "string"}}},
    })
    def test_schema_name_not_in_stream_objects_is_skipped(self, _schemas, _ws):
        """Schemas without a matching stream object are silently skipped."""
        with mock.patch.dict(Context.stream_objects, {}, clear=True):
            catalog = tap_asana.discover()
        self.assertEqual(catalog["streams"], [])
