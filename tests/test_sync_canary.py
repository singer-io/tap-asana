from tap_tester import connections
from base import AsanaBase

class AsanaSyncTest(AsanaBase):

    def name(self):
        return "tap_tester_asana_sync_test"

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        expected_streams = self.expected_streams()
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        # check if all streams have collected records
        for stream in expected_streams:
            self.assertGreater(record_count_by_stream.get(stream, 0), 0)
