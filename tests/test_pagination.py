import json
from tap_tester import runner, connections
from base import AsanaBase


class AsanaPaginationTest(AsanaBase):
    """
    Verify that tap can replicate multiple pages of data for streams that use pagination.
    """

    def name(self):
        return "tap_tester_asana_pagination_test"

    def test_run(self):
        """
        Testing that the pagination works when there are records greater than the page size
        - Verify for each stream you can get multiple pages of data
        - Verify by pks that the data replicated matches the data we expect.
        """

        # The default page size is 50:
        # https://github.com/Asana/python-asana/blob/master/asana/client.py#L41
        # We have more than 50 records for all the stream except 'workspace'
        # To get data from more than 1 'workspaces' we need to upgrade asana plan
        expected_streams = self.expected_streams() - {'workspaces'} | {'stories'}
        conn_id = connections.ensure_connection(self)

        # Select all streams and all fields within streams
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.select_found_catalogs(conn_id, found_catalogs,
                                   only_streams=expected_streams)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(
            conn_id, expected_streams)
        actual_fields_by_stream = runner.examine_target_output_for_fields()
        sync_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Verify that we can paginate with all fields selected
                minimum_record_count = 50

                self.assertGreater(record_count_by_stream.get(stream, -1), minimum_record_count,
                                   msg="The number of records is not over the stream max limit")

                expected_pk = self.expected_primary_keys().get(stream, set())
                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                # Verify that the automatic fields are sent to the target
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).issuperset(
                        expected_pk |
                        self.expected_replication_keys().get(stream, set())),
                    msg="The fields sent to the target don't include all automatic fields"
                )

                # Verify we have more fields sent to the target than just automatic fields
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).symmetric_difference(
                        expected_pk |
                        self.expected_replication_keys().get(stream, set())),
                    msg="The fields sent to the target don't include non-automatic fields"
                )

                # Get records
                records = [message.get("data") for message in sync_messages.get('messages', [])
                           if message.get('action') == 'upsert']
                # Remove duplicate records
                records_pks_list = [tuple(message.get(pk) for pk in expected_pk)
                                   for message in [json.loads(t) for t in {json.dumps(d) for d in records}]]
                records_pks_set = set(records_pks_list)

                # Verify we did not duplicate any records across pages
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg=f"We have duplicate records for {stream}")
