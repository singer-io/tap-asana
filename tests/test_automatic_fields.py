import json
from tap_tester import runner, connections
from base import AsanaBase


class AsanaAutomaticFieldsTest(AsanaBase):
    """
    Verify that running the tap with all streams selected and all fields deselected results in the
    replication of just the primary keys and replication keys (automatic fields)
    """

    def name(self):
        return "tap_tester_asana_automatic_fields_test"

    def test_run(self):
        """
        Testing that all the automatic fields are replicated despite de-selecting them
        - Verify that only the automatic fields are sent to the target.
        - Verify that all replicated records have unique primary key values.
        """
        conn_id = connections.ensure_connection(self)
        expected_streams = self.expected_streams()

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # De-select all the fields
        self.select_found_catalogs(conn_id, found_catalogs,
                                   deselect_all_fields=True, only_streams=expected_streams)

        # Run sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_automatic_keys = expected_primary_keys | self.expected_replication_keys()[stream]

                # Collect actual values
                messages = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys())
                                        for row in messages['messages']]

                # Check if the stream has collected some records
                self.assertGreater(record_count_by_stream.get(stream, 0), 0)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_automatic_keys, actual_keys)

                # Get records
                records = [message.get("data") for message in messages.get('messages', [])
                           if message.get('action') == 'upsert']

                # Remove duplicate records
                records_primary_list = [tuple(message.get(pk) for pk in expected_primary_keys)
                                   for message in [json.loads(t) for t in {json.dumps(d) for d in records}]]

                # Remove duplicate primary keys
                records_primary_set = set(records_primary_list)

                # Verify defined primary key is unique
                # Note: In tap-asana streams can have duplicate records with exact same primary key and other field values
                # Below assertion will check if we have records with same primary key but different field values
                self.assertCountEqual(records_primary_set, records_primary_list,
                                      msg=f"{expected_primary_keys} is not a unique primary key for {stream} stream.")
