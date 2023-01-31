from tap_tester import runner, connections, menagerie
from base import AsanaBase


class AsanaAllFieldsTest(AsanaBase):
    """
    Test to verify that running the tap run with all streams and fields selected results
    in the replication of all fields.
    """

    # Removed below fields as data cannot be generated
    fields_to_remove = {
        'tasks': {
            'external',
            'is_rendered_as_seperator'
        },
        'stories': {
            'old_approval_status',
            'old_name',
            'previews',
            'task',
            'new_number_value',
            'old_number_value',
            'new_name'
        },
        'sections': {
            'projects'
        },
        'portfolios': {
            'is_template'
        },
        'projects': {
            'is_template',
            'project_brief'
        }
    }
    def name(self):
        """ Returns the test name """
        return "tap_tester_asana_all_fields_test"

    def test_run(self):
        """
        Testing that all fields mentioned in the catalog are synced from the tap
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream
        """
        expected_streams = self.expected_streams()

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        self.select_found_catalogs(
            conn_id, found_catalogs, only_streams=expected_streams)

        # Grab metadata after performing table-and-field selection to set expectations
        # Used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in found_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1] for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # Run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_automatic_keys = self.expected_primary_keys()[stream] | \
                    self.expected_replication_keys()[stream]

                # Get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # Collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # Collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # Verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys),
                                msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                # Removed below fields as data cannot be generated
                for field in self.fields_to_remove.get(stream, set()):
                    expected_all_keys.remove(field)

                self.assertSetEqual(expected_all_keys, actual_all_keys)
