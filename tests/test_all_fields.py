from tap_tester import runner, connections, menagerie
from base import AsanaBase

class AsanaAllFieldsTest(AsanaBase):

    def name(self):
        return "tap_tester_asana_all_fields_test"

    def test_run(self):
        expected_streams = self.expected_streams()
        
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in found_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1] for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_automatic_keys = self.expected_primary_keys()[stream] | self.expected_replication_keys()[stream]

                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                # remove fields as data cannot be generated
                if stream == 'tasks':
                    expected_all_keys.remove('is_rendered_as_seperator')
                    expected_all_keys.remove('external')
                elif stream == 'stories':
                    expected_all_keys.remove('old_approval_status')
                    expected_all_keys.remove('old_name')
                    expected_all_keys.remove('previews')
                    expected_all_keys.remove('task')
                    expected_all_keys.remove('new_number_value')
                    expected_all_keys.remove('old_number_value')
                    expected_all_keys.remove('new_name')
                elif stream == 'sections':
                    expected_all_keys.remove('projects')
                elif stream == 'portfolios':
                    expected_all_keys.remove('is_template')

                self.assertSetEqual(expected_all_keys, actual_all_keys)
