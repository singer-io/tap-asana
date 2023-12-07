from tap_tester import connections, runner
from base import AsanaBase


class AsanaStartDateTest(AsanaBase):
    """
        Verify that that the tap respects the start date.
    """

    def name(self):
        return "tap_tester_asana_start_date_test"

    def test_run(self):
        # running sync with multiple date timestamps accross different streams due to differences in bookmark values
        self.run_test("2021-11-09T00:00:00Z", "2023-11-10T00:00:00Z", {"projects",})
        self.run_test("2023-11-28T00:00:00Z", "2023-11-30T00:00:00Z", {"subtasks",})
        self.run_test("2019-01-28T00:00:00Z", "2023-11-30T00:00:00Z", self.expected_streams() - {"subtasks","projects"})
        
    def run_test(self, start_date_1, start_date_2, streams):
        """
        Testing that the tap respects the start date
        - INCREMENTAL
            - Verify 1st sync (start date=today-N days) record count > 2nd sync
                (start date=today) record count.
            - Verify minimum bookmark sent to the target for incremental streams >= start date
                for both syncs.
            - Verify by primary key values, that all records in the 2nd sync are included in
                the 1st sync since 2nd sync has a later start date.
        - FULL TABLE
            - Verify that the 2nd sync includes the same number of records as the 1st sync.
            - Verify by primary key values, that the 2nd sync and 1st sync replicated the same
                records.
        """

        self.first_start_date = start_date_1
        self.second_start_date = start_date_2
        self.streams = streams
        expected_streams = streams


        start_date_1_epoch = self.dt_to_ts(self.first_start_date, self.START_DATE_FORMAT)
        start_date_2_epoch = self.dt_to_ts(self.second_start_date, self.START_DATE_FORMAT)

        ##########################################################################
        # Update Start Date for 1st sync
        ##########################################################################

        self.START_DATE = self.first_start_date

        ##########################################################################
        # First Sync
        ##########################################################################

        conn_id_1 = connections.ensure_connection(self, original_properties=False)
        runner.run_check_mode(self, conn_id_1)

        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)
        self.select_found_catalogs(conn_id_1, found_catalogs_1,
                                   only_streams=expected_streams)

        sync_record_count_1 = self.run_and_verify_sync(conn_id_1)

        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        # Update Start Date for 2nd sync
        ##########################################################################

        self.START_DATE = self.second_start_date

        ##########################################################################
        # Second Sync
        ##########################################################################

        conn_id_2 = connections.ensure_connection(self, original_properties=False)
        runner.run_check_mode(self, conn_id_2)

        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)
        self.select_found_catalogs(conn_id_2, found_catalogs_2,
                                   only_streams=expected_streams)

        sync_record_count_2 = self.run_and_verify_sync(conn_id_2)

        synced_records_2 = runner.get_records_from_target_output()

        self.assertGreaterEqual(sum(sync_record_count_1.values()), sum(sync_record_count_2.values()))

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_metadata = self.expected_metadata()[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = sync_record_count_1.get(stream, 0)
                record_count_sync_2 = sync_record_count_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk)
                                       for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream,{}).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk)
                                       for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream,{}).get('messages')
                                       if message.get('action') == 'upsert']

                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                if expected_metadata[self.OBEYS_START_DATE]:
                    # Expected bookmark key is one element in set so directly access it
                    start_date_keys_list_1 = [message.get('data').get(list(expected_replication_keys)[0])
                        for message in synced_records_1.get(stream).get('messages')
                        if message.get('action') == 'upsert']
                    start_date_keys_list_2 = [message.get('data').get(list(expected_replication_keys)[0])
                        for message in synced_records_2.get(stream).get('messages')
                        if message.get('action') == 'upsert']

                    start_date_key_sync_1 = set(start_date_keys_list_1)
                    start_date_key_sync_2 = set(start_date_keys_list_2)

                    # Verify bookmark key values are greater than or equal to start date of sync 1
                    for start_date_key_value in start_date_key_sync_1:
                        self.assertGreaterEqual(self.dt_to_ts(start_date_key_value, self.REPLICATION_DATE_FOMAT), start_date_1_epoch)

                    # Verify bookmark key values are greater than or equal to start date of sync 2
                    for start_date_key_value in start_date_key_sync_2:
                        self.assertGreaterEqual(self.dt_to_ts(start_date_key_value, self.REPLICATION_DATE_FOMAT), start_date_2_epoch)

                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2 for stream
                    self.assertGreater(record_count_sync_1, record_count_sync_2)

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

                else:
                    # Verify that the 2nd sync with a later start date replicates the same number of
                    # records as the 1st sync.
                    self.assertEqual(record_count_sync_2, record_count_sync_1)

                    # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
