from base import AsanaBase
from tap_tester import connections, menagerie, runner


class AsanaBookmarksTest(AsanaBase):
    """
    Test to verify that the bookmark works as expected.
    """

    def name(self):
        """ Returns the test name """
        return "tap_tester_asana_bookmarks_test"

    def test_run(self):
        """
        Testing that the bookmarking for the tap works as expected
        - Verify for each incremental stream you can do a sync which records bookmarks
        - Verify that a bookmark doesn't exist for full table streams.
        - Verify the bookmark is the max value sent to the target for the a given replication key.
        - Verify 2nd sync respects the bookmark
        - All data of the 2nd sync is >= the bookmark from the first sync
        - The number of records in the 2nd sync is less then the first
        """
        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        expected_streams = self.expected_streams()

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs,
                                   only_streams=expected_streams)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State
        ##########################################################################

        # Setting 'second_start_date' as bookmark for running 2nd sync
        new_state = {'bookmarks': dict()}
        replication_keys = self.expected_replication_keys()
        # Add state for bookmark stored for streams in 1st sync
        for stream in first_sync_bookmarks.get('bookmarks').keys():
            if self.is_incremental(stream):
                new_state['bookmarks'][stream] = dict()
                # This date is being re-used from the start date test because it is more recent
                # than the default start date used by the connection and is acting as a simulated
                # bookmark
                new_state['bookmarks'][stream][list(replication_keys[stream])[0]] = self.second_start_date

        # Set state for next sync
        menagerie.set_state(conn_id, new_state)

        ##########################################################################
        # Second Sync
        ##########################################################################

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        for stream in expected_streams:

            with self.subTest(stream=stream):
                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [
                    record.get('data') for record in first_sync_records.get(stream).get('messages')
                    if record.get('action') == 'upsert']
                second_sync_messages = [
                    record.get('data') for record in second_sync_records.get(stream).get('messages')
                    if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get(
                    'bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get(
                    'bookmarks', {stream: None}).get(stream)

                if self.is_incremental(stream):

                    # Collect information specific to incremental streams from syncs 1 & 2
                    replication_key = list(self.expected_replication_keys()[stream])[0]
                    first_bookmark_value = first_bookmark_key_value.get(replication_key)
                    second_bookmark_value = second_bookmark_key_value.get(replication_key)
                    first_bookmark_value_parsed = self.dt_to_ts(first_bookmark_value, self.BOOKMARK_FOMAT)
                    second_bookmark_value_parsed = self.dt_to_ts(second_bookmark_value, self.BOOKMARK_FOMAT)

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_key_value.get(replication_key))

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_key_value.get(replication_key))

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value)  # assumes no changes to data during test

                    # Verify that you get some records for each stream
                    self.assertGreater(second_sync_count, 0)

                    # second sync is less than the first
                    self.assertLess(second_sync_count, first_sync_count)

                    # Verify that the 2nd sync respects the bookmark. The number of records in the
                    for record in second_sync_messages:

                        # Verify the second sync bookmark value is the max replication key value for
                        # a given stream
                        replication_key_value = record.get(replication_key)
                        replication_key_value_parsed = self.dt_to_ts(replication_key_value, self.REPLICATION_DATE_FOMAT)
                        self.assertLessEqual(
                            replication_key_value_parsed, second_bookmark_value_parsed,
                            msg="Second sync bookmark was set incorrectly, \
                                a record with a greater replication-key value was synced."
                        )

                        # Verify the data of the second sync is greater-equal to the bookmark from
                        # the first sync.
                        # We have added 'second_start_date' as the bookmark, it is more recent than
                        # the default start date and it will work as a simulated bookmark
                        self.assertGreaterEqual(
                            replication_key_value_parsed, self.dt_to_ts(self.second_start_date, self.START_DATE_FORMAT),
                            msg="Second sync did not respect the bookmark, \
                                a record with a smaller replication-key value was synced."
                        )

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for
                        # a given stream
                        replication_key_value = record.get(replication_key)
                        replication_key_value_parsed = self.dt_to_ts(replication_key_value, self.REPLICATION_DATE_FOMAT)
                        self.assertLessEqual(
                            replication_key_value_parsed, first_bookmark_value_parsed,
                            msg="First sync bookmark was set incorrectly, \
                                a record with a greater replication-key value was synced."
                        )

                else:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)
