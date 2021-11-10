import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os
import unittest
from datetime import datetime as dt
import time

class AsanaBase(unittest.TestCase):
    START_DATE = ""
    DATETIME_FMT = {
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ"
    }
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"

    first_start_date = '2019-01-01T00:00:00Z'
    second_start_date = '2020-08-15T00:00:00Z'

    def name(self):
        return "tap_tester_asana_base"

    def tap_name(self):
        return "tap-asana"

    def setUp(self):
        required_env = {
            "TAP_ASANA_CLIENT_ID",
            "TAP_ASANA_CLIENT_SECRET",
            "TAP_ASANA_REDIRECT_URI",
            "TAP_ASANA_REFRESH_TOKEN"
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def get_type(self):
        return "platform.asana"

    def get_credentials(self):
        return {
            "client_secret": os.getenv("TAP_ASANA_CLIENT_SECRET"),
            "refresh_token": os.getenv("TAP_ASANA_REFRESH_TOKEN")
        }

    def get_properties(self, original: bool = True):
        return_value = {
            "start_date": "2018-04-11T00:00:00Z",
            "client_id": os.getenv("TAP_ASANA_CLIENT_ID"),
            "redirect_uri": os.getenv("TAP_ASANA_REDIRECT_URI")
        }
        if original:
            return return_value

        # Reassign start date
        return_value["start_date"] = self.START_DATE
        return return_value

    def expected_metadata(self):
        return {
            "portfolios": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "projects": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"modified_at"}
            },
            "sections": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "stories": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"created_at"}
            },
            "tags": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"created_at"}
            },
            "tasks": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"modified_at"}
            },
            "teams": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "users": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "workspaces": {
                self.PRIMARY_KEYS: {"gid"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            }
        }

    def expected_streams(self):
        return set(self.expected_metadata().keys())

    def expected_replication_keys(self):
        return {table: properties.get(self.REPLICATION_KEYS, set()) for table, properties
                in self.expected_metadata().items()}

    def expected_primary_keys(self):
        return {table: properties.get(self.PRIMARY_KEYS, set()) for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        return {table: properties.get(self.REPLICATION_METHOD, set()) for table, properties
                in self.expected_metadata().items()}

    def select_found_catalogs(self, conn_id, catalogs, only_streams=None, deselect_all_fields: bool = False, non_selected_props=[]):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if only_streams and catalog["stream_name"] not in only_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = non_selected_props if not deselect_all_fields else []
            if deselect_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {})
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               catalog,
                                                               schema,
                                                               additional_md=additional_md,
                                                               non_selected_fields=non_selected_properties)

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        print(found_catalog_names)
        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              self.expected_streams(),
                                                              self.expected_primary_keys())

        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def dt_to_ts(self, dtime):
        for date_format in self.DATETIME_FMT:
            try:
                date_stripped = int(time.mktime(dt.strptime(dtime, date_format).timetuple()))
                return date_stripped
            except ValueError:
                continue

    def is_incremental(self, stream):
        return self.expected_metadata()[stream][self.REPLICATION_METHOD] == self.INCREMENTAL
