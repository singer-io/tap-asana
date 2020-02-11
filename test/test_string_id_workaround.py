import unittest

from tap_asana.streams.base import Stream

class TestStringIdWorkaround(unittest.TestCase):
    stream = Stream()

    def test_patch_ids_has_no_effect_in_current_state(self):
        test_obj = {'color': 'dark-purple',
                    'created_at': '2020-02-10T23:38:58.382Z',
                    'followers': [],
                    'gid': '9991311522123467',
                    'id': 9991311522123467,
                    'name': 'new_tag',
                    'notes': '',
                    'workspace': {'gid': '99916671235410',
                                  'id': 99916671235410,
                                  'resource_type': 'workspace'}}
        expected = {'color': 'dark-purple',
                    'created_at': '2020-02-10T23:38:58.382Z',
                    'followers': [],
                    'gid': '9991311522123467',
                    'id': 9991311522123467,
                    'name': 'new_tag',
                    'notes': '',
                    'workspace': {'gid': '99916671235410',
                                  'id': 99916671235410,
                                  'resource_type': 'workspace'}}
        actual = self.stream._patch_ids(test_obj)
        self.assertEqual(expected, actual)
        
        
    def test_patch_ids_works_with_int_gid_no_id(self):
        test_obj = {'color': 'dark-purple',
                    'created_at': '2020-02-10T23:38:58.382Z',
                    'followers': [],
                    'gid': '9991311522123467',
                    'name': 'new_tag',
                    'notes': '',
                    'workspace': {'gid': '99916671235410',
                                  'resource_type': 'workspace'}}
        expected = {'color': 'dark-purple',
                    'created_at': '2020-02-10T23:38:58.382Z',
                    'followers': [],
                    'gid': '9991311522123467',
                    'id': 9991311522123467,
                    'name': 'new_tag',
                    'notes': '',
                    'workspace': {'gid': '99916671235410',
                                  'id': 99916671235410,
                                  'resource_type': 'workspace'}}
        actual = self.stream._patch_ids(test_obj)
        self.assertEqual(expected, actual)

    def test_patch_ids_works_with_string_gid_no_id(self):
        test_obj = {'color': 'dark-purple',
                    'created_at': '2020-02-10T23:38:58.382Z',
                    'followers': [],
                    'gid': 'abc123hereisastring',
                    'name': 'new_tag',
                    'notes': '',
                    'workspace': {'gid': 'aworkspaceidstring',
                                  'resource_type': 'workspace'}}
        expected = {'color': 'dark-purple',
                    'created_at': '2020-02-10T23:38:58.382Z',
                    'followers': [],
                    'gid': 'abc123hereisastring',
                    'id': 'abc123hereisastring',
                    'name': 'new_tag',
                    'notes': '',
                    'workspace': {'gid': 'aworkspaceidstring',
                                  'id': 'aworkspaceidstring',
                                  'resource_type': 'workspace'}}
        actual = self.stream._patch_ids(test_obj)
        self.assertEqual(expected, actual)
