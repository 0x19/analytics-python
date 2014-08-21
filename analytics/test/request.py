import unittest

from analytics.request import post

class TestRequests(unittest.TestCase):

    def test_valid_request(self):
        res = post('testsecret', batch=[{
            'userId': 'userId',
            'event': 'event',
            'type': 'track'
        }])
        self.assertEqual(res.status_code, 200)


    def test_invalid_write_key(self):
        with self.assertRaises(Exception) as e:
            post('invalid', batch=[{
                'userId': 'userId',
                'event': 'event',
                'type': 'track'
            }])

            self.assertEqual('invalid_write_key', e.code)

    def test_invalid_request_error(self):
        with self.assertRaises(Exception) as e:
            post('testsecret', batch='invalid')

            self.assertEqual('invalid_request_error', e.code)

