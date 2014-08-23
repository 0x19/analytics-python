import unittest

try:
    import queue
except:
    import Queue as queue


from analytics.consumer import Consumer

class TestConsumer(unittest.TestCase):

    def test_next(self):
        q = queue.Queue()
        consumer = Consumer(q, '')
        q.put(1)
        next = consumer.next()
        self.assertEqual(next, [1])

    def test_next_limit(self):
        q = queue.Queue()
        upload_size = 50
        consumer = Consumer(q, '', upload_size)
        for i in range(10000):
            q.put(i)
        next = consumer.next()
        self.assertEqual(next, range(upload_size))

    def test_upload(self):
        q = queue.Queue()
        consumer  = Consumer(q, 'testsecret')
        track = {
            'type': 'track',
            'event': 'event',
            'userId': 'userId'
        }
        q.put(track)
        success = consumer.upload()
        self.assertTrue(success)

    def test_request(self):
        consumer = Consumer(None, 'testsecret')
        track = {
            'type': 'track',
            'event': 'event',
            'userId': 'userId'
        }
        consumer.request([track])
