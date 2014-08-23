from threading import Thread
import logging

from analytics.version import VERSION
from analytics.request import post

log = logging.getLogger('analytics')


class Consumer(Thread):
    """ Consumes the messages from the client's queue. """

    def __init__(self, queue, write_key, upload_size=50):
        super(Thread, self).__init__()
        self.upload_size = upload_size
        self.write_key = write_key
        self.queue = queue
        self.retries = 3

    def run(self):
        """ Runs the consumer. """
        log('debug', 'consumer is running...')
        while True:
            self.upload()

    def upload(self):
        """ Upload the next batch of items, return whether successful. """
        success = False
        batch = self.next()
        try:
            self.request(batch)
            success = True
        except:
            log('error')

        for item in batch:
            self.queue.task_done()

        return success

    def next(self):
        """ Block and return the next batch of items to upload. """
        queue = self.queue
        item = queue.get(block=True)
        items = [item]

        while len(items) < self.upload_size and not queue.empty():
            items.append(queue.get(block=True))

        return items

    def request(self, batch, attempt=0):
        """ Attempt to upload the batch and retry before raising. """
        context = {
            'library': {
                'name': 'analytics-python',
                'version': VERSION
            }
        }
        try:
            post(self.write_key, batch=batch, context=context)
        except:
            if attempt > self.retries:
                raise
            self.request(batch, attempt+1)
