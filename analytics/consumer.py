from threading import Thread
import requests
import logging

log = logging.getLogger('analytics')


class Consumer(Thread):
    """ Consumes the messages from the client's queue. """

    def __init__(self, client):
        super(Thread, self).__init__()
        self.queue = client.queue

    def run(self):
        """ Runs the consumer. """

        log('debug', 'consumer is running...')
        while True:
            batch = self.next()
            try:
                self.upload(batch)
            except:
                log('error')

            self.finish(batch)

    def next(self):
        """ Block and return the next batch of items to upload. """
        queue = self.queue
        item = queue.get(block=True)
        items = [item]

        while len(items) < self.upload_size and not queue.empty():
            items.append(queue.get(block=True))

        return items

    def finish(self, batch):
        """ Marks all items in the batch as being done. """
        for item in batch:
            self.queue.task_done()

    def upload(self, batch, attempt=0):
        """ Attempt to upload the batch and retry before raising. """
        try:
            requests.post(self.write_key, batch=batch)
        except:
            if attempt > self.retries:
                raise
            self.upload(batch, attempt+1)
