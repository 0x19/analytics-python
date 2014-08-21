from datetime import datetime, timedelta
import collections
import threading
import logging
import numbers
import json

from dateutil.tz import tzutc
import requests

from analytics.stats import Statistics
from analytics.errors import ApiError
from analytics.consumer import Consumer
from analytics.utils import guess_timezone, DatetimeSerializer


try:
    import queue
except:
    import Queue as queue


logging_enabled = True
logger = logging.getLogger('analytics')


def log(level, *args, **kwargs):
    if logging_enabled:
        method = getattr(logger, level)
        method(*args, **kwargs)


def package_exception(client, data, e):
    log('warning', 'Segment.io request error', exc_info=True)
    client._on_failed_flush(data, e)


def package_response(client, data, response):
    # TODO: reduce the complexity (mccabe)
    if response.status_code == 200:
        client._on_successful_flush(data, response)
    elif response.status_code == 400:
        content = response.text
        try:
            body = json.loads(content)

            code = 'bad_request'
            message = 'Bad request'

            if 'error' in body:
                error = body.error

                if 'code' in error:
                    code = error['code']

                if 'message' in error:
                    message = error['message']

            client._on_failed_flush(data, ApiError(code, message))

        except Exception:
            client._on_failed_flush(data, ApiError('Bad Request', content))
    else:
        client._on_failed_flush(data,
                                ApiError(response.status_code, response.text))


def request(client, url, data):

    log('debug', 'Sending request to Segment.io ...')
    try:

        response = requests.post(url,
                                 data=json.dumps(data, cls=DatetimeSerializer),
                                 headers={'content-type': 'application/json'},
                                 timeout=client.timeout)

        log('debug', 'Finished Segment.io request.')

        package_response(client, data, response)

        return response.status_code == 200

    except requests.ConnectionError as e:
        package_exception(client, data, e)
    except requests.Timeout as e:
        package_exception(client, data, e)

    return False



class Client(object):
    """The Client class is a batching asynchronous python wrapper over the
    Segment.io API.

    """

    def __init__(self, write_key=None, debug=False, flush_at=20,
                 max_queue_size=10000, stats=Statistics(),
                 timeout=10, send=True, retries=3):
        """Create a new instance of a analytics-python Client

        :param str write_key: The Segment.io API secret
        :param logging.LOG_LEVEL log_level: The logging log level for the
        client talks to. Use log_level=logging.DEBUG to troubleshoot
        : param bool log: False to turn off logging completely, True by default
        : param int flush_at: Specicies after how many messages the client will
        flush to the server. Use flush_at=1 to disable batching
        : param datetime.timedelta flush_after: Specifies after how much time
        of no flushing that the server will flush. Used in conjunction with
        the flush_at size policy
        : param bool async: True to have the client flush to the server on
        another thread, therefore not blocking code (this is the default).
        False to enable blocking and making the request on the calling thread.
        : param float timeout: Number of seconds before timing out request to
        Segment.io
        : param bool send: True to send requests, False to not send. False to
        turn analytics off (for testing).
        """

        self.queue = queue.Queue(max_queue_size)
        self.consumer = Consumer(self)
        self.write_key = write_key
        self.timeout = timeout
        self.stats = stats
        self.send = send
        self.debug = debug

    def _check_write_key(self):
        if not self.write_key:
            raise Exception('Please set analytics.secret before calling ' +
                            'identify or track.')

    def identify(self, user_id=None, traits={}, context={}, timestamp=None,
                 session_id=None):
        """Identifying a user ties all of their actions to an id, and
        associates user traits to that id.

        :param str user_id: the user's id after they are logged in. It's the
        same id as which you would recognize a signed-in user in your system.

        : param dict traits: a dictionary with keys like subscriptionPlan or
        age. You only need to record a trait once, no need to send it again.
        Accepted value types are string, boolean, ints,, longs, and
        datetime.datetime.

        : param dict context: An optional dictionary with additional
        information thats related to the visit. Examples are userAgent, and IP
        address of the visitor.

        : param datetime.datetime timestamp: If this event happened in the
        past, the timestamp  can be used to designate when the identification
        happened.  Careful with this one,  if it just happened, leave it None.
        If you do choose to provide a timestamp, make sure it has a timezone.
        """

        self._check_for_secret()

        if not user_id:
            raise Exception('Must supply a user_id.')

        if traits is not None and not isinstance(traits, dict):
            raise Exception('Traits must be a dictionary.')

        if context is not None and not isinstance(context, dict):
            raise Exception('Context must be a dictionary.')

        if timestamp is None:
            timestamp = datetime.utcnow().replace(tzinfo=tzutc())
        elif not isinstance(timestamp, datetime):
            raise Exception('Timestamp must be a datetime object.')
        else:
            timestamp = guess_timezone(timestamp)

        cleaned_traits = self._clean(traits)

        action = {'userId':      user_id,
                  'traits':      cleaned_traits,
                  'context':     context,
                  'timestamp':   timestamp.isoformat(),
                  'action':      'identify'}

        context['library'] = 'analytics-python'

        if self._enqueue(action):
            self.stats.identifies += 1

    def track(self, user_id=None, event=None, properties={}, context={},
              timestamp=None):
        """Whenever a user triggers an event, you'll want to track it.

        :param str user_id:  the user's id after they are logged in. It's the
        same id as which you would recognize a signed-in user in your system.

        :param str event: The event name you are tracking. It is recommended
        that it is in human readable form. For example, "Bought T-Shirt"
        or "Started an exercise"

        :param dict properties: A dictionary with items that describe the
        event in more detail. This argument is optional, but highly recommended
        - you'll find these properties extremely useful later. Accepted value
        types are string, boolean, ints, doubles, longs, and datetime.datetime.

        :param dict context: An optional dictionary with additional information
        thats related to the visit. Examples are userAgent, and IP address
        of the visitor.

        :param datetime.datetime timestamp: If this event happened in the past,
        the timestamp   can be used to designate when the identification
        happened.  Careful with this one,  if it just happened, leave it None.
        If you do choose to provide a timestamp, make sure it has a timezone.

        """

        self._check_for_secret()

        if not user_id:
            raise Exception('Must supply a user_id.')

        if not event:
            raise Exception('Event is a required argument as a non-empty ' +
                            'string.')

        if properties is not None and not isinstance(properties, dict):
            raise Exception('Context must be a dictionary.')

        if context is not None and not isinstance(context, dict):
            raise Exception('Context must be a dictionary.')

        if timestamp is None:
            timestamp = datetime.utcnow().replace(tzinfo=tzutc())
        elif not isinstance(timestamp, datetime):
            raise Exception('Timestamp must be a datetime.datetime object.')
        else:
            timestamp = guess_timezone(timestamp)

        cleaned_properties = self._clean(properties)

        action = {'userId':       user_id,
                  'event':        event,
                  'context':      context,
                  'properties':   cleaned_properties,
                  'timestamp':    timestamp.isoformat(),
                  'action':       'track'}

        context['library'] = 'analytics-python'

        if self._enqueue(action):
            self.stats.tracks += 1

    def alias(self, from_id, to_id, context={}, timestamp=None):
        """Aliases an anonymous user into an identified user

        :param str from_id: the anonymous user's id before they are logged in

        :param str to_id: the identified user's id after they're logged in

        :param dict context: An optional dictionary with additional information
        thats related to the visit. Examples are userAgent, and IP address
        of the visitor.

        :param datetime.datetime timestamp: If this event happened in the past,
        the timestamp   can be used to designate when the identification
        happened.  Careful with this one,  if it just happened, leave it None.
        If you do choose to provide a timestamp, make sure it has a timezone.
        """

        self._check_for_secret()

        if not from_id:
            raise Exception('Must supply a from_id.')

        if not to_id:
            raise Exception('Must supply a to_id.')

        if context is not None and not isinstance(context, dict):
            raise Exception('Context must be a dictionary.')

        if timestamp is None:
            timestamp = datetime.utcnow().replace(tzinfo=tzutc())
        elif not isinstance(timestamp, datetime):
            raise Exception('Timestamp must be a datetime.datetime object.')
        else:
            timestamp = guess_timezone(timestamp)

        action = {'from':         from_id,
                  'to':           to_id,
                  'context':      context,
                  'timestamp':    timestamp.isoformat(),
                  'action':       'alias'}

        context['library'] = 'analytics-python'

        if self._enqueue(action):
            self.stats.aliases += 1

    def _should_flush(self):
        """ Determine whether we should sync """

        full = len(self.queue) >= self.flush_at
        stale = self.last_flushed is None

        if not stale:
            stale = datetime.now() - self.last_flushed > self.flush_after

        return full or stale

    def _enqueue(self, msg):
        # if we've disabled sending, just return False
        if not self.send:
            return False

        submitted = False

        if len(self.queue) < self.max_queue_size:
            self.queue.append(action)

            self.stats.submitted += 1

            submitted = True

            log('debug', 'Enqueued ' + action['action'] + '.')

        else:
            log('warn', 'analytics-python queue is full')

        if self._should_flush():
            self.flush()

        return submitted

    def _on_successful_flush(self, data, response):
        if 'batch' in data:
            for item in data['batch']:
                self.stats.successful += 1
                for callback in self.success_callbacks:
                    callback(data, response)

    def _on_failed_flush(self, data, error):
        if 'batch' in data:
            for item in data['batch']:
                self.stats.failed += 1
                for callback in self.failure_callbacks:
                    callback(data, error)

    def _flush_thread_is_free(self):
        return self.flushing_thread is None \
            or not self.flushing_thread.is_alive()

    def flush(self):
        """ Forces a flush from the internal queue to the server"""
        queue = self.queue

        size = queue.qsize()
        queue.join()

        log('debug', 'Successfully flushed {0} items [{1} failed].'.
                     format(str(successful), str(failed)))
