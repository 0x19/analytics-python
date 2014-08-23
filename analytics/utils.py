from dateutil.tz import tzlocal, tzutc
from datetime import datetime
import numbers
import json

import six

try:
    import queue
except:
    import Queue as queue


def is_naive(dt):
    """ Determines if a given datetime.datetime is naive. """
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None

def total_seconds(delta):
    """ Determines total seconds with python < 2.7 compat """
    # http://stackoverflow.com/questions/3694835/python-2-6-5-divide-timedelta-with-timedelta
    return (delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 1e6) / 1e6

def guess_timezone(dt):
    """ Attempts to convert a naive datetime to an aware datetime """
    if is_naive(dt):
        # attempts to guess the datetime.datetime.now() local timezone
        # case, and then defaults to utc

        delta = datetime.now() - dt
        if total_seconds(delta) < 5:
            # this was created using datetime.datetime.now()
            # so we are in the local timezone
            return dt.replace(tzinfo=tzlocal())
        else:
            # at this point, the best we can do (I htink) is guess UTC
            return dt.replace(tzinfo=tzutc())

    return dt

def clean(self, item):
    if isinstance(item, (str, six.text_type, int, six.integer_types, float,
                         bool, numbers.Number, datetime)):
        return item
    elif isinstance(item, (set, list, tuple)):
        return _clean_list(item)
    elif isinstance(item, dict):
        return _clean_dict(item)
    else:
        return _coerce_unicode(item)

def _clean_list(self, l):
    return [clean(item) for item in l]

def _clean_dict(self, d):
    data = {}
    for k, v in six.iteritems(d):
        try:
            data[k] = clean(v)
        except TypeError:
            log('warning', 'Dictionary values must be serializeable to ' +
                        'JSON "%s" value %s of type %s is unsupported.'
                        % (k, v, type(v)))
    return data

def _coerce_unicode(self, cmplx):
    try:
        item = cmplx.decode("utf-8", "strict")
    except AttributeError as exception:
        item = ":".join(exception)
        item.decode("utf-8", "strict")
    except:
        raise
    return item


class DatetimeSerializer(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)
