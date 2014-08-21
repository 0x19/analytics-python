from requests.auth import HTTPBasicAuth
import requests
import json


from analytics.utils import DatetimeSerializer


def post(write_key, **kwargs):
    body = kwargs
    url = 'https://api.segment.io/v1/batch'
    auth = HTTPBasicAuth(write_key, '')
    data = json.dumps(body, cls=DatetimeSerializer)
    headers = { 'content-type': 'application/json' }
    res = requests.post(url, data=data, auth=auth, headers=headers)

    if res.status_code == 200:
        return res

    try:
        payload = res.json()
        raise APIError(res.status_code, payload['code'], payload['message'])
    except ValueError:
        raise APIError(res.status_code, 'unknown', res.text)


class APIError(Exception):

    def __init__(self, status, code, message):
        self.message = message
        self.status = status
        self.code = code

    def __str__(self):
        msg = "[Segment] {0}: {1} ({2})"
        return msg.format(self.code, self.message, self.status)