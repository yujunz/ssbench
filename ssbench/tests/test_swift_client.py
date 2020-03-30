# Copyright (c) 2020 Nvidia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import TestCase
import mock
from contextlib import contextmanager
from StringIO import StringIO

from ssbench import swift_client as c


class StubResponse(object):

    def __init__(self, status, headers=None, body=''):
        self.status = status
        self.headers = headers or {}
        if isinstance(body, StringIO):
            self.body = body
        else:
            self.body = StringIO(body)

    def read(self, *args, **kwargs):
        return self.body.read(*args, **kwargs)

    def getheaders(self):
        return self.headers.items()


class FakeConnection(object):

    def __init__(self, resp):
        if isinstance(resp, list):
            self.resp_pool = resp
        else:
            self.resp_pool = [resp]
        self.requests = []

    def fake_http_connection(self, url):
        self.parsed = c.urlparse(url)
        return self.parsed, self

    @property
    def host(self):
        return self.parsed.hostname

    @property
    def port(self):
        return self.parsed.port

    def request(self, method, path, body='', headers=None):
        headers = headers or {}
        self.requests.append((method, path, body, headers))

    def getresponse(self):
        return self.resp_pool.pop(0)


@contextmanager
def mocked_http(resp):

    fake_conn = FakeConnection(resp)

    with mock.patch('ssbench.swift_client.http_connection',
                    fake_conn.fake_http_connection):
        yield fake_conn
        if fake_conn.resp_pool:
            raise AssertionError('Unused resp:\n' + '\n'.join(fake_conn.resp))


class TestGetObject(TestCase):
    def setUp(self):
        self.url = 'http://storage.example.com/v1/AUTH_test'
        self._parsed = c.urlparse(self.url)
        self.token = 'stub-token'
        self.stub_body = 'TEST' * 65536

    @property
    def path(self):
        return self._parsed.path

    def test_get_object_zero_byte_success(self):
        resp = StubResponse(200)
        with mocked_http(resp) as log:
            c.get_object(self.url, self.token, 'mycontainer', 'myobject')
        self.assertEqual(log.requests, [
            ('GET', self.path + '/mycontainer/myobject', '', {
                'X-Auth-Token': self.token}),
        ])
        self.assertEqual('', resp.read())

    def test_get_object_with_body_success(self):
        resp = StubResponse(200, body=self.stub_body)
        with mocked_http(resp) as log:
            c.get_object(self.url, self.token, 'mycontainer', 'myobject')
        self.assertEqual(log.requests, [
            ('GET', self.path + '/mycontainer/myobject', '', {
                'X-Auth-Token': self.token}),
        ])
        self.assertEqual('', resp.read())

    def test_get_object_with_short_read(self):
        too_long = len(self.stub_body) + 10
        resp = StubResponse(200, headers={'Content-Length': too_long},
                            body=self.stub_body)
        with mocked_http(resp) as log, self.assertRaises(c.ClientException) as ctx:
            c.get_object(self.url, self.token, 'mycontainer', 'myobject')
        self.assertEqual(log.requests, [
            ('GET', self.path + '/mycontainer/myobject', '', {
                'X-Auth-Token': self.token}),
        ])
        self.assertEqual('', resp.read())
        self.assertEqual(str(ctx.exception), 'Object GET read disconnect: '
                         '%s 503 Server Disconnect' % (
                             self.url + '/mycontainer/myobject'))
