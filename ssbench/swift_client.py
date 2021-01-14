#
#Copyright (c) 2012-2021, NVIDIA CORPORATION.
#Copyright (c) 2010-2012 OpenStack, LLC.
#SPDX-License-Identifier: Apache-2.0

# NOTE: hacked by SwiftStack for benchmarking

"""
Cloud Files client library used internally
"""

import socket
import sys
import logging
from time import time
from functools import wraps

from urllib import quote as _quote
from urlparse import urlparse, urlunparse

from httplib import HTTPException
from geventhttpclient.httplib import HTTPConnection, HTTPSConnection
from gevent import sleep


logger = logging.getLogger("swiftclient")


# Timeout, in seconds, for individual read/write (not a timeout for the entire
# get_object or put_object or whatever).
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_NETWORK_TIMEOUT = 20.0


def http_log(args, kwargs, resp, body):
    if not logger.isEnabledFor(logging.DEBUG):
        return

    string_parts = ['curl -i']
    for element in args:
        if element == 'HEAD':
            string_parts.append(' -I')
        elif element in ('GET', 'POST', 'PUT'):
            string_parts.append(' -X %s' % element)
        else:
            string_parts.append(' %s' % element)

    if 'headers' in kwargs:
        for element in kwargs['headers']:
            header = ' -H "%s: %s"' % (element, kwargs['headers'][element])
            string_parts.append(header)

    logger.debug("REQ: %s", "".join(string_parts))
    if 'raw_body' in kwargs:
        logger.debug("REQ BODY (RAW): %s", kwargs['raw_body'])
    if 'body' in kwargs:
        logger.debug("REQ BODY: %s", kwargs['body'])

    logger.debug("RESP STATUS: %s", resp.status)
    if body:
        logger.debug("RESP BODY: %s", body)


def quote(value, safe='/'):
    """
    Patched version of urllib.quote that encodes utf8 strings before quoting
    """
    value = encode_utf8(value)
    if isinstance(value, str):
        return _quote(value, safe)
    else:
        return value


def encode_utf8(value):
    if isinstance(value, unicode):
        value = value.encode('utf8')
    return value


# look for a real json parser first
try:
    # simplejson is popular and pretty good
    from simplejson import loads as json_loads
except ImportError:
    # 2.6 will have a json module in the stdlib
    from json import loads as json_loads


class ClientException(Exception):

    def __init__(self, msg, http_scheme='', http_host='', http_port='',
                 http_path='', http_query='', http_status=0, http_reason='',
                 http_device='', http_response_content=''):
        Exception.__init__(self, msg)
        self.msg = msg
        self.http_scheme = http_scheme
        self.http_host = http_host
        self.http_port = http_port
        self.http_path = http_path
        self.http_query = http_query
        self.http_status = http_status
        self.http_reason = http_reason
        self.http_device = http_device
        self.http_response_content = http_response_content

    def __str__(self):
        a = self.msg
        b = ''
        if self.http_scheme:
            b += '%s://' % self.http_scheme
        if self.http_host:
            b += self.http_host
        if self.http_port:
            b += ':%s' % self.http_port
        if self.http_path:
            b += self.http_path
        if self.http_query:
            b += '?%s' % self.http_query
        if self.http_status:
            if b:
                b = '%s %s' % (b, self.http_status)
            else:
                b = str(self.http_status)
        if self.http_reason:
            if b:
                b = '%s %s' % (b, self.http_reason)
            else:
                b = '- %s' % self.http_reason
        if self.http_device:
            if b:
                b = '%s: device %s' % (b, self.http_device)
            else:
                b = 'device %s' % self.http_device
        if self.http_response_content:
            if len(self.http_response_content) <= 60:
                b += '   %s' % self.http_response_content
            else:
                b += '  [first 60 chars of response] %s' \
                    % self.http_response_content[:60]
        return b and '%s: %s' % (a, b) or a


def http_connection(url, proxy=None, connect_timeout=DEFAULT_CONNECT_TIMEOUT):
    """
    Make an HTTPConnection or HTTPSConnection

    :param url: url to connect to
    :param proxy: proxy to connect through, if any; None by default; str of the
                  format 'http://127.0.0.1:8888' to set one
    :returns: tuple of (parsed url, connection object)
    :raises ClientException: Unable to handle protocol scheme
    """
    url = encode_utf8(url)
    parsed = urlparse(url)
    proxy_parsed = urlparse(proxy) if proxy else None
    if parsed.scheme == 'http':
        conn = HTTPConnection(
            (proxy_parsed if proxy else parsed).netloc,
            timeout=connect_timeout)
    elif parsed.scheme == 'https':
        conn = HTTPSConnection(
            (proxy_parsed if proxy else parsed).netloc,
            timeout=connect_timeout)
    else:
        raise ClientException('Cannot handle protocol scheme %s for url %s' %
                              (parsed.scheme, repr(url)))

    def putheader_wrapper(func):

        @wraps(func)
        def putheader_escaped(key, value):
            func(encode_utf8(key), encode_utf8(value))
        return putheader_escaped
    conn.putheader = putheader_wrapper(conn.putheader)

    def request_wrapper(func):

        @wraps(func)
        def request_escaped(method, url, body=None, headers=None):
            url = encode_utf8(url)
            if body:
                body = encode_utf8(body)
            func(method, url, body=body, headers=headers or {})
        return request_escaped
    conn.request = request_wrapper(conn.request)
    if proxy:
        conn._set_tunnel(parsed.hostname, parsed.port)
    return parsed, conn


def get_auth_1_0(url, user, key, snet):
    parsed, conn = http_connection(url)
    method = 'GET'
    conn.request(method, parsed.path, '',
                 {'X-Auth-User': user, 'X-Auth-Key': key})
    resp = conn.getresponse()
    body = resp.read()
    url = resp.getheader('x-storage-url')
    http_log((url, method,), {}, resp, body)

    # There is a side-effect on current Rackspace 1.0 server where a
    # bad URL would get you that document page and a 200. We error out
    # if we don't have a x-storage-url header and if we get a body.
    if resp.status < 200 or resp.status >= 300 or (body and not url):
        raise ClientException('Auth GET failed', http_scheme=parsed.scheme,
                              http_host=conn.host, http_port=conn.port,
                              http_path=parsed.path, http_status=resp.status,
                              http_reason=resp.reason)
    if snet:
        parsed = list(urlparse(url))
        # Second item in the list is the netloc
        netloc = parsed[1]
        parsed[1] = 'snet-' + netloc
        url = urlunparse(parsed)
    return url, resp.getheader('x-storage-token',
                               resp.getheader('x-auth-token'))


def get_keystoneclient_2_0(auth_url, user, key, os_options, **kwargs):
    """
    Authenticate against a auth 2.0 server.

    We are using the keystoneclient library for our 2.0 authentication.
    """

    insecure = kwargs.get('insecure', False)
    debug = logger.isEnabledFor(logging.DEBUG) and True or False

    try:
        from keystoneclient.v2_0 import client as ksclient
        from keystoneclient import exceptions
    except ImportError:
        sys.exit('''
Auth version 2.0 requires python-keystoneclient, install it or use Auth
version 1.0 which requires ST_AUTH, ST_USER, and ST_KEY environment
variables to be set or overridden with -A, -U, or -K.''')

    try:
        _ksclient = ksclient.Client(username=user,
                                    password=key,
                                    tenant_name=os_options.get('tenant_name'),
                                    tenant_id=os_options.get('tenant_id'),
                                    debug=debug,
                                    cacert=kwargs.get('cacert'),
                                    auth_url=auth_url, insecure=insecure)
    except exceptions.Unauthorized:
        raise ClientException('Unauthorised. Check username, password'
                              ' and tenant name/id')
    except exceptions.AuthorizationFailure, err:
        raise ClientException('Authorization Failure. %s' % err)
    service_type = os_options.get('service_type') or 'object-store'
    endpoint_type = os_options.get('endpoint_type') or 'publicURL'
    try:
        endpoint = _ksclient.service_catalog.url_for(
            attr='region',
            filter_value=os_options.get('region_name'),
            service_type=service_type,
            endpoint_type=endpoint_type)
    except exceptions.EndpointNotFound:
        raise ClientException('Endpoint for %s not found - '
                              'have you specified a region?' % service_type)
    return (endpoint, _ksclient.auth_token)


def get_auth(auth_url, user, key, **kwargs):
    """
    Get authentication/authorization credentials.

    The snet parameter is used for Rackspace's ServiceNet internal network
    implementation. In this function, it simply adds *snet-* to the beginning
    of the host name for the returned storage URL. With Rackspace Cloud Files,
    use of this network path causes no bandwidth charges but requires the
    client to be running on Rackspace's ServiceNet network.
    """
    auth_version = kwargs.get('auth_version', '1')
    os_options = kwargs.get('os_options', {})

    if auth_version in ['1.0', '1', 1]:
        return get_auth_1_0(auth_url,
                            user,
                            key,
                            kwargs.get('snet'))

    if auth_version in ['2.0', '2', 2]:

        # We are allowing to specify a token/storage-url to re-use
        # without having to re-authenticate.
        if (os_options.get('object_storage_url') and
                os_options.get('auth_token')):
            return(os_options.get('object_storage_url'),
                   os_options.get('auth_token'))

        # We are handling a special use case here when we were
        # allowing specifying the account/tenant_name with the -U
        # argument
        if not kwargs.get('tenant_name') and ':' in user:
            (os_options['tenant_name'], user) = user.split(':')

        # We are allowing to have an tenant_name argument in get_auth
        # directly without having os_options
        if kwargs.get('tenant_name'):
            os_options['tenant_name'] = kwargs['tenant_name']

        if ('tenant_name' not in os_options):
            raise ClientException('No tenant specified')

        insecure = kwargs.get('insecure', False)
        cacert = kwargs.get('cacert', None)
        (auth_url, token) = get_keystoneclient_2_0(auth_url, user,
                                                   key, os_options,
                                                   cacert=cacert,
                                                   insecure=insecure)
        return (auth_url, token)

    raise ClientException('Unknown auth_version %s specified.'
                          % auth_version)


def get_account(url, token, marker=None, limit=None, prefix=None,
                http_conn=None, full_listing=False):
    """
    Get a listing of containers for the account.

    :param url: storage URL
    :param token: auth token
    :param marker: marker query
    :param limit: limit query
    :param prefix: prefix query
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param full_listing: if True, return a full listing, else returns a max
                         of 10000 listings
    :returns: a tuple of (response headers, a list of containers) The response
              headers will be a dict and all header names will be lowercase.
    :raises ClientException: HTTP GET request failed
    """
    if not http_conn:
        http_conn = http_connection(url)
    if full_listing:
        rv = get_account(url, token, marker, limit, prefix, http_conn)
        listing = rv[1]
        while listing:
            marker = listing[-1]['name']
            listing = \
                get_account(url, token, marker, limit, prefix, http_conn)[1]
            if listing:
                rv[1].extend(listing)
        return rv
    parsed, conn = http_conn
    qs = 'format=json'
    if marker:
        qs += '&marker=%s' % quote(marker)
    if limit:
        qs += '&limit=%d' % limit
    if prefix:
        qs += '&prefix=%s' % quote(prefix)
    full_path = '%s?%s' % (parsed.path, qs)
    headers = {'X-Auth-Token': token}
    method = 'GET'
    conn.request(method, full_path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(("%s?%s" % (url, qs), method,), {'headers': headers}, resp, body)

    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Account GET failed', http_scheme=parsed.scheme,
                              http_host=conn.host, http_port=conn.port,
                              http_path=parsed.path, http_query=qs,
                              http_status=resp.status, http_reason=resp.reason,
                              http_response_content=body)
    if resp.status == 204:
        return resp_headers, []
    return resp_headers, json_loads(body)


def head_account(url, token, http_conn=None):
    """
    Get account stats.

    :param url: storage URL
    :param token: auth token
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :returns: a dict containing the response's headers (all header names will
              be lowercase)
    :raises ClientException: HTTP HEAD request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    method = "HEAD"
    headers = {'X-Auth-Token': token}
    conn.request(method, parsed.path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log((url, method,), {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Account HEAD failed', http_scheme=parsed.scheme,
                              http_host=conn.host, http_port=conn.port,
                              http_path=parsed.path, http_status=resp.status,
                              http_reason=resp.reason,
                              http_response_content=body)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers


def post_account(url, token, headers, http_conn=None):
    """
    Update an account's metadata.

    :param url: storage URL
    :param token: auth token
    :param headers: additional headers to include in the request
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP POST request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    method = 'POST'
    headers['X-Auth-Token'] = token
    conn.request(method, parsed.path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log((url, method,), {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Account POST failed',
                              http_scheme=parsed.scheme,
                              http_host=conn.host,
                              http_port=conn.port,
                              http_path=parsed.path,
                              http_status=resp.status,
                              http_reason=resp.reason,
                              http_response_content=body)


def get_container(url, token, container, marker=None, limit=None,
                  prefix=None, delimiter=None, http_conn=None,
                  full_listing=False):
    """
    Get a listing of objects for the container.

    :param url: storage URL
    :param token: auth token
    :param container: container name to get a listing for
    :param marker: marker query
    :param limit: limit query
    :param prefix: prefix query
    :param delimeter: string to delimit the queries on
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param full_listing: if True, return a full listing, else returns a max
                         of 10000 listings
    :returns: a tuple of (response headers, a list of objects) The response
              headers will be a dict and all header names will be lowercase.
    :raises ClientException: HTTP GET request failed
    """
    if not http_conn:
        http_conn = http_connection(url)
    if full_listing:
        rv = get_container(url, token, container, marker, limit, prefix,
                           delimiter, http_conn)
        listing = rv[1]
        while listing:
            if not delimiter:
                marker = listing[-1]['name']
            else:
                marker = listing[-1].get('name', listing[-1].get('subdir'))
            listing = get_container(url, token, container, marker, limit,
                                    prefix, delimiter, http_conn)[1]
            if listing:
                rv[1].extend(listing)
        return rv
    parsed, conn = http_conn
    path = '%s/%s' % (parsed.path, quote(container))
    qs = 'format=json'
    if marker:
        qs += '&marker=%s' % quote(marker)
    if limit:
        qs += '&limit=%d' % limit
    if prefix:
        qs += '&prefix=%s' % quote(prefix)
    if delimiter:
        qs += '&delimiter=%s' % quote(delimiter)
    headers = {'X-Auth-Token': token}
    method = 'GET'
    conn.request(method, '%s?%s' % (path, qs), '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(('%s?%s' % (url, qs), method,), {'headers': headers}, resp, body)

    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container GET failed',
                              http_scheme=parsed.scheme, http_host=conn.host,
                              http_port=conn.port, http_path=path,
                              http_query=qs, http_status=resp.status,
                              http_reason=resp.reason,
                              http_response_content=body)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    if resp.status == 204:
        return resp_headers, []
    return resp_headers, json_loads(body)


def head_container(url, token, container, http_conn=None, headers=None):
    """
    Get container stats.

    :param url: storage URL
    :param token: auth token
    :param container: container name to get stats for
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :returns: a dict containing the response's headers (all header names will
              be lowercase)
    :raises ClientException: HTTP HEAD request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s' % (parsed.path, quote(container))
    method = 'HEAD'
    req_headers = {'X-Auth-Token': token}
    if headers:
        req_headers.update(headers)
    conn.request(method, path, '', req_headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), method,),
             {'headers': req_headers}, resp, body)

    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container HEAD failed',
                              http_scheme=parsed.scheme, http_host=conn.host,
                              http_port=conn.port, http_path=path,
                              http_status=resp.status, http_reason=resp.reason,
                              http_response_content=body)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers


def put_container(url, token, container, headers=None, http_conn=None):
    """
    Create a container

    :param url: storage URL
    :param token: auth token
    :param container: container name to create
    :param headers: additional headers to include in the request
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP PUT request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s' % (parsed.path, quote(container))
    method = 'PUT'
    if not headers:
        headers = {}
    headers['X-Auth-Token'] = token
    if 'content-length' not in (k.lower() for k in headers):
        headers['Content-Length'] = 0
    conn.request(method, path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), method,),
             {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container PUT failed',
                              http_scheme=parsed.scheme, http_host=conn.host,
                              http_port=conn.port, http_path=path,
                              http_status=resp.status, http_reason=resp.reason,
                              http_response_content=body)


def post_container(url, token, container, headers, http_conn=None):
    """
    Update a container's metadata.

    :param url: storage URL
    :param token: auth token
    :param container: container name to update
    :param headers: additional headers to include in the request
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP POST request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s' % (parsed.path, quote(container))
    method = 'POST'
    headers['X-Auth-Token'] = token
    if 'content-length' not in (k.lower() for k in headers):
        headers['Content-Length'] = 0
    conn.request(method, path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), method,),
             {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container POST failed',
                              http_scheme=parsed.scheme, http_host=conn.host,
                              http_port=conn.port, http_path=path,
                              http_status=resp.status, http_reason=resp.reason,
                              http_response_content=body)


def delete_container(url, token, container, http_conn=None):
    """
    Delete a container

    :param url: storage URL
    :param token: auth token
    :param container: container name to delete
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP DELETE request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s' % (parsed.path, quote(container))
    headers = {'X-Auth-Token': token}
    method = 'DELETE'
    conn.request(method, path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), method,),
             {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container DELETE failed',
                              http_scheme=parsed.scheme, http_host=conn.host,
                              http_port=conn.port, http_path=path,
                              http_status=resp.status, http_reason=resp.reason,
                              http_response_content=body)


def get_object(url, token, container, name, http_conn=None,
               resp_chunk_size=65536):
    """
    Modified for benchmarking to GET an object in "chunk sizes" of
    resp_chunk_size, throwing away the actual contents.

    :param url: storage URL
    :param token: auth token
    :param container: container name that the object is in
    :param name: object name to get
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param resp_chunk_size: chunk size of data to read; defaults to 65536.
    :returns: benchmarking-decorated response headers.
    :raises ClientException: HTTP GET request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s/%s' % (parsed.path, quote(container), quote(name))
    method = 'GET'
    headers = {'X-Auth-Token': token}
    start = time()
    conn.request(method, path, '', headers)
    resp = conn.getresponse()
    first_byte_latency = time() - start
    if resp.status < 200 or resp.status >= 300:
        body = resp.read()
        http_log(('%s%s' % (url.replace(parsed.path, ''), path), method,),
                 {'headers': headers}, resp, body)
        raise ClientException('Object GET failed', http_scheme=parsed.scheme,
                              http_host=conn.host, http_port=conn.port,
                              http_path=path, http_status=resp.status,
                              http_reason=resp.reason,
                              http_response_content=body)
    last_byte_latency = None
    buf = True
    while buf:
        buf = resp.read(resp_chunk_size)
    last_byte_latency = time() - start
    resp_headers = _decorated_response_headers(
        resp, first_byte_latency=first_byte_latency,
        last_byte_latency=last_byte_latency)
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), method,),
             {'headers': headers}, resp, None)
    return resp_headers


def _decorated_response_headers(resp, first_byte_latency=None,
                                last_byte_latency=None):
    resp_headers = {}
    if first_byte_latency is not None:
        resp_headers['x-swiftstack-first-byte-latency'] = first_byte_latency
    if last_byte_latency is not None:
        resp_headers['x-swiftstack-last-byte-latency'] = last_byte_latency
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers


def head_object(url, token, container, name, http_conn=None):
    """
    Get object info

    :param url: storage URL
    :param token: auth token
    :param container: container name that the object is in
    :param name: object name to get info for
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :returns: a dict containing the response's headers (all header names will
              be lowercase)
    :raises ClientException: HTTP HEAD request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s/%s' % (parsed.path, quote(container), quote(name))
    method = 'HEAD'
    headers = {'X-Auth-Token': token}
    start_time = time()
    conn.request(method, path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), method,),
             {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Object HEAD failed', http_scheme=parsed.scheme,
                              http_host=conn.host, http_port=conn.port,
                              http_path=path, http_status=resp.status,
                              http_reason=resp.reason,
                              http_response_content=body)
    now = time()
    return _decorated_response_headers(
        resp, last_byte_latency=now - start_time)


def put_object(url, token=None, container=None, name=None, contents=None,
               content_length=None, chunk_size=65536,
               content_type=None, headers=None, http_conn=None, proxy=None):
    """
    Modified for benchmarking to take a constant string in "contents" and write
    out the first "chunk_size" bytes of "contents" until "content_length" bytes
    have been sent.  A "contents" value of None will still do a zero-byte PUT.

    If the length of contents is less than chunk_size, the length of contents
    will be the de facto chunk size.

    :param url: storage URL
    :param token: auth token; if None, no token will be sent
    :param container: container name that the object is in; if None, the
                      container name is expected to be part of the url
    :param name: object name to put; if None, the object name is expected to be
                 part of the url
    :param contents: a static string; if None, a zero-byte put will be done
    :param content_length: value to send as content-length header; also limits
                           the amount of bytes from "contents" sent.  Cannot be
                           None.
    :param chunk_size: chunk size of data to write; default 65536
    :param content_type: value to send as content-type header; if None, no
                         content-type will be set (remote end will likely try
                         to auto-detect it)
    :param headers: additional headers to include in the request, if any
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param proxy: proxy to connect through, if any; None by default; str of the
                  format 'http://127.0.0.1:8888' to set one
    :returns: dict with benchmarking headers
    :raises ClientException: HTTP PUT request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url, proxy=proxy)
    path = parsed.path
    if container:
        path = '%s/%s' % (path.rstrip('/'), quote(container))
    if name:
        path = '%s/%s' % (path.rstrip('/'), quote(name))
    if headers:
        headers = dict(headers)
    else:
        headers = {}
    if token:
        headers['X-Auth-Token'] = token
    if not contents:
        content_length = 0
    if content_length is not None:
        headers['Content-Length'] = str(content_length)
    else:
        raise ValueError('For benchmarking, content_length cannot be None!')
    if content_type is not None:
        headers['Content-Type'] = content_type
    request_start = time()
    conn.putrequest('PUT', path)
    for header, value in headers.iteritems():
        conn.putheader(header, value)
    conn.endheaders()
    left = content_length
    chunk_size = min(chunk_size, len(contents))
    while left > 0:
        if left < chunk_size:
            conn.send(contents[:left])
            left = 0
        else:
            conn.send(contents)
            left -= chunk_size
    resp = conn.getresponse()
    body = resp.read()
    headers = {'X-Auth-Token': token}
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), 'PUT',),
             {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Object PUT failed', http_scheme=parsed.scheme,
                              http_host=conn.host, http_port=conn.port,
                              http_path=path, http_status=resp.status,
                              http_reason=resp.reason,
                              http_response_content=body)
    return _decorated_response_headers(
        resp, last_byte_latency=time() - request_start)


def post_object(url, token, container, name, headers, http_conn=None):
    """
    Update object metadata

    :param url: storage URL
    :param token: auth token
    :param container: container name that the object is in
    :param name: name of the object to update
    :param headers: additional headers to include in the request
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP POST request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s/%s' % (parsed.path, quote(container), quote(name))
    headers['X-Auth-Token'] = token
    conn.request('POST', path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), 'POST',),
             {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Object POST failed', http_scheme=parsed.scheme,
                              http_host=conn.host, http_port=conn.port,
                              http_path=path, http_status=resp.status,
                              http_reason=resp.reason,
                              http_response_content=body)


def delete_object(url, token=None, container=None, name=None, http_conn=None,
                  headers=None, proxy=None):
    """
    Delete object

    :param url: storage URL
    :param token: auth token; if None, no token will be sent
    :param container: container name that the object is in; if None, the
                      container name is expected to be part of the url
    :param name: object name to delete; if None, the object name is expected to
                 be part of the url
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param headers: additional headers to include in the request
    :param proxy: proxy to connect through, if any; None by default; str of the
                  format 'http://127.0.0.1:8888' to set one
    :returns: A dict of response headers including observed latencies
    :raises ClientException: HTTP DELETE request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url, proxy=proxy)
    path = parsed.path
    if container:
        path = '%s/%s' % (path.rstrip('/'), quote(container))
    if name:
        path = '%s/%s' % (path.rstrip('/'), quote(name))
    if headers:
        headers = dict(headers)
    else:
        headers = {}
    if token:
        headers['X-Auth-Token'] = token
    start_time = time()
    conn.request('DELETE', path, '', headers)
    resp = conn.getresponse()
    body = resp.read()
    http_log(('%s%s' % (url.replace(parsed.path, ''), path), 'DELETE',),
             {'headers': headers}, resp, body)
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Object DELETE failed',
                              http_scheme=parsed.scheme, http_host=conn.host,
                              http_port=conn.port, http_path=path,
                              http_status=resp.status, http_reason=resp.reason,
                              http_response_content=body)
    return _decorated_response_headers(
        resp, last_byte_latency=time() - start_time)


class Connection(object):
    """Convenience class to make requests that will also retry the request"""

    def __init__(self, authurl=None, user=None, key=None, retries=5,
                 preauthurl=None, preauthtoken=None, snet=False,
                 starting_backoff=1, tenant_name=None, os_options=None,
                 auth_version="1", cacert=None, insecure=False):
        """
        :param authurl: authentication URL
        :param user: user name to authenticate as
        :param key: key/password to authenticate with
        :param retries: Number of times to retry the request before failing
        :param preauthurl: storage URL (if you have already authenticated)
        :param preauthtoken: authentication token (if you have already
                             authenticated) note authurl/user/key/tenant_name
                             are not required when specifying preauthtoken
        :param snet: use SERVICENET internal network default is False
        :param auth_version: OpenStack auth version, default is 1.0
        :param tenant_name: The tenant/account name, required when connecting
                            to a auth 2.0 system.
        :param os_options: The OpenStack options which can have tenant_id,
                           auth_token, service_type, endpoint_type,
                           tenant_name, object_storage_url, region_name
        :param insecure: Allow to access insecure keystone server.
                         The keystone's certificate will not be verified.
        """
        self.authurl = authurl
        self.user = user
        self.key = key
        self.retries = retries
        self.http_conn = None
        self.url = preauthurl
        self.token = preauthtoken
        self.attempts = 0
        self.snet = snet
        self.starting_backoff = starting_backoff
        self.auth_version = auth_version
        self.os_options = os_options or {}
        if tenant_name:
            self.os_options['tenant_name'] = tenant_name
        self.cacert = cacert
        self.insecure = insecure

    def get_auth(self):
        return get_auth(self.authurl,
                        self.user,
                        self.key,
                        snet=self.snet,
                        auth_version=self.auth_version,
                        os_options=self.os_options,
                        cacert=self.cacert,
                        insecure=self.insecure)

    def http_connection(self):
        return http_connection(self.url)

    def _retry(self, reset_func, func, *args, **kwargs):
        self.attempts = 0
        backoff = self.starting_backoff
        while self.attempts <= self.retries:
            self.attempts += 1
            try:
                if not self.url or not self.token:
                    self.url, self.token = self.get_auth()
                    self.http_conn = None
                if not self.http_conn:
                    self.http_conn = self.http_connection()
                kwargs['http_conn'] = self.http_conn
                rv = func(self.url, self.token, *args, **kwargs)
                return rv
            except (socket.error, HTTPException):
                if self.attempts > self.retries:
                    raise
                self.http_conn = None
            except ClientException, err:
                if self.attempts > self.retries:
                    raise
                if err.http_status == 401:
                    self.url = self.token = None
                    if self.attempts > 1:
                        raise
                elif err.http_status == 408:
                    self.http_conn = None
                elif 500 <= err.http_status <= 599:
                    pass
                else:
                    raise
            sleep(backoff)
            backoff *= 2
            if reset_func:
                reset_func()

    def head_account(self):
        """Wrapper for :func:`head_account`"""
        return self._retry(None, head_account)

    def get_account(self, marker=None, limit=None, prefix=None,
                    full_listing=False):
        """Wrapper for :func:`get_account`"""
        # TODO(unknown): With full_listing=True this will restart the entire
        # listing with each retry. Need to make a better version that just
        # retries where it left off.
        return self._retry(None, get_account, marker=marker, limit=limit,
                           prefix=prefix, full_listing=full_listing)

    def post_account(self, headers):
        """Wrapper for :func:`post_account`"""
        return self._retry(None, post_account, headers)

    def head_container(self, container):
        """Wrapper for :func:`head_container`"""
        return self._retry(None, head_container, container)

    def get_container(self, container, marker=None, limit=None, prefix=None,
                      delimiter=None, full_listing=False):
        """Wrapper for :func:`get_container`"""
        # TODO(unknown): With full_listing=True this will restart the entire
        # listing with each retry. Need to make a better version that just
        # retries where it left off.
        return self._retry(None, get_container, container, marker=marker,
                           limit=limit, prefix=prefix, delimiter=delimiter,
                           full_listing=full_listing)

    def put_container(self, container, headers=None):
        """Wrapper for :func:`put_container`"""
        return self._retry(None, put_container, container, headers=headers)

    def post_container(self, container, headers):
        """Wrapper for :func:`post_container`"""
        return self._retry(None, post_container, container, headers)

    def delete_container(self, container):
        """Wrapper for :func:`delete_container`"""
        return self._retry(None, delete_container, container)

    def head_object(self, container, obj):
        """Wrapper for :func:`head_object`"""
        return self._retry(None, head_object, container, obj)

    def get_object(self, container, obj, resp_chunk_size=65536):
        """Wrapper for :func:`get_object`"""
        return self._retry(None, get_object, container, obj,
                           resp_chunk_size=resp_chunk_size)

    def put_object(self, container, obj, contents, content_length=None,
                   chunk_size=65536, content_type=None,
                   headers=None):
        """Wrapper for :func:`put_object`"""

        def _default_reset():
            raise ClientException('put_object(%r, %r, ...) failure and no '
                                  'ability to reset contents for reupload.'
                                  % (container, obj))

        reset_func = _default_reset
        tell = getattr(contents, 'tell', None)
        seek = getattr(contents, 'seek', None)
        if tell and seek:
            orig_pos = tell()
            reset_func = lambda: seek(orig_pos)
        elif not contents:
            reset_func = lambda: None

        return self._retry(reset_func, put_object, container, obj, contents,
                           content_length=content_length,
                           chunk_size=chunk_size, content_type=content_type,
                           headers=headers)

    def post_object(self, container, obj, headers):
        """Wrapper for :func:`post_object`"""
        return self._retry(None, post_object, container, obj, headers)

    def delete_object(self, container, obj):
        """Wrapper for :func:`delete_object`"""
        return self._retry(None, delete_object, container, obj)
