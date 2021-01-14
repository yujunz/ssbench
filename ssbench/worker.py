#
#Copyright (c) 2012-2021, NVIDIA CORPORATION.
#Copyright (c) 2010-2012 OpenStack, LLC.
#SPDX-License-Identifier: Apache-2.0
#
# Portions of this file copied from swift/common/bench.py

import gevent
import gevent.pool
import gevent.queue
import gevent.local
import gevent.coros
import gevent.monkey
gevent.monkey.patch_socket()
gevent.monkey.patch_ssl()
gevent.monkey.patch_time()

import os
import time
import socket
import msgpack
import logging
import traceback
import zmq.green as zmq
from httplib import CannotSendRequest
from contextlib import contextmanager
from geventhttpclient.response import HTTPConnectionClosed

from ssbench.importer import random
from ssbench.util import add_dicts, raise_file_descriptor_limit
from ssbench.util import is_ipv6
import ssbench.swift_client as client


DEFAULT_BLOCK_SIZE = 2 ** 16  # 65536


class ConnectionPool(gevent.queue.Queue):
    def __init__(self, factory, factory_kwargs, maxsize=1,
                 network_timeout=client.DEFAULT_NETWORK_TIMEOUT):
        self.factory = factory
        self.factory_kwargs = factory_kwargs
        self.network_timeout = network_timeout

        gevent.queue.Queue.__init__(self, maxsize)

        logging.info('Initializing ConnectionPool to %s with %d '
                     'connections...',
                     factory_kwargs.get('url', 'UNKNOWN'), maxsize)
        for _ in xrange(maxsize):
            self.put(self.create(is_initial=True))  # pylint: disable=E1101

    def create(self, is_initial=False):
        if not is_initial:
            logging.info('ConnectionPool: re-creating connection...')
        conn = None
        try:
            conn = self.factory(**self.factory_kwargs)
            conn[1].connect()
        except (socket.error, socket.timeout):
            # Give the server a little time, then try one more time...
            gevent.sleep(0.01)
            conn = self.factory(**self.factory_kwargs)
            conn[1].connect()
        if not conn:
            if is_initial:
                connect_type = 'connect'
            else:
                connect_type = 'reconnect'
            raise Exception('ConnectionPool failed to %s!' % connect_type)
        conn[1].sock.settimeout(self.network_timeout)
        assert conn[1].sock.timeout == self.network_timeout
        return conn


class Worker(object):
    def __init__(self, zmq_host, zmq_work_port, zmq_results_port, worker_id,
                 max_retries, profile_count=0, concurrency=256, batch_size=1):
        work_endpoint = 'tcp://%s:%d' % (zmq_host, zmq_work_port)
        results_endpoint = 'tcp://%s:%d' % (zmq_host, zmq_results_port)
        ipv6 = is_ipv6(zmq_host)
        self.worker_id = worker_id
        self.max_retries = max_retries
        self.profile_count = profile_count
        self.batch_size = batch_size

        raise_file_descriptor_limit()

        self.concurrency = concurrency
        self.conn_pools_lock = gevent.coros.Semaphore(1)
        self.conn_pools = {}  # hashed by storage_url
        self.token_data = {}
        self.token_data_lock = gevent.coros.Semaphore(1)

        self.context = zmq.Context()
        self.work_pull = self.context.socket(zmq.PULL)
        self.work_pull.ipv6 = ipv6
        self.work_pull.connect(work_endpoint)
        self.results_push = self.context.socket(zmq.PUSH)
        self.results_push.ipv6 = ipv6
        self.results_push.connect(results_endpoint)

        self.result_queue = gevent.queue.Queue()

    @contextmanager
    def connection(self, storage_url):
        pool = self.conn_pools[storage_url]
        try:
            hc = pool.get()
            try:
                yield hc
            except (CannotSendRequest, HTTPConnectionClosed,
                    socket.timeout, socket.error) as e:
                logging.debug("@connection hit %r...", e)
                try:
                    hc[1].close()
                except Exception:
                    pass
                hc = pool.create()
        finally:
            if hc and hc[1] and not hc[1].sock.closed:
                pool.put(hc)
            else:
                pool.put(pool.create())

    def go(self):
        logging.debug('Worker %s starting...', self.worker_id)
        gevent.spawn(self._result_writer)
        pool = gevent.pool.Pool(self.concurrency)
        jobs = self.work_pull.recv()
        if self.profile_count:
            import cProfile
            prof = cProfile.Profile()
            prof.enable()
        gotten = 1
        self.spawned = 0
        while jobs:
            job_data = msgpack.loads(jobs, use_list=False)
            for job_datum in job_data:
                try:
                    if 'container' in job_datum:
                        logging.debug('WORK: %13s %s/%-17s',
                                      job_datum['type'],
                                      job_datum['container'],
                                      job_datum['name'])
                    else:
                        logging.debug('CMD: %13s', job_datum['type'])
                except Exception as e:
                    # Under heavy load with VMs on my laptop, I saw job_datum
                    # apparently somehow equal to None.
                    self.put_exception_results({'job_datum': job_datum}, e)
                    continue

                if job_datum['type'] == 'SUICIDE':
                    logging.info('Got SUICIDE; closing sockets and exiting.')
                    self.work_pull.close()
                    self.results_push.close()
                    os._exit(88)
                pool.spawn(self.handle_job, job_datum)
                self.spawned += 1
                if self.profile_count and gotten >= self.profile_count:
                    prof.disable()
                    prof_output_path = '/tmp/worker_go.%d.prof' % os.getpid()
                    prof.dump_stats(prof_output_path)
                    logging.info('PROFILED worker go() to %s',
                                 prof_output_path)
                    self.profile_count = None
                gotten += 1
            jobs = self.work_pull.recv()

    def _result_writer(self):
        while True:
            result = self.result_queue.get()

            result_q = [result]
            while len(result_q) < self.batch_size:
                try:
                    result = self.result_queue.get(timeout=1)
                    result_q.append(result)
                except gevent.queue.Empty:
                    # timed out, go ahead and send
                    break

            if self.results_push.closed:
                logging.warning('_result_writer: exiting due to closed '
                                'socket!')
                break
            self.spawned -= len(result_q)
            self.results_push.send(msgpack.dumps(result_q))

    def handle_job(self, job_data):
        # Dispatch type to a handler, if possible
        if job_data.get('noop', False):
            handler = self.handle_noop
        else:
            handler = getattr(self, 'handle_%s' % job_data['type'], None)
        if handler:
            try:
                handler(job_data)
            except Exception as e:
                # If the handler threw an exception, we need to put a "result"
                # anyway so the master can finish by reading the requisite
                # number of results without having to timeout.
                self.put_exception_results(job_data, e)
        else:
            raise NameError("Unknown job type %r" % job_data['type'])

    def _create_connection_pool(
            self, storage_url,
            connect_timeout=client.DEFAULT_CONNECT_TIMEOUT,
            network_timeout=client.DEFAULT_NETWORK_TIMEOUT):
        self.conn_pools_lock.acquire()
        try:
            if storage_url not in self.conn_pools:
                self.conn_pools[storage_url] = ConnectionPool(
                    client.http_connection,
                    dict(url=storage_url, connect_timeout=connect_timeout),
                    self.concurrency,
                    network_timeout=network_timeout)
        finally:
            self.conn_pools_lock.release()

    def _token_key(self, auth_kwargs):
        parts = []
        for key in sorted(auth_kwargs.keys()):
            value = auth_kwargs.get(key, '') or ''
            if isinstance(value, dict):
                parts.append(self._token_key(value))
            elif isinstance(value, list):
                parts.extend(value)
            else:
                parts.append(value)
        return '\x01'.join(map(str, parts))

    def ignoring_http_responses(self, statuses, fn, call_info, **extra_keys):
        if 401 not in statuses:
            statuses += (401,)
        args = dict(
            container=call_info['container'],
            name=call_info['name'],
        )
        args.update(extra_keys)

        if 'auth_kwargs' not in call_info:
            raise ValueError('Got benchmark job without "auth_kwargs" key!')

        tries = 0
        while True:
            # Make sure we've got a current storage_url/token
            if call_info['auth_kwargs'].get('token', None):
                token_key = None
                args['url'] = random.choice(
                    call_info['auth_kwargs']['storage_urls'])
                args['token'] = call_info['auth_kwargs']['token']
            else:
                token_key = self._token_key(call_info['auth_kwargs'])
                if token_key not in self.token_data:
                    self.token_data_lock.acquire()
                    collided = False
                    try:
                        if token_key not in self.token_data:
                            logging.debug('Authenticating with %r',
                                          call_info['auth_kwargs'])
                            storage_url, token = client.get_auth(
                                **call_info['auth_kwargs'])
                            override_urls = call_info['auth_kwargs'].get(
                                'storage_urls', None)
                            if override_urls:
                                logging.debug(
                                    'Will override auth storage url %s with '
                                    'one of %r', storage_url, override_urls)
                                storage_urls = override_urls
                            else:
                                storage_urls = [storage_url]
                            self.token_data[token_key] = (storage_urls, token)
                        else:
                            collided = True
                    finally:
                        self.token_data_lock.release()
                    if collided:
                        # Wait just a little bit if we just collided with
                        # another greenthread's re-auth
                        logging.debug('Collided on re-auth; sleeping 0.005')
                        gevent.sleep(0.005)
                storage_urls, args['token'] = self.token_data[token_key]
                args['url'] = random.choice(storage_urls)

            # Check for connection pool initialization (protected by a
            # semaphore)
            if args['url'] not in self.conn_pools:
                self._create_connection_pool(
                    args['url'],
                    call_info.get('connect_timeout',
                                  client.DEFAULT_CONNECT_TIMEOUT),
                    call_info.get('network_timeout',
                                  client.DEFAULT_NETWORK_TIMEOUT))

            try:
                fn_results = None
                with self.connection(args['url']) as conn:
                    fn_results = fn(http_conn=conn, **args)
                if fn_results:
                    if tries != 0:
                        logging.info('%r succeeded after %d tries',
                                     call_info, tries)
                    break
                tries += 1
                if tries > self.max_retries:
                    e = Exception('No fn_results for %r after %d retires' % (
                        fn, self.max_retries))
                    e.retries = tries - 1
                    raise e
            # XXX The name of this method does not suggest that it
            # will also retry on socket-level errors. Regardless,
            # sometimes Swift refuses connections (probably when it's
            # way overloaded and the listen socket's connection queue
            # (in the kernel) is full, so the kernel just says RST).
            #
            # UPDATE: connections should be handled by the ConnectionPool
            # (which will trap socket.error and retry after a slight delay), so
            # socket.error should NOT get raised here for connection failures.
            # So hopefully this socket.error trapping code path will not get
            # hit.
            except socket.error as error:
                tries += 1
                if tries > self.max_retries:
                    error.retries = tries - 1
                    raise error
            except client.ClientException as error:
                tries += 1
                if error.http_status in statuses and \
                        tries <= self.max_retries:
                    if error.http_status == 401 and token_key:
                        if token_key in self.token_data and \
                                self.token_data[token_key][1] == args['token']:
                            self.token_data_lock.acquire()
                            try:
                                if token_key in self.token_data and \
                                        self.token_data[token_key][1] == \
                                        args['token']:
                                    logging.debug(
                                        'Deleting token %s',
                                        self.token_data[token_key][1])
                                    del self.token_data[token_key]
                            finally:
                                self.token_data_lock.release()
                    logging.debug("Retrying an error: %r", error)
                else:
                    error.retries = tries - 1
                    raise error
        fn_results['retries'] = tries
        return fn_results

    def put_results(self, *args, **kwargs):
        """
        Put work result into stats queue.  Given *args and **kwargs are
        combined per add_dicts().  This worker's "ID" and the time of
        completion are included in the results.

        :*args: An optional list of dicts (to be combined via add_dicts())
        :**kwargs: An optional set of key/value pairs (to be combined via
                   add_dicts())
        :returns: (nothing)
        """
        self.result_queue.put(add_dicts(*args,
                                        completed_at=time.time(),
                                        worker_id=self.worker_id,
                                        **kwargs))

    def put_exception_results(self, job_data, e):
        # last arg is assumed as the # of retries
        self.put_results(job_data,
                         exception=repr(e),
                         retries=getattr(e, 'retries', 0),
                         traceback=traceback.format_exc())

    def _put_results_from_response(self, object_info, resp_headers):
        # Strip some keys the job had that results don't need:
        object_info.pop('network_timeout', None)
        object_info.pop('connect_timeout', None)
        object_info.pop('auth_kwargs', None)
        object_info.pop('head_first', None)
        object_info.pop('block_size', None)
        self.put_results(
            object_info,
            first_byte_latency=resp_headers.get(
                'x-swiftstack-first-byte-latency', None),
            last_byte_latency=resp_headers.get(
                'x-swiftstack-last-byte-latency', None),
            trans_id=resp_headers.get('x-trans-id', None),
            retries=resp_headers.get('retries', 0))

    def handle_noop(self, object_info):
        self.put_results(
            object_info,
            first_byte_latency=0.0,
            last_byte_latency=0.0,
            trans_id=None,
            retries=0)
    handle_PING = handle_noop

    def handle_upload_object(self, object_info, letter='A'):
        if object_info.get('head_first'):
            # Only upload if it's not already present
            try:
                headers = self.ignoring_http_responses(
                    (503,), client.head_object, object_info)
            except client.ClientException:
                # Not present, so continue on to the upload
                pass
            else:
                self._put_results_from_response(object_info, headers)
                return
        object_info['size'] = int(object_info['size'])
        block_size = object_info.get('block_size') or DEFAULT_BLOCK_SIZE
        contents = letter * block_size
        send_headers = {}
        if object_info.get('delete_after'):
            send_headers.update(
                {'x-delete-after': object_info.get('delete_after')})
        headers = self.ignoring_http_responses(
            (503,), client.put_object, object_info,
            content_length=object_info['size'],
            chunk_size=block_size, contents=contents,
            headers=send_headers)
        self._put_results_from_response(object_info, headers)

    # By the time a job gets to the worker, an object create and update look
    # the same: it's just a PUT.  We use a different letter for the contents
    # for testability.
    def handle_update_object(self, object_info):
        return self.handle_upload_object(object_info, letter='B')

    def handle_delete_object(self, object_info):
        headers = self.ignoring_http_responses(
            (404, 503), client.delete_object, object_info)
        self._put_results_from_response(object_info, headers)

    def handle_get_object(self, object_info):
        headers = self.ignoring_http_responses(
            (404, 503), client.get_object, object_info,
            resp_chunk_size=object_info.get('block_size', DEFAULT_BLOCK_SIZE))
        self._put_results_from_response(object_info, headers)
