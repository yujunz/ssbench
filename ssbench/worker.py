# Copyright (c) 2012-2013 SwiftStack, Inc.
# Copyright (c) 2010-2012 OpenStack, LLC.
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
#
# Portions of this file copied from swift/common/bench.py

import re
import yaml
import random
import time
from functools import partial
from contextlib import contextmanager
import logging

import eventlet
import eventlet.pools
from eventlet.green.httplib import CannotSendRequest
socket = eventlet.import_patched('socket')
beanstalkc = eventlet.import_patched('beanstalkc')

import ssbench
import ssbench.swift_client as client


def add_dicts(*args, **kwargs):
    """
    Utility to "add" together zero or more dicts passed in as positional
    arguments with kwargs.  The positional argument dicts, if present, are not
    mutated.
    """
    result = {}
    for d in args:
        result.update(d)
    result.update(kwargs)
    return result


class ConnectionPool(eventlet.pools.Pool):
    def __init__(self, storage_url, size):
        self.storage_url = storage_url
        eventlet.pools.Pool.__init__(self, size, size)

    def create(self):
        return client.http_connection(self.storage_url)


class ChunkedReader(object):
    def __init__(self, letter, size):
        self.size = size
        self.letter = letter
        self.bytes_left = int(size)  # in case it's a float

    def __eq__(self, other_reader):
        if isinstance(other_reader, ChunkedReader):
            return self.size == other_reader.size and \
                self.letter == other_reader.letter

    def read(self, chunk_size):
        if self.bytes_left == 0:
            return None
        elif self.bytes_left < chunk_size:
            chunk_size = self.bytes_left
        self.bytes_left -= chunk_size
        return self.letter * chunk_size


class Worker:
    def __init__(self, queue_host, queue_port, worker_id, max_retries,
                 concurrency):
        self.queue_host = queue_host
        self.queue_port = queue_port

        # Only one greenthread will be pulling out work...
        self.work_queue = beanstalkc.Connection(host=queue_host,
                                                port=queue_port)
        self.work_queue.watch(ssbench.WORK_TUBE)

        # This greenpool is for sending results
        self.result_queue_pool = eventlet.pools.Pool(
            concurrency, concurrency, create=self.create_result_queue)

        self.worker_id = worker_id
        self.max_retries = max_retries
        self.concurrency = concurrency
        # The connection pool is lazily-created when the first work job comes
        # in which will have the storage_url in it.
        self.conn_pool = None

    def create_result_queue(self):
        queue = beanstalkc.Connection(host=self.queue_host,
                                      port=self.queue_port)
        queue.use(ssbench.STATS_TUBE)
        return queue

    def create_conn_pool(self, storage_url):
        self.conn_pool = ConnectionPool(storage_url, self.concurrency)

    @contextmanager
    def connection(self):
        try:
            hc = self.conn_pool.get()
            try:
                yield hc
            except CannotSendRequest:
                logging.info("@connection hit CannotSendRequest...")
                try:
                    hc.close()
                except Exception:
                    pass
                hc = self.conn_pool.create()
        finally:
            self.conn_pool.put(hc)

    def go(self):
        logging.debug('Worker %s starting...', self.worker_id)
        pool = eventlet.GreenPool(self.concurrency)
        job = self.work_queue.reserve()
        while job:
            job.delete()  # avoid any job-timeout nonsense
            job_data = yaml.load(job.body)
            if self.conn_pool is None:
                self.create_conn_pool(job_data['storage_url'])
            logging.debug('%r', job_data)
            pool.spawn_n(self.handle_job, job_data)
            job = self.work_queue.reserve()

    def handle_job(self, job_data):
        # Dispatch type to a handler, if possible
        handler = getattr(self, 'handle_%s' % job_data['type'], None)
        if handler:
            try:
                handler(job_data)
            except Exception as e:
                # If the handler threw an exception, we need to put a "result"
                # anyway so the master can finish by reading the requisite
                # number of results without having to timeout.
                self.put_results(job_data, exception=repr(e))
        else:
            raise NameError("Unknown job type %r" % job_data['type'])

    def ignoring_http_responses(self, statuses, fn, call_info, **extra_keys):
        tries = 0
        args = dict(
            url=call_info['storage_url'],
            token=call_info['token'],
            container=call_info['container'],
            name=call_info['name'],
        )
        args.update(extra_keys)

        while True:
            try:
                fn_results = None
                with self.connection() as conn:
                    fn_results = fn(http_conn=conn, **args)
                if fn_results:
                    break
                tries += 1
                if tries > self.max_retries:
                    raise Exception('No fn_results for %r after %d '
                                    'retries' % (fn, self.max_retries))
            # XXX The name of this method does not suggest that it
            # will also retry on socket-level errors. Regardless,
            # sometimes Swift refuses connections (probably when it's
            # way overloaded and the listen socket's connection queue
            # (in the kernel) is full, so the kernel just says RST).
            except socket.error:
                tries += 1
                if tries > self.max_retries:
                    raise
            except client.ClientException as error:
                tries += 1
                if error.http_status in statuses and \
                        tries <= self.max_retries:
                    logging.debug("Retrying an error: %r", error)
                else:
                    raise
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
        with self.result_queue_pool.item() as queue:
            return queue.put(
                yaml.dump(add_dicts(*args, completed_at=time.time(),
                                    worker_id=self.worker_id, **kwargs)))

    def handle_upload_object(self, object_info, letter='A'):
        results = self.ignoring_http_responses(
            (503,), client.put_object, object_info,
            content_length=object_info['size'],
            contents=ChunkedReader(letter, object_info['size']))
        self.put_results(
            object_info,
            first_byte_latency=results.get(
                'x-swiftstack-first-byte-latency', None),
            last_byte_latency=results.get(
                'x-swiftstack-last-byte-latency', None))

    # By the time a job gets to the worker, an object create and update look
    # the same: it's just a PUT.
    def handle_update_object(self, object_info):
        return self.handle_upload_object(object_info, letter='B')

    def handle_delete_object(self, object_info):
        results = self.ignoring_http_responses(
            (404, 503), client.delete_object, object_info)
        self.put_results(
            object_info,
            first_byte_latency=results.get(
                'x-swiftstack-first-byte-latency', None),
            last_byte_latency=results.get(
                'x-swiftstack-last-byte-latency', None))

    def handle_get_object(self, object_info):
        results = self.ignoring_http_responses(
            (404, 503), client.get_object, object_info,
            resp_chunk_size=2**16)
        # Read (and throw away) all the file contents (chunked)
        for chunk in results[1]:
            pass
        self.put_results(
            object_info,
            first_byte_latency=results[0].get(
                'x-swiftstack-first-byte-latency', None),
            last_byte_latency=results[0].get(
                'x-swiftstack-last-byte-latency', None))
