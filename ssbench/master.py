# Copyright (c) 2012-2015 SwiftStack, Inc.
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

import gevent
import gevent.pool
import gevent.monkey
gevent.monkey.patch_socket()
gevent.monkey.patch_ssl()
gevent.monkey.patch_time()

import os
import re
import sys
import time
import signal
import logging
import msgpack
import zmq.green as zmq

import ssbench
from ssbench.importer import random
import ssbench.swift_client as client
from ssbench.run_state import RunState
from ssbench.util import raise_file_descriptor_limit


def _container_creator(storage_urls, token, container):
    storage_url = random.choice(storage_urls)
    http_conn = client.http_connection(storage_url)
    try:
        client.head_container(storage_url, token, container,
                              http_conn=http_conn)
    except client.ClientException:
        client.put_container(storage_url, token, container,
                             http_conn=http_conn)


def _container_deleter(concurrency, storage_urls, token, container_info):
    container_name = container_info['name']
    logging.info('deleting %r (%d objs)', container_name,
                 container_info['count'])
    storage_url = random.choice(storage_urls)
    http_conn = client.http_connection(storage_url)
    _, obj_list = client.get_container(
        random.choice(storage_urls), token, container_name,
        http_conn=http_conn)

    pool = gevent.pool.Pool(concurrency)
    for obj_name in [o['name'] for o in obj_list]:
        pool.spawn(client.delete_object, random.choice(storage_urls), token,
                   container_name, obj_name)
    pool.join()

    client.delete_container(
        random.choice(storage_urls), token, container_name,
        http_conn=http_conn)


def _gen_cleanup_job(object_info):
    return {
        'type': ssbench.DELETE_OBJECT,
        'container': object_info[0],
        'name': object_info[1],
    }


class Master(object):
    def __init__(self, zmq_bind_ip=None, zmq_work_port=None,
                 zmq_results_port=11300, quiet=False, connect_timeout=None,
                 network_timeout=None):
        if zmq_bind_ip is not None and zmq_work_port is not None:
            work_endpoint = 'tcp://%s:%d' % (zmq_bind_ip, zmq_work_port)
            results_endpoint = 'tcp://%s:%d' % (zmq_bind_ip, zmq_results_port)
            self.context = zmq.Context()
            self.work_push = self.context.socket(zmq.PUSH)
            self.work_push.bind(work_endpoint)
            self.results_pull = self.context.socket(zmq.PULL)
            self.results_pull.bind(results_endpoint)
        self.connect_timeout = connect_timeout
        self.network_timeout = network_timeout
        self.quiet = quiet

    def process_results_to(self, results_raw, processor, label='',
                           run_results=None):
        results = msgpack.loads(results_raw, use_list=False)
        result_count = 0
        for result in results:
            result_count += 1
            logging.debug(
                'RESULT: %13s %s/%-17s %s/%s %s',
                result['type'], result['container'], result['name'],
                '%7.4f' % result.get('first_byte_latency')
                if result.get('first_byte_latency', None) else ' (none)',
                '%7.4f' % result.get('last_byte_latency')
                if result.get('last_byte_latency', None) else '(none) ',
                result.get('trans_id', ''))
            if label and not self.quiet:
                if 'exception' in result:
                    sys.stderr.write('X')
                elif result.get('first_byte_latency', None) is not None:
                    if result['first_byte_latency'] < 1:
                        sys.stderr.write('.')
                    elif result['first_byte_latency'] < 3:
                        sys.stderr.write('o')
                    elif result['first_byte_latency'] < 10:
                        sys.stderr.write('O')
                    else:
                        sys.stderr.write('*')
                else:
                    if result['last_byte_latency'] < 1:
                        sys.stderr.write('_')
                    elif result['last_byte_latency'] < 3:
                        sys.stderr.write('|')
                    elif result['last_byte_latency'] < 10:
                        sys.stderr.write('^')
                    else:
                        sys.stderr.write('@')
                sys.stderr.flush()
            processor(result)

        if run_results:
            run_results.process_raw_results(results_raw)

        return result_count

    def do_a_run(self, concurrency, job_generator, result_processor,
                 auth_kwargs, mapper_fn=None, label='', noop=False,
                 batch_size=1, run_results=None):

        if label and not self.quiet:
            print >>sys.stderr, label + """
  X    work job raised an exception
  .  <  1s first-byte-latency
  o  <  3s first-byte-latency
  O  < 10s first-byte-latency
  * >= 10s first-byte-latency
  _  <  1s last-byte-latency  (CREATE or UPDATE)
  |  <  3s last-byte-latency  (CREATE or UPDATE)
  ^  < 10s last-byte-latency  (CREATE or UPDATE)
  @ >= 10s last-byte-latency  (CREATE or UPDATE)
            """.rstrip()

        def _job_decorator(raw_job):
            if mapper_fn is not None:
                work_job = mapper_fn(raw_job)
                if not work_job:
                    if noop:
                        work_job = raw_job
                        work_job['container'] = 'who_cares'
                        work_job['name'] = 'who_cares'
                    else:
                        logging.warning('Unable to fill in job %r', raw_job)
                        return None
            else:
                work_job = raw_job
            work_job['auth_kwargs'] = auth_kwargs
            work_job['connect_timeout'] = self.connect_timeout
            work_job['network_timeout'] = self.network_timeout
            return work_job

        active = 0
        for raw_job in job_generator:
            work_job = _job_decorator(raw_job)
            if not work_job:
                continue

            send_q = [work_job]

            logging.debug('active: %d\tconcurrency: %d', active, concurrency)
            if active >= concurrency:
                result_jobs_raw = self.results_pull.recv()
                result_count = self.process_results_to(
                    result_jobs_raw, result_processor, label=label,
                    run_results=run_results)
                active -= result_count

            while len(send_q) < min(batch_size, concurrency - active):
                try:
                    work_job = _job_decorator(job_generator.next())
                    if not work_job:
                        continue
                    send_q.append(work_job)
                except StopIteration:
                    break

            self.work_push.send(msgpack.dumps(send_q))
            active += len(send_q)
            # NOTE: we'll never exit this loop with unsent contents in send_q

        # Drain the results
        logging.debug('All jobs sent; awaiting results...')
        while active > 0:
            logging.debug('Draining results: active = %d', active)
            result_jobs_raw = self.results_pull.recv()
            result_count = self.process_results_to(
                result_jobs_raw, result_processor, label=label,
                run_results=run_results)
            active -= result_count
        if label and not self.quiet:
            sys.stderr.write('\n')
            sys.stderr.flush()

    def kill_workers(self, timeout=5):
        """
        Send a suicide message to all workers, with some kind of timeout.
        """
        logging.info('Killing workers, taking up to %d seconds.', int(timeout))
        poller = zmq.Poller()
        poller.register(self.results_pull, zmq.POLLIN)

        while True:
            # Seems to get stuck gevent-blocking in the work_push.send() after
            # all the workers have died.  Also, gevent.Timeout() doesn't seem
            # to work here?!
            signal.alarm(int(timeout))
            self.work_push.send(msgpack.dumps([{'type': 'PING'}]))
            socks = dict(poller.poll(timeout * 1500))
            if self.results_pull in socks \
                    and socks[self.results_pull] == zmq.POLLIN:
                result_packed = self.results_pull.recv()
                result = msgpack.loads(result_packed, use_list=False)
                logging.info('Heard from worker id=%d; sending SUICIDE',
                             result[0]['worker_id'])
                self.work_push.send(msgpack.dumps([{'type': 'SUICIDE'}]))
                gevent.sleep(0.1)
            else:
                break
            signal.alarm(0)

    def cleanup_containers(self, auth_kwargs, container_base, concurrency):
        storage_urls, token = self._authenticate(auth_kwargs)

        _, container_list = client.get_account(
            random.choice(storage_urls), token)

        our_container_re = re.compile('%s_\d+$' % container_base)

        start_time = time.time()
        obj_count = 0
        container_count = 0
        pool = gevent.pool.Pool(concurrency)
        for container_info in container_list:
            # e.g. {'count': 41, 'bytes': 496485, 'name': 'doc'}
            if our_container_re.match(container_info['name']):
                pool.spawn(_container_deleter, concurrency, storage_urls,
                           token, container_info)
                container_count += 1
                obj_count += container_info['count']
            else:
                logging.debug('Ignoring non-ssbench container %r',
                              container_info['name'])
        pool.join()
        delta_t = time.time() - start_time
        logging.info('Deleted %.1f containers/s, %.1f objs/s',
                     container_count / delta_t, obj_count / delta_t)

    def _authenticate(self, auth_kwargs):
        """
        Helper method to turn some auth_kwargs into a set of potential storage
        URLs and a token.
        """
        if auth_kwargs.get('token'):
            logging.debug('Using token %s at one of %r',
                          auth_kwargs['token'], auth_kwargs['storage_urls'])
            return auth_kwargs['storage_urls'], auth_kwargs['token']

        logging.debug('Authenticating to %s with %s/%s',
                      auth_kwargs['auth_url'], auth_kwargs['user'],
                      auth_kwargs['key'])
        storage_url, token = client.get_auth(**auth_kwargs)
        if auth_kwargs['storage_urls']:
            logging.debug('Overriding auth storage url %s with '
                          'one of %r', storage_url,
                          auth_kwargs['storage_urls'])
            return auth_kwargs['storage_urls'], token

        return [storage_url], token

    def run_scenario(self, scenario, auth_kwargs, run_results, noop=False,
                     with_profiling=False, keep_objects=False, batch_size=1):
        """
        Runs a CRUD scenario, given cluster parameters and a Scenario object.

        :param scenario: Scenario object describing the benchmark run
        :param auth_kwargs: All-you-can-eat dictionary of
                            authentication-related arguments.
        :param run_results: RunResults objects for the run
        :param noop: Run in no-op mode?
        :param with_profiing: Profile the run?
        :param keep_objects: Keep uploaded objects instead of deleting them?
        :param batch_size: Send this many bench jobs per packet to workers
        :param returns: Collected result records from workers
        """

        run_state = RunState()

        logging.info(u'Starting scenario run for "%s"', scenario.name)

        raise_file_descriptor_limit()

        # Construct auth_kwargs appropriate for client.get_auth()
        if auth_kwargs.get('token'):
            auth_kwargs = {
                'storage_urls': auth_kwargs['storage_urls'],
                'token': auth_kwargs['token'],
            }

        # Ensure containers exist
        if not noop:
            storage_urls, c_token = self._authenticate(auth_kwargs)

            logging.info('Ensuring %d containers (%s_*) exist; '
                         'concurrency=%d...',
                         len(scenario.containers), scenario.container_base,
                         scenario.container_concurrency)
            pool = gevent.pool.Pool(scenario.container_concurrency)
            for container in scenario.containers:
                pool.spawn(_container_creator, storage_urls, c_token,
                           container)
            pool.join()

        # Enqueue initialization jobs
        if not noop:
            logging.info('Initializing cluster with stock data (up to %d '
                         'concurrent workers)', scenario.user_count)

            self.do_a_run(scenario.user_count, scenario.initial_jobs(),
                          run_state.handle_initialization_result, auth_kwargs,
                          batch_size=batch_size)

        logging.info('Starting benchmark run (up to %d concurrent '
                     'workers)', scenario.user_count)
        if noop:
            logging.info('  (not actually talking to Swift cluster!)')

        if with_profiling:
            import cProfile
            prof = cProfile.Profile()
            prof.enable()
        self.do_a_run(scenario.user_count, scenario.bench_jobs(),
                      run_state.handle_run_result, auth_kwargs,
                      mapper_fn=run_state.fill_in_job,
                      label='Benchmark Run:', noop=noop, batch_size=batch_size,
                      run_results=run_results)
        if with_profiling:
            prof.disable()
            prof_output_path = '/tmp/do_a_run.%d.prof' % os.getpid()
            prof.dump_stats(prof_output_path)
            logging.info('PROFILED main do_a_run to %s', prof_output_path)

        if not noop and not keep_objects:
            logging.info('Deleting population objects from cluster')
            self.do_a_run(scenario.user_count,
                          run_state.cleanup_object_infos(),
                          lambda *_: None,
                          auth_kwargs, mapper_fn=_gen_cleanup_job,
                          batch_size=batch_size)
        elif keep_objects:
            logging.info('NOT deleting any objects due to -k/--keep-objects')
