# Copyright (c) 2012-2013 SwiftStack, Inc.
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
import sys
import signal
import logging
import msgpack
import resource
from gevent_zeromq import zmq

import ssbench
import ssbench.swift_client as client
from ssbench.run_state import RunState


def _container_creator(storage_url, token, container):
    http_conn = client.http_connection(storage_url)
    try:
        client.head_container(storage_url, token, container,
                              http_conn=http_conn)
    except client.ClientException:
        client.put_container(storage_url, token, container,
                             http_conn=http_conn)


def _gen_cleanup_job(object_info):
    return {
        'type': ssbench.DELETE_OBJECT,
        'container': object_info[0],
        'name': object_info[1],
    }


class Master:
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
                logging.warning('Unable to fill in job %r', raw_job)
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
                    send_q.append(work_job)
                except StopIteration:
                    break

            self.work_push.send(msgpack.dumps(send_q))
            active += len(send_q)

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

    def run_scenario(self, scenario, auth_url, user, key, auth_version,
                     os_options, cacert, insecure, storage_url, token,
                     run_results, noop=False, with_profiling=False,
                     keep_objects=False, batch_size=1):
        """
        Runs a CRUD scenario, given cluster parameters and a Scenario object.

        :param scenario: Scenario object describing the benchmark run
        :param auth_url: Authentication URL for the Swift cluster
        :param user: Account/Username to use (format is <account>:<username>)
        :param key: Password for the Account/Username
        :param auth_version: OpenStack auth version, default is 1.0
        :param os_options: The OpenStack options which can have tenant_id,
                           auth_token, service_type, endpoint_type,
                           tenant_name, object_storage_url, region_name
        :param insecure: Allow to access insecure keystone server.
                         The keystone's certificate will not be verified.
        :param cacert: Bundle file to use in verifying SSL.
        :param storage_url: Optional user-specified x-storage-url
        :param token: Optional user-specified x-auth-token
        :param run_results: RunResults objects for the run
        :param noop: Run in no-op mode?
        :param with_profiing: Profile the run?
        :param keep_objects: Keep uploaded objects instead of deleting them?
        :param batch_size: Send this many bench jobs per packet to workers
        :param returns: Collected result records from workers
        """

        run_state = RunState()

        logging.info(u'Starting scenario run for "%s"', scenario.name)

        soft_nofile, hard_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
        nofile_target = 1024
        if os.geteuid() == 0:
            nofile_target = max(nofile_target, scenario.user_count + 20)
            hard_nofile = nofile_target
        resource.setrlimit(resource.RLIMIT_NOFILE, (nofile_target,
                                                    hard_nofile))

        # Construct auth_kwargs appropriate for client.get_auth()
        if not token:
            auth_kwargs = dict(
                auth_url=auth_url, user=user, key=key,
                auth_version=auth_version, os_options=os_options,
                cacert=cacert, insecure=insecure, storage_url=storage_url)
        else:
            auth_kwargs = dict(storage_url=storage_url, token=token)

        # Ensure containers exist
        if not noop:
            if not token:
                logging.debug('Authenticating to %s with %s/%s', auth_url,
                              user, key)
                c_storage_url, c_token = client.get_auth(**auth_kwargs)
                if storage_url:
                    logging.debug('Overriding auth storage url %s with %s',
                                  c_storage_url, storage_url)
                    c_storage_url = storage_url
            else:
                c_storage_url, c_token = storage_url, token
                logging.debug('Using token %s at %s', c_token, c_storage_url)

            logging.info('Ensuring %d containers (%s_*) exist; '
                         'concurrency=%d...',
                         len(scenario.containers), scenario.container_base,
                         scenario.container_concurrency)
            pool = gevent.pool.Pool(scenario.container_concurrency)
            for container in scenario.containers:
                pool.spawn(_container_creator, c_storage_url, c_token,
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
