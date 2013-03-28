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
gevent.monkey.patch_time()

import os
import sys
import math
import signal
import logging
import msgpack
import resource
import statlib.stats
from gevent_zeromq import zmq
from datetime import datetime
from mako.template import Template

import ssbench
import ssbench.swift_client as client
from ssbench.run_state import RunState
from ssbench.ordered_dict import OrderedDict

from pprint import pprint, pformat


REPORT_TIME_FORMAT = '%F %T UTC'


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

    def process_results_to(self, results, processor, label=''):
        for result in results:
            logging.debug('RESULT: %13s %s/%-17s %s/%s %s',
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

    def do_a_run(self, concurrency, job_generator, result_processor,
                 auth_kwargs, mapper_fn=None, label='', noop=False,
                 batch_size=1):
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
                result_jobs = msgpack.loads(result_jobs_raw)
                self.process_results_to(result_jobs, result_processor,
                                        label=label)
                active -= len(result_jobs)

            while len(send_q) < min(batch_size, concurrency - active):
                try:
                    work_job = _job_decorator(job_generator.next())
                    send_q.append(work_job)
                except StopIteration:
                    break

            logging.debug('len(send_q): %d', len(send_q))
            self.work_push.send(msgpack.dumps(send_q))
            active += len(send_q)

        # Drain the results
        logging.debug('All jobs sent; awaiting results...')
        while active > 0:
            logging.debug('Draining results: active = %d', active)
            result_jobs_raw = self.results_pull.recv()
            result_jobs = msgpack.loads(result_jobs_raw)
            self.process_results_to(result_jobs, result_processor,
                                    label=label)
            active -= len(result_jobs)
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
                result = msgpack.loads(result_packed)
                logging.info('Heard from worker id=%d; sending SUICIDE',
                            result['worker_id'])
                self.work_push.send(msgpack.dumps([{'type': 'SUICIDE'}]))
                gevent.sleep(0.1)
            else:
                break
            signal.alarm(0)

    def run_scenario(self, scenario, auth_url, user, key, auth_version,
                     os_options, cacert, insecure, storage_url, token,
                     noop=False, with_profiling=False, keep_objects=False,
                     batch_size=1):
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
                      label='Benchmark Run:', noop=noop, batch_size=batch_size)
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

        return run_state.run_results

    def write_rps_histogram(self, stats, csv_file):
        csv_file.write('"Seconds Since Start","Requests Completed"\n')
        for i, req_count in enumerate(stats['time_series']['data'], 1):
            csv_file.write('%d,%d\n' % (i, req_count))

    def scenario_template(self):
        return """
${scenario.name}
Worker count: ${'%3d' % agg_stats['worker_count']}   Concurrency: ${'%3d' % scenario.user_count}  Ran ${start_time} to ${stop_time} (${'%.0f' % round(duration)}s)

%% Ops    C   R   U   D       Size Range       Size Name
% for size_datum in size_data:
${size_datum['pct_total_ops']}   % ${size_datum['crud_pcts']}      ${size_datum['size_range']}  ${size_datum['size_name']}
% endfor
---------------------------------------------------------------------
        ${'%3.0f' % weighted_c} ${'%3.0f' % weighted_r} ${'%3.0f' % weighted_u} ${'%3.0f' % weighted_d}      CRUD weighted average

% for label, stats, sstats in stat_list:
% if stats['req_count']:
${label}
       Count: ${'%5d' % stats['req_count']}  Average requests per second: ${'%5.1f' % stats['avg_req_per_sec']}
                            min       max      avg      std_dev  ${'%02d' % nth_pctile}%-ile  ${'%15s' % ''}  Worst latency TX ID
       First-byte latency: ${stats['first_byte_latency']['min']} - ${stats['first_byte_latency']['max']}  ${stats['first_byte_latency']['avg']}  (${stats['first_byte_latency']['std_dev']})  ${stats['first_byte_latency']['pctile']}  (all obj sizes)  ${stats['worst_first_byte_latency'][1] if 'worst_first_byte_latency' in stats else ''}
       Last-byte  latency: ${stats['last_byte_latency']['min']} - ${stats['last_byte_latency']['max']}  ${stats['last_byte_latency']['avg']}  (${stats['last_byte_latency']['std_dev']})  ${stats['last_byte_latency']['pctile']}  (all obj sizes)  ${stats['worst_last_byte_latency'][1] if 'worst_last_byte_latency' in stats else ''}
% for size_str, per_size_stats in sstats.iteritems():
% if per_size_stats:
       First-byte latency: ${per_size_stats['first_byte_latency']['min']} - ${per_size_stats['first_byte_latency']['max']}  ${per_size_stats['first_byte_latency']['avg']}  (${per_size_stats['first_byte_latency']['std_dev']})  ${per_size_stats['first_byte_latency']['pctile']}  ${'(%8s objs)' % size_str}  ${per_size_stats['worst_first_byte_latency'][1] if 'worst_first_byte_latency' in per_size_stats else ''}
       Last-byte  latency: ${per_size_stats['last_byte_latency']['min']} - ${per_size_stats['last_byte_latency']['max']}  ${per_size_stats['last_byte_latency']['avg']}  (${per_size_stats['last_byte_latency']['std_dev']})  ${per_size_stats['last_byte_latency']['pctile']}  ${'(%8s objs)' % size_str}  ${per_size_stats['worst_last_byte_latency'][1] if 'worst_last_byte_latency' in per_size_stats else ''}
% endif
% endfor

% endif
% endfor
"""

    def generate_scenario_report(self, scenario, stats):
        """Format a report based on calculated statistics for an executed
        scenario.

        :stats: A python data structure with calculated statistics
        :returns: A report (string) suitable for printing, emailing, etc.
        """

        template = Template(self.scenario_template())
        tmpl_vars = {
            'size_data': [],
            'stat_list': [
                ('TOTAL', stats['agg_stats'], stats['size_stats']),
                ('CREATE', stats['op_stats'][ssbench.CREATE_OBJECT],
                 stats['op_stats'][ssbench.CREATE_OBJECT]['size_stats']),
                ('READ', stats['op_stats'][ssbench.READ_OBJECT],
                 stats['op_stats'][ssbench.READ_OBJECT]['size_stats']),
                ('UPDATE', stats['op_stats'][ssbench.UPDATE_OBJECT],
                 stats['op_stats'][ssbench.UPDATE_OBJECT]['size_stats']),
                ('DELETE', stats['op_stats'][ssbench.DELETE_OBJECT],
                 stats['op_stats'][ssbench.DELETE_OBJECT]['size_stats']),
            ],
            'agg_stats': stats['agg_stats'],
            'nth_pctile': stats['nth_pctile'],
            'start_time': datetime.utcfromtimestamp(
                stats['time_series']['start_time']
            ).strftime(REPORT_TIME_FORMAT),
            'stop_time': datetime.utcfromtimestamp(
                stats['time_series']['stop']).strftime(REPORT_TIME_FORMAT),
            'duration': stats['time_series']['stop']
            - stats['time_series']['start_time'],
            'weighted_c': 0.0,
            'weighted_r': 0.0,
            'weighted_u': 0.0,
            'weighted_d': 0.0,
        }
        for size_data in scenario.sizes_by_name.values():
            if size_data['size_min'] == size_data['size_max']:
                size_range = '%-15s' % (
                    self._format_bytes(size_data['size_min']),)
            else:
                size_range = '%s - %s' % (
                    self._format_bytes(size_data['size_min']),
                    self._format_bytes(size_data['size_max']))
            initial_files = scenario._scenario_data['initial_files']
            initial_total = sum(initial_files.values())
            pct_total = (initial_files.get(size_data['name'], 0)
                         / float(initial_total) * 100.0)
            tmpl_vars['size_data'].append({
                'crud_pcts': '  '.join(map(lambda p: '%2.0f' % p,
                                           size_data['crud_pcts'])),
                'size_range': size_range,
                'size_name': size_data['name'],
                'pct_total_ops': '%3.0f%%' % pct_total,
            })
            tmpl_vars['weighted_c'] += pct_total * size_data['crud_pcts'][0] / 100.0
            tmpl_vars['weighted_r'] += pct_total * size_data['crud_pcts'][1] / 100.0
            tmpl_vars['weighted_u'] += pct_total * size_data['crud_pcts'][2] / 100.0
            tmpl_vars['weighted_d'] += pct_total * size_data['crud_pcts'][3] / 100.0
        return template.render(scenario=scenario, stats=stats, **tmpl_vars)

    def _format_bytes(self, byte_count):
        units = [' B', 'kB', 'MB', 'GB']
        i = 0
        while round(byte_count / 1000.0, 3) >= 1.0:
            byte_count = byte_count / 1000.0
            i += 1
        return '%3.0f %s' % (round(byte_count), units[i])

    def calculate_scenario_stats(self, scenario, results, nth_pctile=95):
        """Given a list of worker job result dicts, compute various statistics.

        :results: A list of worker job result dicts
        :returns: A stats python dict which looks like:
            SERIES_STATS = {
                'min': 1.1,
                'max': 1.1,
                'avg': 1.1,
                'std_dev': 1.1,
                'median': 1.1,
            }
            {
                'agg_stats': {
                    'worker_count': 1,
                    'start': 1.1,
                    'stop': 1.1,
                    'req_count': 1,
                    'avg_req_per_sec': 1.1, # req_count / (stop - start)?
                    'first_byte_latency': SERIES_STATS,
                    'last_byte_latency': SERIES_STATS,
                },
                'worker_stats': {
                    1: {  # keys are worker_ids
                        'start': 1.1,
                        'stop': 1.1,
                        'req_count': 1,
                        'avg_req_per_sec': 1.1, # req_count / (stop - start)?
                        'first_byte_latency': SERIES_STATS,
                        'last_byte_latency': SERIES_STATS,
                    },
                    # ...
                },
                'op_stats': {
                    CREATE_OBJECT: { # keys are CRUD constants: CREATE_OBJECT, READ_OBJECT, etc.
                        'req_count': 1, # num requests of this CRUD type
                        'avg_req_per_sec': 1.1, # total_requests / sum(last_byte_latencies)
                        'first_byte_latency': SERIES_STATS,
                        'last_byte_latency': SERIES_STATS,
                        'size_stats': {
                            'small': { # keys are size_str values
                                'req_count': 1, # num requests of this type and size
                                'avg_req_per_sec': 1.1, # total_requests / sum(last_byte_latencies)
                                'first_byte_latency': SERIES_STATS,
                                'last_byte_latency': SERIES_STATS,
                            },
                            # ...
                        },
                    },
                    # ...
                },
                'size_stats': {
                    'small': { # keys are size_str values
                        'req_count': 1, # num requests of this size (for all CRUD types)
                        'avg_req_per_sec': 1.1, # total_requests / sum(last_byte_latencies)
                        'first_byte_latency': SERIES_STATS,
                        'last_byte_latency': SERIES_STATS,
                    },
                    # ...
                },
                'time_series': {
                    'start': 1, # epoch time of first data point
                    'data': [
                        1, # number of requests finishing during this second
                        # ...
                    ],
                },
            }
        """
        # Each result looks like:
        # {
        #   'worker_id': 1,
        #   'type': 'get_object',
        #   'size': 4900000,
        #   'first_byte_latency': 0.9137639999389648,
        #   'last_byte_latency': 0.913769006729126,
        #   'completed_at': 1324372892.360802,
        #}
        # OR
        # {
        #   'worker_id': 1,
        #   'type': 'get_object',
        #   'completed_at': 1324372892.360802,
        #   'exception': '...',
        # }
        logging.info('Calculating statistics for %d result items...',
                     len(results))
        agg_stats = dict(start=2 ** 32, stop=0, req_count=0)
        op_stats = {}
        for crud_type in [ssbench.CREATE_OBJECT, ssbench.READ_OBJECT,
                          ssbench.UPDATE_OBJECT, ssbench.DELETE_OBJECT]:
            op_stats[crud_type] = dict(
                req_count=0, avg_req_per_sec=0,
                size_stats=OrderedDict.fromkeys(scenario.sizes_by_name.keys()))

        req_completion_seconds = {}
        start_time = 0
        completion_time_max = 0
        completion_time_min = 2 ** 32
        stats = dict(
            nth_pctile=nth_pctile,
            agg_stats=agg_stats,
            worker_stats={},
            op_stats=op_stats,
            size_stats=OrderedDict.fromkeys(scenario.sizes_by_name.keys()))
        for result in results:
            if 'exception' in result:
                # skip but log exceptions
                logging.warn('calculate_scenario_stats: exception from '
                             'worker %d: %s',
                             result['worker_id'], result['exception'])
                logging.info(result['traceback'])
                continue
            completion_time = int(result['completed_at'])
            if completion_time < completion_time_min:
                completion_time_min = completion_time
                start_time = completion_time - result['last_byte_latency']
            if completion_time > completion_time_max:
                completion_time_max = completion_time
            req_completion_seconds[completion_time] = \
                1 + req_completion_seconds.get(completion_time, 0)
            result['start'] = (
                result['completed_at'] - result['last_byte_latency'])

            # Stats per-worker
            if result['worker_id'] not in stats['worker_stats']:
                stats['worker_stats'][result['worker_id']] = {}
            self._add_result_to(stats['worker_stats'][result['worker_id']],
                                result)

            # Stats per-file-size
            if not stats['size_stats'][result['size_str']]:
                stats['size_stats'][result['size_str']] = {}
            self._add_result_to(stats['size_stats'][result['size_str']],
                                result)

            self._add_result_to(agg_stats, result)
            self._add_result_to(op_stats[result['type']], result)

            # Stats per-operation-per-file-size
            if not op_stats[result['type']]['size_stats'][result['size_str']]:
                op_stats[result['type']]['size_stats'][result['size_str']] = {}
            self._add_result_to(
                op_stats[result['type']]['size_stats'][result['size_str']],
                result)
        agg_stats['worker_count'] = len(stats['worker_stats'].keys())
        self._compute_req_per_sec(agg_stats)
        self._compute_latency_stats(agg_stats, nth_pctile)
        for worker_stats in stats['worker_stats'].values():
            self._compute_req_per_sec(worker_stats)
            self._compute_latency_stats(worker_stats, nth_pctile)
        for op_stat, op_stats_dict in op_stats.iteritems():
            if op_stats_dict['req_count']:
                self._compute_req_per_sec(op_stats_dict)
                self._compute_latency_stats(op_stats_dict, nth_pctile)
                for size_str, size_stats in \
                        op_stats_dict['size_stats'].iteritems():
                    if size_stats:
                        self._compute_req_per_sec(size_stats)
                        self._compute_latency_stats(size_stats, nth_pctile)
                    else:
                        op_stats_dict['size_stats'].pop(size_str)
        for size_str, size_stats in stats['size_stats'].iteritems():
            if size_stats:
                self._compute_req_per_sec(size_stats)
                self._compute_latency_stats(size_stats, nth_pctile)
            else:
                stats['size_stats'].pop(size_str)
        time_series_data = [req_completion_seconds.get(t, 0)
                            for t in range(completion_time_min,
                                           completion_time_max + 1)]
        stats['time_series'] = dict(start=completion_time_min,
                                    start_time=start_time,
                                    stop=completion_time_max,
                                    data=time_series_data)

        return stats

    def _compute_latency_stats(self, stat_dict, nth_pctile):
        try:
            for latency_type in ('first_byte_latency', 'last_byte_latency'):
                stat_dict[latency_type] = self._series_stats(
                    stat_dict[latency_type], nth_pctile)
        except KeyError:
            logging.exception('stat_dict: %r', stat_dict)
            raise

    def _compute_req_per_sec(self, stat_dict):
        stat_dict['avg_req_per_sec'] = round(stat_dict['req_count'] /
                                             (stat_dict['stop'] -
                                              stat_dict['start']), 6)

    def _add_result_to(self, stat_dict, result):
        if 'start' not in stat_dict or result['start'] < stat_dict['start']:
            stat_dict['start'] = result['start']
        if 'stop' not in stat_dict or \
                result['completed_at'] > stat_dict['stop']:
            stat_dict['stop'] = result['completed_at']
        if 'req_count' not in stat_dict:
            stat_dict['req_count'] = 1
        else:
            stat_dict['req_count'] += 1
        self._rec_latency(stat_dict, result)

    def _series_stats(self, sequence, nth_pctile):
        pre_filter_count = len(sequence)
        sequence = filter(None, sequence)
        logging.debug('_series_stats pre/post seq len: %d/%d',
                      pre_filter_count, len(sequence))
        if not sequence:
            # No data available
            return dict(min=' N/A  ', max='  N/A  ', avg='  N/A  ',
                        pctile='  N/A  ', std_dev='  N/A  ', median='  N/A  ')
        sequence.sort()
        try:
            n, (minval, maxval), mean, std_dev, skew, kurtosis = \
                statlib.stats.ldescribe(sequence)
        except ZeroDivisionError:
            # Handle the case of a single-element sequence (population standard
            # deviation divides by N-1)
            minval = sequence[0]
            maxval = sequence[0]
            mean = sequence[0]
            std_dev = 0
        return dict(
            min='%6.3f' % minval,
            max='%7.3f' % maxval,
            avg='%7.3f' % mean,
            pctile='%7.3f' % self.pctile(sequence, nth_pctile),
            std_dev='%7.3f' % statlib.stats.lsamplestdev(sequence),
            median='%7.3f' % statlib.stats.lmedianscore(sequence))

    def pctile(self, sequence, nth_pctile):
        seq_len = len(sequence)
        rank = seq_len * nth_pctile / 100.0
        if float(int(rank)) == rank:
            # integer rank means we interpolate between two values
            rank = int(rank)
            return (sequence[rank - 1] + sequence[rank]) / 2.0
        else:
            return sequence[int(math.ceil(rank)) - 1]

    def _rec_latency(self, stats_dict, result):
        for latency_type in ('first_byte_latency', 'last_byte_latency'):
            if latency_type in stats_dict:
                stats_dict[latency_type].append(result[latency_type])
            else:
                stats_dict[latency_type] = [result[latency_type]]
            if result[latency_type] is not None:
                worst_key = 'worst_%s' % latency_type
                if worst_key not in stats_dict \
                        or result[latency_type] > stats_dict[worst_key][0]:
                    stats_dict[worst_key] = (round(result[latency_type], 6),
                                             result['trans_id'])
