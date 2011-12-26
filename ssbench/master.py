import yaml

import logging
from statlib import stats
from mako.template import Template

from ssbench.constants import *
from ssbench import swift_client as client

from pprint import pformat

class Master:
    def __init__(self, queue):
        queue.watch(STATS_TUBE)
        queue.ignore('default')
        self.queue = queue

    def generate_scenario_report(self, scenario, stats):
        """Format a report based on calculated statistics for an executed
        scenario.
        
        :stats: A python data structure with calculated statistics
        :returns: A report (string) suitable for printing, emailing, etc.
        """

        template = Template(self.scenario_template())
        tmpl_vars = {
            'crud_pcts': scenario.crud_pcts,
            'stat_list': [
                ('TOTAL', stats['agg_stats'], stats['size_stats']),
                ('CREATE', stats['op_stats'][CREATE_OBJECT],
                 stats['op_stats'][CREATE_OBJECT]['size_stats']),
                ('READ', stats['op_stats'][READ_OBJECT],
                 stats['op_stats'][READ_OBJECT]['size_stats']),
                ('UPDATE', stats['op_stats'][UPDATE_OBJECT],
                 stats['op_stats'][UPDATE_OBJECT]['size_stats']),
                ('DELETE', stats['op_stats'][DELETE_OBJECT],
                 stats['op_stats'][DELETE_OBJECT]['size_stats']),
            ],
            'agg_stats': stats['agg_stats'],
        }
        return template.render(scenario=scenario, stats=stats, **tmpl_vars)

    def calculate_scenario_stats(self, results):
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
                            1: { # keys are file size byte-counts
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
                    1: { # keys are file size byte-counts
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
        #   'object_size': 4900000,
        #   'first_byte_latency': 0.9137639999389648,
        #   'last_byte_latency': 0.913769006729126,
        #   'completed_at': 1324372892.360802,
        #}
        agg_stats = dict(start=2**32, stop=0, req_count=0)
        op_stats = {}
        for crud_type in [CREATE_OBJECT, READ_OBJECT, UPDATE_OBJECT, DELETE_OBJECT]:
            op_stats[crud_type] = dict(
                req_count=0, avg_req_per_sec=0, size_stats = {},
            )

        req_completion_seconds = {}
        completion_time_max = 0
        completion_time_min = 2**32
        stats = dict(
            agg_stats = agg_stats,
            worker_stats = {},
            op_stats = op_stats,
            size_stats = {},
        )
        for result in results:
            completion_time = int(result['completed_at'])
            if completion_time < completion_time_min:
                completion_time_min = completion_time
            if completion_time > completion_time_max:
                completion_time_max = completion_time
            req_completion_seconds[completion_time] = \
                1 + req_completion_seconds.get(completion_time, 0)
            result['start'] = result['completed_at'] - result['last_byte_latency']

            # Stats per-worker
            if not stats['worker_stats'].has_key(result['worker_id']):
                stats['worker_stats'][result['worker_id']] = {}
            self._add_result_to(stats['worker_stats'][result['worker_id']], result)

            # Stats per-file-size
            if not stats['size_stats'].has_key(result['object_size']):
                stats['size_stats'][result['object_size']] = {}
            self._add_result_to(stats['size_stats'][result['object_size']], result)

            self._add_result_to(agg_stats, result)
            self._add_result_to(op_stats[result['type']], result)

            # Stats per-operation-per-file-size
            if not op_stats[result['type']]['size_stats'].has_key(result['object_size']):
                op_stats[result['type']]['size_stats'][result['object_size']] = {}
            self._add_result_to(op_stats[result['type']]['size_stats'][result['object_size']], result)
        agg_stats['worker_count'] = len(stats['worker_stats'].keys())
        self._compute_req_per_sec(agg_stats)
        self._compute_latency_stats(agg_stats)
        for worker_stats in stats['worker_stats'].values():
            self._compute_req_per_sec(worker_stats)
            self._compute_latency_stats(worker_stats)
        for op_stats_dict in op_stats.values():
            if op_stats_dict['req_count']:
                self._compute_req_per_sec(op_stats_dict)
                self._compute_latency_stats(op_stats_dict)
                for size_stats in op_stats_dict['size_stats'].values():
                    self._compute_req_per_sec(size_stats)
                    self._compute_latency_stats(size_stats)
        for size_stats in stats['size_stats'].values():
            self._compute_req_per_sec(size_stats)
            self._compute_latency_stats(size_stats)
        time_series_data = [
            req_completion_seconds.get(t, 0) for t in range(completion_time_min, completion_time_max + 1)
        ]
        stats['time_series'] = dict(start=completion_time_min,
                                    data=time_series_data)

        return stats

    def _compute_latency_stats(self, stat_dict):
        for latency_type in ('first_byte_latency', 'last_byte_latency'):
            stat_dict[latency_type] = self._series_stats(stat_dict[latency_type])
 
    def _compute_req_per_sec(self, stat_dict):
        stat_dict['avg_req_per_sec'] = round(stat_dict['req_count'] /
                                             (stat_dict['stop'] -
                                              stat_dict['start']), 6)


    def _add_result_to(self, stat_dict, result):
        if not stat_dict.has_key('start') or result['start'] < stat_dict['start']:
            stat_dict['start'] = result['start']
        if not stat_dict.has_key('stop') or result['completed_at'] > stat_dict['stop']:
            stat_dict['stop'] = result['completed_at']
        if not stat_dict.has_key('req_count'):
            stat_dict['req_count'] = 1
        else:
            stat_dict['req_count'] += 1
        self._rec_latency(stat_dict, result)

    def _series_stats(self, sequence):
        try:
            n, (minval, maxval), mean, std_dev, skew, kurtosis = stats.ldescribe(sequence)
        except ZeroDivisionError:
            # Handle the case of a single-element sequence (sample standard
            # deviation divides by N-1)
            minval=sequence[0]
            maxval=sequence[0]
            mean=sequence[0]
            std_dev=0
        return dict(
            min=round(minval, 6), max=round(maxval, 6), avg=round(mean, 6),
            std_dev=round(stats.lsamplestdev(sequence), 6),
            median=round(stats.lmedianscore(sequence), 6),
        )

    def _rec_latency(self, stats_dict, result):
        for latency_type in ('first_byte_latency', 'last_byte_latency'):
            if stats_dict.has_key(latency_type):
                stats_dict[latency_type].append(result[latency_type])
            else:
                stats_dict[latency_type] = [result[latency_type]]

    def run_scenario(self, auth_url, user, key, scenario):
        """Runs a CRUD scenario, given cluter parameters and a Scenario object.
        
        :auth_url: Authentication URL for the Swift cluster
        :user: Account/Username to use (format is <account>:<username>)
        :key: Password for the Account/Username
        :scenario: Scenario object describing the benchmark run
        :returns: Collected result records from workers
        """
    
        self.drain_stats_queue()
        url, token = client.get_auth(auth_url, user, key)

        logging.info('Starting scenario run for %r', scenario.name)
        # Ensure containers exist
        logging.info('Making sure benchmark containers exist...')
        for container in ['Picture', 'Audio', 'Document', 'Video',
                          'Application']:
            if not self.container_exists(url, token, container):
                logging.info('  creating container %r', container)
                self.create_container(url, token, container)

        self.queue.use('work_%04d' % scenario.user_count)

        # Enqueue initialization jobs
        logging.info('Initializing cluster with stock data (%d concurrent workers)',
                     scenario.user_count)
        initial_jobs = scenario.initial_jobs()
        for initial_job in initial_jobs:
            initial_job.update(url=url, token=token)
            self.queue.put(yaml.dump(initial_job), priority=PRIORITY_SETUP)

        # Wait for them to all finish
        results = self.gather_results(len(initial_jobs), timeout=600)

        # Enqueue bench jobs
        logging.info('Starting benchmark run (%d concurrent workers)',
                     scenario.user_count)
        bench_jobs = scenario.bench_jobs()
        for bench_job in bench_jobs:
            bench_job.update(url=url, token=token)
            self.queue.put(yaml.dump(bench_job), priority=PRIORITY_WORK)

        # Wait for them to all finish and return the results
        results = self.gather_results(len(bench_jobs), timeout=600)
        return results

    def bench_container_creation(self, auth_url, user, key, count):
        self.drain_stats_queue()
        url, token = client.get_auth(auth_url, user, key)

        for i in range(count):
            job = {
                "type": CREATE_CONTAINER,
                "url":  url,
                "token": token,
                "container": self.container_name(i),
                }
            self.queue.put(yaml.dump(job), priority=PRIORITY_WORK)

        results = self.gather_results(count)

        for i in range(count):
            job = {
                "type": DELETE_CONTAINER,
                "url":  url,
                "token": token,
                "container": self.container_name(i),
                }
            self.queue.put(yaml.dump(job), priority=PRIORITY_CLEANUP)

        return results

    def bench_object_creation(self, auth_url, user, key, containers, size, object_count):
        self.drain_stats_queue()
        url, token = client.get_auth(auth_url, user, key)

        for c in containers:
            if not self.container_exists(url, token, c):
                self.create_container(url, token, c)

        for i in range(object_count):
            job = {
                "type": CREATE_OBJECT,
                "url":  url,
                "token": token,
                "container": containers[i % len(containers)],
                "object_name": self.object_name(i),
                "object_size": size,
                }

            self.queue.put(yaml.dump(job), priority=PRIORITY_WORK)

        results = self.gather_results(object_count)

        for i in range(object_count):
            job = {
                "type": DELETE_OBJECT,
                "url":  url,
                "token": token,
                "container": containers[i % len(containers)],
                "object_name": self.object_name(i),
                }

            self.queue.put(yaml.dump(job), priority=PRIORITY_CLEANUP)

        return results

    def drain_stats_queue(self):
        self.gather_results(count=0,   # no limit
                            timeout=0) # no waiting

    def gather_results(self, count=0, timeout=15):
        results = []
        job = self.queue.reserve(timeout=timeout)
        while job:
            job.delete()
            results.append(yaml.load(job.body))
            if (count <= 0 or len(results) < count):
                job = self.queue.reserve(timeout=timeout)
            else:
                job = None
        return results

    def container_exists(self, url, token, container):
        try:
            client.head_container(url, token, container)
            return True
        except client.ClientException:
            return False

    def create_container(self, url, token, container):
        client.put_container(url, token, container)

    def container_name(self, index):
        return "ssbench-container%d" % (index,)

    def object_name(self, index):
        return "ssbench-obj%d" % (index,)

    def scenario_template(self):
        return """
${scenario.name}
  C   R   U   D     Worker count: ${'%3d' % agg_stats['worker_count']}
%% ${'%02.0f  %02.0f  %02.0f  %02.0f' % (crud_pcts[0], crud_pcts[1], crud_pcts[2], crud_pcts[3])}
% for label, stats, sstats in stat_list:

${label}
       Count: ${'%5d' % stats['req_count']}  Average requests per second: ${'%5.1f' % stats['avg_req_per_sec']}
                           min     max    avg    std_dev  median
       First-byte latency: ${'%5.2f' % stats['first_byte_latency']['min']} - ${'%5.2f' % stats['first_byte_latency']['max']}  ${'%5.2f' % stats['first_byte_latency']['avg']}  (${'%5.2f' % stats['first_byte_latency']['std_dev']})  ${'%5.2f' % stats['first_byte_latency']['median']}
       Last-byte  latency: ${'%5.2f' % stats['last_byte_latency']['min']} - ${'%5.2f' % stats['last_byte_latency']['max']}  ${'%5.2f' % stats['last_byte_latency']['avg']}  (${'%5.2f' % stats['last_byte_latency']['std_dev']})  ${'%5.2f' % stats['last_byte_latency']['median']}
% endfor


"""
