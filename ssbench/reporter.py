#
#Copyright (c) 2012-2021, NVIDIA CORPORATION.
#SPDX-License-Identifier: Apache-2.0

import csv
import math
import logging
from pprint import pformat
from datetime import datetime
from cStringIO import StringIO
from mako.template import Template

import ssbench
import ssbench.util
from ssbench.ordered_dict import OrderedDict


REPORT_TIME_FORMAT = '%F %T UTC'


if hasattr(csv.DictWriter, "writeheader"):
    DictWriter = csv.DictWriter
else:
    class DictWriter(csv.DictWriter):

        def writeheader(self):
            header = dict(zip(self.fieldnames, self.fieldnames))
            self.writerow(header)


class Reporter(object):
    def __init__(self, run_results):
        self.run_results = run_results

    def read_results(self, nth_pctile=95, format_numbers=True):
        self.scenario, self.unpacker = self.run_results.read_results()
        self.stats = self.calculate_scenario_stats(nth_pctile, format_numbers)

    def write_rps_histogram(self, target_file):
        target_file.write('"Seconds Since Start","Requests Completed"\n')
        for i, req_count in enumerate(self.stats['time_series']['data'], 1):
            target_file.write('%d,%d\n' % (i, req_count))

    def scenario_template(self):
        return """
${scenario.name}  (generated with ssbench version ${scenario.version})
Worker count: ${'%3d' % agg_stats['worker_count']}   Concurrency: ${'%3d' % scenario.user_count}  Ran ${start_time} to ${stop_time} (${'%.0f' % round(duration)}s)
Object expiration (X-Delete-After): ${scenario.delete_after} (sec)

%% Ops    C   R   U   D       Size Range       Size Name
% for size_datum in size_data:
${size_datum['pct_total_ops']}   % ${size_datum['crud_pcts']}      ${size_datum['size_range']}  ${size_datum['size_name']}
% endfor
---------------------------------------------------------------------
        ${'%3.0f' % weighted_c} ${'%3.0f' % weighted_r} ${'%3.0f' % weighted_u} ${'%3.0f' % weighted_d}      CRUD weighted average

% for label, stats, sstats in stat_list:
% if stats['req_count']:
${label}
       Count: ${'%5d' % stats['req_count']} (${'%5d' % stats['errors']} error; ${'%5d' % stats['retries']} retries: ${'%5.2f' % stats['retry_rate']}%)  Average requests per second: ${'%5.1f' % stats['avg_req_per_sec']}
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
Distribution of requests per worker-ID: ${jobs_per_worker_stats['min']} - ${jobs_per_worker_stats['max']} (avg: ${jobs_per_worker_stats['avg']}; stddev: ${jobs_per_worker_stats['std_dev']})
"""

    def generate_default_report(self, output_csv=False):
        """Format a default summary report based on calculated statistics for
        an executed scenario.

        :returns: A report (string) suitable for printing, emailing, etc.
        """

        stats = self.stats
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
            'jobs_per_worker_stats': stats['jobs_per_worker_stats'],
            'weighted_c': 0.0,
            'weighted_r': 0.0,
            'weighted_u': 0.0,
            'weighted_d': 0.0,
        }
        for size_data in self.scenario.sizes_by_name.values():
            if size_data['size_min'] == size_data['size_max']:
                size_range = '%-15s' % (
                    self._format_bytes(size_data['size_min']),)
            else:
                size_range = '%s - %s' % (
                    self._format_bytes(size_data['size_min']),
                    self._format_bytes(size_data['size_max']))
            initial_files = self.scenario._scenario_data['initial_files']
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
            tmpl_vars['weighted_c'] += \
                pct_total * size_data['crud_pcts'][0] / 100.0
            tmpl_vars['weighted_r'] += \
                pct_total * size_data['crud_pcts'][1] / 100.0
            tmpl_vars['weighted_u'] += \
                pct_total * size_data['crud_pcts'][2] / 100.0
            tmpl_vars['weighted_d'] += \
                pct_total * size_data['crud_pcts'][3] / 100.0
        if output_csv:
            csv_fields = [
                'scenario_name', 'ssbench_version', 'worker_count',
                'concurrency', 'start_time', 'stop_time', 'duration',
                'delete_after']
            csv_data = {
                'scenario_name': self.scenario.name,
                'ssbench_version': self.scenario.version,
                'worker_count': tmpl_vars['agg_stats']['worker_count'],
                'concurrency': self.scenario.user_count,
                'start_time': tmpl_vars['start_time'],
                'stop_time': tmpl_vars['stop_time'],
                'duration': tmpl_vars['duration'],
                'delete_after': str(self.scenario.delete_after),
            }
            for label, stats, sstats in tmpl_vars['stat_list']:
                label_lc = label.lower()
                if stats.get('req_count', 0):
                    self._add_csv_kv(csv_fields, csv_data,
                                     '%s_count' % label_lc, stats['req_count'])
                    self._add_csv_kv(csv_fields, csv_data,
                                     '%s_errors' % label_lc,
                                     stats['errors'])
                    self._add_csv_kv(csv_fields, csv_data,
                                     '%s_retries' % label_lc,
                                     stats['retries'])
                    self._add_csv_kv(csv_fields, csv_data,
                                     '%s_retry_rate' % label_lc,
                                     '%f' % stats['retry_rate'])
                    self._add_csv_kv(csv_fields, csv_data,
                                     '%s_avg_req_per_s' % label_lc,
                                     stats['avg_req_per_sec'])
                    self._add_stats_for(csv_fields, csv_data, label, 'all',
                                        stats, tmpl_vars['nth_pctile'])
                    for size_str, per_size_stats in sstats.iteritems():
                        if per_size_stats:
                            self._add_stats_for(csv_fields, csv_data, label,
                                                size_str, per_size_stats,
                                                tmpl_vars['nth_pctile'])
            csv_file = StringIO()
            csv_writer = DictWriter(csv_file, csv_fields,
                                    lineterminator='\n',
                                    quoting=csv.QUOTE_NONNUMERIC)
            csv_writer.writeheader()
            csv_writer.writerow(csv_data)
            return csv_file.getvalue()
        else:
            return template.render(scenario=self.scenario, **tmpl_vars)

    def _add_csv_kv(self, csv_fields, csv_data, key, value):
        csv_fields.append(key)
        csv_data[key] = value

    def _add_stats_for(self, csv_fields, csv_data, label, size_str, stats,
                       nth_pctile):
        for latency_type in ('first', 'last'):
            latency_stats = stats['%s_byte_latency' % latency_type]
            key_base = '%s_%s_%s_' % (label.lower(), latency_type, size_str)
            self._add_csv_kv(csv_fields, csv_data, key_base + 'min',
                             latency_stats['min'])
            self._add_csv_kv(csv_fields, csv_data, key_base + 'max',
                             latency_stats['max'])
            self._add_csv_kv(csv_fields, csv_data, key_base + 'avg',
                             latency_stats['avg'])
            self._add_csv_kv(csv_fields, csv_data, key_base + 'std_dev',
                             latency_stats['std_dev'])
            self._add_csv_kv(csv_fields, csv_data,
                             key_base + '%d_pctile' % nth_pctile,
                             latency_stats['pctile'])
            worst_key = 'worst_%s_byte_latency' % latency_type
            self._add_csv_kv(
                csv_fields, csv_data, key_base + 'worst_txid',
                stats[worst_key][1] if worst_key in stats else '')

    def _format_bytes(self, byte_count):
        units = [' B', 'kB', 'MB', 'GB']
        i = 0
        while round(byte_count / 1000.0, 3) >= 1.0:
            byte_count = byte_count / 1000.0
            i += 1
        return '%3.0f %s' % (round(byte_count), units[i])

    def calculate_scenario_stats(self, nth_pctile=95, format_numbers=True):
        """Compute various statistics from worker job result dicts.

        :param nth_pctile: Use this percentile when calculating the stats
        :param format_numbers: Should various floating-point numbers be
        formatted as strings or left full-precision floats
        :returns: A stats python dict which looks something like:
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
                    'retries': 0,
                    'errors' : 0,
                    'avg_req_per_sec': 1.1, # req_count / (stop - start)?
                    'retry_rate': 0.0,
                    'first_byte_latency': SERIES_STATS,
                    'last_byte_latency': SERIES_STATS,
                },
                'worker_stats': {
                    1: {  # keys are worker_ids
                        'start': 1.1,
                        'stop': 1.1,
                        'req_count': 1,
                        'retries': 0,
                        'retry_rate': 0.0,
                        'errors': 0,
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
                                'retries': 0, # num of retries
                                'avg_req_per_sec': 1.1, # total_requests / sum(last_byte_latencies)
                                'errors': 0,
                                'retry_rate': 0.0,
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
                        'retries': 0, # num of retries
                        'acutual_request_count': 1, # num requests includes retries
                        'avg_req_per_sec': 1.1, # total_requests / sum(last_byte_latencies)
                        'errors': 0,
                        'retry_rate': 0.0,
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
        #   'size_str': 'large',
        #   'first_byte_latency': 0.9137639999389648,
        #   'last_byte_latency': 0.913769006729126,
        #   'retries': 1
        #   'completed_at': 1324372892.360802,
        # }
        # OR
        # {
        #   'worker_id': 1,
        #   'type': 'get_object',
        #   'size_str': 'large'
        #   'completed_at': 1324372892.360802,
        #   'retries': 1
        #   'exception': '...',
        # }
        logging.info('Calculating statistics...')
        agg_stats = dict(start=2 ** 32, stop=0, req_count=0)
        op_stats = {}
        for crud_type in [ssbench.CREATE_OBJECT, ssbench.READ_OBJECT,
                          ssbench.UPDATE_OBJECT, ssbench.DELETE_OBJECT]:
            op_stats[crud_type] = dict(
                req_count=0, avg_req_per_sec=0,
                size_stats=OrderedDict.fromkeys(
                    self.scenario.sizes_by_name.keys()))

        req_completion_seconds = {}
        start_time = 0
        completion_time_max = 0
        completion_time_min = 2 ** 32
        stats = dict(
            nth_pctile=nth_pctile,
            agg_stats=agg_stats,
            worker_stats={},
            op_stats=op_stats,
            size_stats=OrderedDict.fromkeys(
                self.scenario.sizes_by_name.keys()))
        for results in self.unpacker:
            skipped = 0
            for result in results:
                try:
                    res_completed_at = result['completed_at']
                    res_completion_time = int(res_completed_at)
                    res_worker_id = result['worker_id']
                    res_type = result['type']
                    res_size_str = result['size_str']
                except KeyError as err:
                    logging.info('Skipped result with missing keys (%r): %r',
                                 err, result)
                    skipped += 1
                    continue

                try:
                    res_exception = result['exception']
                except KeyError:
                    try:
                        res_last_byte_latency = result['last_byte_latency']
                    except KeyError:
                        logging.info('Skipped result with missing'
                                     ' last_byte_latency key: %r',
                                     result)
                        skipped += 1
                        continue
                    if res_completion_time < completion_time_min:
                        completion_time_min = res_completion_time
                        start_time = (
                            res_completion_time - res_last_byte_latency)
                    if res_completion_time > completion_time_max:
                        completion_time_max = res_completion_time
                    req_completion_seconds[res_completion_time] = \
                        1 + req_completion_seconds.get(res_completion_time, 0)
                    result['start'] = res_completed_at - res_last_byte_latency
                else:
                    # report log exceptions
                    logging.warn('calculate_scenario_stats: exception from '
                                 'worker %d: %s',
                                 res_worker_id, res_exception)
                    try:
                        res_traceback = result['traceback']
                    except KeyError:
                        logging.warn('traceback missing')
                    else:
                        logging.info(res_traceback)

                # Stats per-worker
                if res_worker_id not in stats['worker_stats']:
                    stats['worker_stats'][res_worker_id] = {}
                self._add_result_to(stats['worker_stats'][res_worker_id],
                                    result)

                # Stats per-file-size
                try:
                    val = stats['size_stats'][res_size_str]
                except KeyError:
                    stats['size_stats'][res_size_str] = {}
                else:
                    if not val:
                        stats['size_stats'][res_size_str] = {}
                self._add_result_to(stats['size_stats'][res_size_str],
                                    result)

                self._add_result_to(agg_stats, result)

                type_stats = op_stats[res_type]
                self._add_result_to(type_stats, result)

                # Stats per-operation-per-file-size
                try:
                    val = type_stats['size_stats'][res_size_str]
                except KeyError:
                    type_stats['size_stats'][res_size_str] = {}
                else:
                    if not val:
                        type_stats['size_stats'][res_size_str] = {}
                self._add_result_to(
                    type_stats['size_stats'][res_size_str], result)
            if skipped > 0:
                logging.warn("Total number of results skipped: %d", skipped)

        agg_stats['worker_count'] = len(stats['worker_stats'].keys())
        self._compute_req_per_sec(agg_stats)
        self._compute_retry_rate(agg_stats)
        self._compute_latency_stats(agg_stats, nth_pctile, format_numbers)

        jobs_per_worker = []
        for worker_stats in stats['worker_stats'].values():
            jobs_per_worker.append(worker_stats['req_count'])
            self._compute_req_per_sec(worker_stats)
            self._compute_retry_rate(worker_stats)
            self._compute_latency_stats(worker_stats, nth_pctile,
                                        format_numbers)
        stats['jobs_per_worker_stats'] = self._series_stats(jobs_per_worker,
                                                            nth_pctile,
                                                            format_numbers)
        logging.debug('Jobs per worker stats:\n' +
                      pformat(stats['jobs_per_worker_stats']))

        for op_stats_dict in op_stats.itervalues():
            if op_stats_dict['req_count']:
                self._compute_req_per_sec(op_stats_dict)
                self._compute_retry_rate(op_stats_dict)
                self._compute_latency_stats(op_stats_dict, nth_pctile,
                                            format_numbers)
                for size_str, size_stats in \
                        op_stats_dict['size_stats'].iteritems():
                    if size_stats:
                        self._compute_req_per_sec(size_stats)
                        self._compute_retry_rate(size_stats)
                        self._compute_latency_stats(size_stats, nth_pctile,
                                                    format_numbers)
                    else:
                        op_stats_dict['size_stats'].pop(size_str)
        for size_str, size_stats in stats['size_stats'].iteritems():
            if size_stats:
                self._compute_req_per_sec(size_stats)
                self._compute_retry_rate(size_stats)
                self._compute_latency_stats(size_stats, nth_pctile,
                                            format_numbers)
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

    def _compute_latency_stats(self, stat_dict, nth_pctile, format_numbers):
        try:
            for latency_type in ('first_byte_latency', 'last_byte_latency'):
                stat_dict[latency_type] = self._series_stats(
                    stat_dict.get(latency_type, []), nth_pctile,
                    format_numbers)
        except KeyError:
            logging.exception('stat_dict: %r', stat_dict)
            raise

    def _compute_req_per_sec(self, stat_dict):
        try:
            sd_start = stat_dict['start']
        except KeyError:
            stat_dict['avg_req_per_sec'] = 0.0
        else:
            delta_t = stat_dict['stop'] - sd_start
            if delta_t != 0:
                stat_dict['avg_req_per_sec'] = round(
                    stat_dict['req_count'] / delta_t,
                    6)
            else:
                stat_dict['avg_req_per_sec'] = float("inf")

    def _compute_retry_rate(self, stat_dict):
        stat_dict['retry_rate'] = round((float(stat_dict['retries']) /
                                         stat_dict['req_count']) * 100, 6)

    def _add_result_to(self, stat_dict, result):
        if 'errors' not in stat_dict:
            stat_dict['errors'] = 0
        try:
            res_start = result['start']
        except KeyError:
            pass
        else:
            try:
                sd_start = stat_dict['start']
            except KeyError:
                stat_dict['start'] = res_start
            else:
                if res_start < sd_start:
                    stat_dict['start'] = res_start
        try:
            sd_stop = stat_dict['stop']
        except KeyError:
            stat_dict['stop'] = result['completed_at']
        else:
            if result['completed_at'] > sd_stop:
                stat_dict['stop'] = result['completed_at']
        stat_dict['retries'] = \
            stat_dict.get('retries', 0) + int(result['retries'])
        if 'exception' not in result:
            stat_dict['req_count'] = stat_dict.get('req_count', 0) + 1
            self._rec_latency(stat_dict, result)
        else:
            stat_dict['errors'] += 1

    def _series_stats(self, sequence, nth_pctile, format_numbers):
        sequence = filter(None, sequence)
        if not sequence:
            # No data available
            return dict(min=' N/A  ', max='  N/A  ', avg='  N/A  ',
                        pctile='  N/A  ', std_dev='  N/A  ', median='  N/A  ')
        sequence.sort()
        try:
            minval = min(sequence)
            maxval = max(sequence)
            mean = ssbench.util.mean(sequence)
        except ZeroDivisionError:
            # Handle the case of a single-element sequence (population standard
            # deviation divides by N-1)
            minval = sequence[0]
            maxval = sequence[0]
            mean = sequence[0]
        if format_numbers:
            return dict(
                min='%6.3f' % minval,
                max='%7.3f' % maxval,
                avg='%7.3f' % mean,
                pctile='%7.3f' % self.pctile(sequence, nth_pctile),
                std_dev='%7.3f' % ssbench.util.uncorrected_stdev(sequence),
                median='%7.3f' % ssbench.util.median(sequence))
        else:
            return dict(
                min=round(minval, 6),
                max=round(maxval, 6),
                avg=round(mean, 6),
                pctile=round(self.pctile(sequence, nth_pctile), 6),
                std_dev=round(ssbench.util.uncorrected_stdev(sequence), 6),
                median=round(ssbench.util.median(sequence), 6))

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
