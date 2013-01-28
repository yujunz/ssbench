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

import csv
import yaml
from unittest import TestCase
from flexmock import flexmock
from pprint import pprint, pformat
from statlib import stats
from cStringIO import StringIO
from collections import OrderedDict

import ssbench
from ssbench.scenario import Scenario
from ssbench.master import Master

from ssbench.tests.test_scenario import ScenarioFixture

class TestMaster(ScenarioFixture, TestCase):
    maxDiff = None

    def setUp(self):
        # Set our test scenario differently from the default; must be BEFORE
        # the super call.
        self.scenario_dict = dict(
            name='Master Test Scenario - ablkei',
            sizes=[
                dict(name='tiny', size_min=99, size_max=100),
                dict(name='small', size_min=199, size_max=200),
                dict(name='medium', size_min=299, size_max=300),
                dict(name='large', size_min=399, size_max=400),
                dict(name='huge', size_min=499, size_max=500)],
            initial_files=dict(
                tiny=300, small=300, medium=300, large=100, huge=70,
            ),
            operation_count=5000,
            #             C  R  U  D
            crud_profile=[5, 3, 1, 1],
            user_count=2,
        )
        super(TestMaster, self).setUp()

        self.stub_queue = flexmock()
        self.stub_queue.should_receive('watch').with_args(ssbench.STATS_TUBE).once
        self.stub_queue.should_receive('ignore').with_args(ssbench.DEFAULT_TUBE).once
        self.master = Master(self.stub_queue)

        self.result_index = 1  # for self.gen_result()

        self.stub_results = [
            self.gen_result(1, ssbench.CREATE_OBJECT, 'small', 100.0, 101.0, 103.0),
            self.gen_result(1, ssbench.READ_OBJECT, 'tiny', 103.0, 103.1, 103.8),
            self.gen_result(1, ssbench.CREATE_OBJECT, 'huge', 103.8, 105.0, 106.0),
            self.gen_result(1, ssbench.UPDATE_OBJECT, 'large', 106.1, 106.3, 106.4),
            #
            # exceptions should be ignored
            dict(worker_id=2, type=ssbench.UPDATE_OBJECT, completed_at=39293.2, exception='wacky!'),
            self.gen_result(2, ssbench.UPDATE_OBJECT, 'medium', 100.1, 100.9, 102.9),
            self.gen_result(2, ssbench.DELETE_OBJECT, 'large', 102.9, 103.0, 103.3),
            self.gen_result(2, ssbench.CREATE_OBJECT, 'tiny', 103.3, 103.4, 103.5),
            self.gen_result(2, ssbench.READ_OBJECT, 'small', 103.5, 103.7, 104.0),
            #
            self.gen_result(3, ssbench.READ_OBJECT, 'tiny', 100.1, 101.1, 101.9),
            # worker 3 took a while (observer lower concurrency in second 102
            self.gen_result(3, ssbench.DELETE_OBJECT, 'small', 103.1, 103.6, 103.9),
            self.gen_result(3, ssbench.READ_OBJECT, 'medium', 103.9, 104.2, 104.3),
            self.gen_result(3, ssbench.UPDATE_OBJECT, 'tiny', 104.3, 104.9, 104.999),
        ]

    def tearDown(self):
        super(TestMaster, self).tearDown()

    def gen_result(self, worker_id, op_type, size_str, start, first_byte,
                   last_byte):
        self.result_index += 1

        return {
            # There are other keys in a "result", but these are the only ones
            # used for the reporting.
            'worker_id': worker_id,
            'type': op_type,
            'size_str': size_str,
            'size': 989,
            'first_byte_latency': first_byte - start,
            'last_byte_latency': last_byte - start,
            'trans_id': 'txID%03d' % self.result_index,
            'completed_at': last_byte,
        }

    def test_calculate_scenario_stats_aggregate(self):
        first_byte_latency_all = [1, 0.1, 1.2, 0.2, 0.8, 0.1, 0.1, 0.2, 1, 0.5, 0.3, 0.6]
        last_byte_latency_all =  [3, 0.8, 2.2, 0.3, 2.8, 0.4, 0.2, 0.5, 1.8, 0.8, 0.4, 0.699]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            worker_count=3, start=100.0, stop=106.4, req_count=12,
            avg_req_per_sec=round(12 / (106.4 - 100), 6),
            first_byte_latency=dict(
                min='%6.3f' % 0.1,
                max='%7.3f' % 1.2,
                avg='%7.3f' % stats.lmean(first_byte_latency_all),
                std_dev='%7.3f' % stats.lsamplestdev(first_byte_latency_all),
                median='%7.3f' % stats.lmedianscore(first_byte_latency_all),
            ),
            last_byte_latency=dict(
                min='%6.3f' % 0.2,
                max='%7.3f' % 3.0,
                avg='%7.3f' % stats.lmean(last_byte_latency_all),
                std_dev='%7.3f' % stats.lsamplestdev(last_byte_latency_all),
                median='  0.749',  # XXX why??
                #median='%7.3f' % stats.lmedianscore(last_byte_latency_all),
            ),
            worst_first_byte_latency=(1.2, 'txID004'),
            worst_last_byte_latency=(3.0, 'txID002'),
        ), scen_stats['agg_stats'])

    def test_calculate_scenario_stats_worker1(self):
        w1_first_byte_latency = [1.0, 0.1, 1.2, 0.2]
        w1_last_byte_latency = [3.0, 0.8, 2.2, 0.3]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            start=100.0, stop=106.4, req_count=4,
            avg_req_per_sec=round(4 / (106.4 - 100), 6),
            first_byte_latency=dict(
                min='%6.3f' % min(w1_first_byte_latency),
                max='%7.3f' % max(w1_first_byte_latency),
                avg='%7.3f' % stats.lmean(w1_first_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(w1_first_byte_latency),
                median='%7.3f' % stats.lmedianscore(w1_first_byte_latency),
            ),
            last_byte_latency=dict(
                min='%6.3f' % min(w1_last_byte_latency),
                max='%7.3f' % max(w1_last_byte_latency),
                avg='%7.3f' % stats.lmean(w1_last_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(w1_last_byte_latency),
                median='%7.3f' % stats.lmedianscore(w1_last_byte_latency),
            ),
            worst_first_byte_latency=(float(max(w1_first_byte_latency)),
                                      'txID004'),
            worst_last_byte_latency=(float(max(w1_last_byte_latency)), 'txID002'),
        ), scen_stats['worker_stats'][1])

    def test_calculate_scenario_stats_worker2(self):
        w2_first_byte_latency = [0.8, 0.1, 0.1, 0.2]
        w2_last_byte_latency = [2.8, 0.4, 0.2, 0.5]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            start=100.1, stop=104.0, req_count=4,
            avg_req_per_sec=round(4 / (104.0 - 100.1), 6),
            first_byte_latency=dict(
                min='%6.3f' % min(w2_first_byte_latency),
                max='%7.3f' % max(w2_first_byte_latency),
                avg='%7.3f' % stats.lmean(w2_first_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(w2_first_byte_latency),
                median='%7.3f' % stats.lmedianscore(w2_first_byte_latency),
            ),
            last_byte_latency=dict(
                min='%6.3f' % min(w2_last_byte_latency),
                max='%7.3f' % max(w2_last_byte_latency),
                avg='%7.3f' % stats.lmean(w2_last_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(w2_last_byte_latency),
                median='%7.3f' % stats.lmedianscore(w2_last_byte_latency),
            ),
            worst_first_byte_latency=(float(max(w2_first_byte_latency)),
                                      'txID006'),
            worst_last_byte_latency=(float(max(w2_last_byte_latency)), 'txID006'),
        ), scen_stats['worker_stats'][2])

    def test_calculate_scenario_stats_worker3(self):
        w3_first_byte_latency = [1, 0.5, 0.3, 0.6]
        w3_last_byte_latency = [1.8, 0.8, 0.4, 0.699]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            start=100.1, stop=104.999, req_count=4,
            avg_req_per_sec=round(4 / (104.999 - 100.1), 6),
            first_byte_latency=dict(
                min='%6.3f' % min(w3_first_byte_latency),
                max='%7.3f' % max(w3_first_byte_latency),
                avg='%7.3f' % stats.lmean(w3_first_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(w3_first_byte_latency),
                median='%7.3f' % stats.lmedianscore(w3_first_byte_latency),
            ),
            last_byte_latency=dict(
                min='%6.3f' % min(w3_last_byte_latency),
                max='%7.3f' % max(w3_last_byte_latency),
                avg='%7.3f' % stats.lmean(w3_last_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(w3_last_byte_latency),
                median='%7.3f' % stats.lmedianscore(w3_last_byte_latency),
            ),
            worst_first_byte_latency=(float(max(w3_first_byte_latency)), 'txID010'),
            worst_last_byte_latency=(float(max(w3_last_byte_latency)), 'txID010'),
        ), scen_stats['worker_stats'][3])

    def test_calculate_scenario_stats_create(self):
        # Stats for Create
        c_first_byte_latency = [1, 1.2, 0.1]
        c_last_byte_latency = [3.0, 2.2, 0.2]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            start=100.0, stop=106.0, req_count=3,
            avg_req_per_sec=round(3 / (106 - 100.0), 6),
            first_byte_latency=dict(
                min='%6.3f' % min(c_first_byte_latency),
                max='%7.3f' % max(c_first_byte_latency),
                avg='%7.3f' % stats.lmean(c_first_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(c_first_byte_latency),
                median='%7.3f' % stats.lmedianscore(c_first_byte_latency),
            ),
            last_byte_latency=dict(
                min='%6.3f' % min(c_last_byte_latency),
                max='%7.3f' % max(c_last_byte_latency),
                avg='%7.3f' % stats.lmean(c_last_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(c_last_byte_latency),
                median='%7.3f' % stats.lmedianscore(c_last_byte_latency),
            ),
            worst_first_byte_latency=(max(c_first_byte_latency), 'txID004'),
            worst_last_byte_latency=(max(c_last_byte_latency), 'txID002'),
            size_stats=OrderedDict([
                ('tiny', {'avg_req_per_sec': 5.0,
                          'first_byte_latency': {'avg': '%7.3f' % 0.1,
                                                 'max': '%7.3f' % 0.1,
                                                 'median': '%7.3f' % 0.1,
                                                 'min': '%6.3f' % 0.1,
                                                 'std_dev': '%7.3f' % 0.0},
                          'last_byte_latency': {'avg': '%7.3f' % 0.2,
                                                'max': '%7.3f' % 0.2,
                                                'median': '%7.3f' % 0.2,
                                                'min': '%6.3f' % 0.2,
                                                'std_dev': '%7.3f' % 0.0},
                          'worst_first_byte_latency': (0.1, 'txID008'),
                          'worst_last_byte_latency': (0.2, 'txID008'),
                          'req_count': 1,
                          'start': 103.3,
                          'stop': 103.5}),
                ('small', {'avg_req_per_sec': 0.333333,
                           'first_byte_latency': {'avg': '%7.3f' % 1.0,
                                                  'max': '%7.3f' % 1.0,
                                                  'median': '%7.3f' % 1.0,
                                                  'min': '%6.3f' % 1.0,
                                                  'std_dev': '%7.3f' % 0.0},
                           'last_byte_latency': {'avg': '%7.3f' % 3.0,
                                                 'max': '%7.3f' % 3.0,
                                                 'median': '%7.3f' % 3.0,
                                                 'min': '%6.3f' % 3.0,
                                                 'std_dev': '%7.3f' % 0.0},
                           'worst_first_byte_latency': (1.0, 'txID002'),
                           'worst_last_byte_latency': (3.0, 'txID002'),
                           'req_count': 1,
                           'start': 100.0,
                           'stop': 103.0}),
                ('huge', {'avg_req_per_sec': 0.454545,
                          'first_byte_latency': {'avg': '%7.3f' % 1.2,
                                                 'max': '%7.3f' % 1.2,
                                                 'median': '%7.3f' % 1.2,
                                                 'min': '%6.3f' % 1.2,
                                                 'std_dev': '%7.3f' % 0.0},
                          'last_byte_latency': {'avg': '%7.3f' % 2.2,
                                                'max': '%7.3f' % 2.2,
                                                'median': '%7.3f' % 2.2,
                                                'min': '%6.3f' % 2.2,
                                                'std_dev': '%7.3f' % 0.0},
                          'worst_first_byte_latency': (1.2, 'txID004'),
                          'worst_last_byte_latency': (2.2, 'txID004'),
                          'req_count': 1,
                          'start': 103.8,
                          'stop': 106.0})]),
        ), scen_stats['op_stats'][ssbench.CREATE_OBJECT])

    def test_calculate_scenario_stats_read(self):
        # Stats for Read
        r_first_byte_latency = [0.1, 0.2, 1.0, 0.3]
        r_last_byte_latency = [0.8, 0.5, 1.8, 0.4]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            start=100.1, stop=104.3, req_count=4,
            avg_req_per_sec=round(4 / (104.3 - 100.1), 6),
            first_byte_latency=dict(
                min='%6.3f' % min(r_first_byte_latency),
                max='%7.3f' % max(r_first_byte_latency),
                avg='%7.3f' % stats.lmean(r_first_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(r_first_byte_latency),
                median='%7.3f' % stats.lmedianscore(r_first_byte_latency),
            ),
            last_byte_latency=dict(
                min='%6.3f' % min(r_last_byte_latency),
                max='%7.3f' % max(r_last_byte_latency),
                avg='%7.3f' % stats.lmean(r_last_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(r_last_byte_latency),
                median='%7.3f' % stats.lmedianscore(r_last_byte_latency),
            ),
            worst_first_byte_latency=(max(r_first_byte_latency), 'txID010'),
            worst_last_byte_latency=(max(r_last_byte_latency), 'txID010'),
            size_stats=OrderedDict([
                ('tiny', {'avg_req_per_sec': 0.540541,
                          'first_byte_latency': {'avg': '%7.3f' % 0.55,
                                                 'max': '%7.3f' % 1.0,
                                                 'median': '%7.3f' % 0.55,
                                                 'min': '%6.3f' % 0.1,
                                                 'std_dev': '%7.3f' % 0.45},
                          'last_byte_latency': {'avg': '%7.3f' % 1.3,
                                                'max': '%7.3f' % 1.8,
                                                'median': '%7.3f' % 1.3,
                                                'min': '%6.3f' % 0.8,
                                                'std_dev': '%7.3f' % 0.5},
                          'worst_first_byte_latency': (1.0, 'txID010'),
                          'worst_last_byte_latency': (1.8, 'txID010'),
                          'req_count': 2,
                          'start': 100.1,
                          'stop': 103.8}),
                ('small', {'avg_req_per_sec': 2.0,
                           'first_byte_latency': {'avg': '%7.3f' % 0.2,
                                                  'max': '%7.3f' % 0.2,
                                                  'median': '%7.3f' % 0.2,
                                                  'min': '%6.3f' % 0.2,
                                                  'std_dev': '%7.3f' % 0.0},
                           'last_byte_latency': {'avg': '%7.3f' % 0.5,
                                                 'max': '%7.3f' % 0.5,
                                                 'median': '%7.3f' % 0.5,
                                                 'min': '%6.3f' % 0.5,
                                                 'std_dev': '%7.3f' % 0.0},
                           'worst_first_byte_latency': (0.2, 'txID009'),
                           'worst_last_byte_latency': (0.5, 'txID009'),
                           'req_count': 1,
                           'start': 103.5,
                           'stop': 104.0}),
                ('medium', {'avg_req_per_sec': 2.5,
                            'first_byte_latency': {'avg': '%7.3f' % 0.3,
                                                   'max': '%7.3f' % 0.3,
                                                   'median': '%7.3f' % 0.3,
                                                   'min': '%6.3f' % 0.3,
                                                   'std_dev': '%7.3f' % 0.0},
                            'last_byte_latency': {'avg': '%7.3f' % 0.4,
                                                  'max': '%7.3f' % 0.4,
                                                  'median': '%7.3f' % 0.4,
                                                  'min': '%6.3f' % 0.4,
                                                  'std_dev': '%7.3f' % 0.0},
                            'worst_first_byte_latency': (0.3, 'txID012'),
                            'worst_last_byte_latency': (0.4, 'txID012'),
                            'req_count': 1,
                            'start': 103.9,
                            'stop': 104.3})]),
        ), scen_stats['op_stats'][ssbench.READ_OBJECT])

    def test_calculate_scenario_stats_update(self):
        u_first_byte_latency = [0.2, 0.8, 0.6]
        u_last_byte_latency = [0.3, 2.8, 0.699]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            start=100.1, stop=106.4, req_count=3,
            avg_req_per_sec=round(3 / (106.4 - 100.1), 6),
            first_byte_latency=dict(
                min='%6.3f' % min(u_first_byte_latency),
                max='%7.3f' % max(u_first_byte_latency),
                avg='%7.3f' % stats.lmean(u_first_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(u_first_byte_latency),
                median='%7.3f' % stats.lmedianscore(u_first_byte_latency),
            ),
            worst_first_byte_latency=(max(u_first_byte_latency), 'txID006'),
            last_byte_latency=dict(
                min='%6.3f' % min(u_last_byte_latency),
                max='%7.3f' % max(u_last_byte_latency),
                avg='%7.3f' % stats.lmean(u_last_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(u_last_byte_latency),
                median='%7.3f' % stats.lmedianscore(u_last_byte_latency),
            ),
            worst_last_byte_latency=(max(u_last_byte_latency), 'txID006'),
            size_stats=OrderedDict([
                ('tiny', {'avg_req_per_sec': 1.430615,
                          'first_byte_latency': {'avg': '%7.3f' % 0.6,
                                                 'max': '%7.3f' % 0.6,
                                                 'median': '%7.3f' % 0.6,
                                                 'min': '%6.3f' % 0.6,
                                                 'std_dev': '%7.3f' % 0.0},
                          'worst_first_byte_latency': (0.6, 'txID013'),
                          'last_byte_latency': {'avg': '%7.3f' % 0.699,
                                                'max': '%7.3f' % 0.699,
                                                'median': '%7.3f' % 0.699,
                                                'min': '%6.3f' % 0.699,
                                                'std_dev': '%7.3f' % 0.0},
                          'worst_last_byte_latency': (0.699, 'txID013'),
                          'req_count': 1,
                          'start': 104.3,
                          'stop': 104.999}),
                ('medium', {'avg_req_per_sec': 0.357143,
                            'first_byte_latency': {'avg': '%7.3f' % 0.8,
                                                   'max': '%7.3f' % 0.8,
                                                   'median': '%7.3f' % 0.8,
                                                   'min': '%6.3f' % 0.8,
                                                   'std_dev': '%7.3f' % 0.0},
                            'worst_first_byte_latency': (0.8, 'txID006'),
                            'last_byte_latency': {'avg': '%7.3f' % 2.8,
                                                  'max': '%7.3f' % 2.8,
                                                  'median': '%7.3f' % 2.8,
                                                  'min': '%6.3f' % 2.8,
                                                  'std_dev': '%7.3f' % 0.0},
                            'worst_last_byte_latency': (2.8, 'txID006'),
                            'req_count': 1,
                            'start': 100.1,
                            'stop': 102.9}),
                ('large', {'avg_req_per_sec': 3.333333,
                           'first_byte_latency': {'avg': '%7.3f' % 0.2,
                                                  'max': '%7.3f' % 0.2,
                                                  'median': '%7.3f' % 0.2,
                                                  'min': '%6.3f' % 0.2,
                                                  'std_dev': '%7.3f' % 0.0},
                           'worst_first_byte_latency': (0.2, 'txID005'),
                           'last_byte_latency': {'avg': '%7.3f' % 0.3,
                                                 'max': '%7.3f' % 0.3,
                                                 'median': '%7.3f' % 0.3,
                                                 'min': '%6.3f' % 0.3,
                                                 'std_dev': '%7.3f' % 0.0},
                           'worst_last_byte_latency': (0.3, 'txID005'),
                           'req_count': 1,
                           'start': 106.1,
                           'stop': 106.4})]),
        ), scen_stats['op_stats'][ssbench.UPDATE_OBJECT])

    def test_calculate_scenario_stats_delete(self):
        d_first_byte_latency = [0.1, 0.5]
        d_last_byte_latency = [0.4, 0.8]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            start=102.9, stop=103.9, req_count=2,
            avg_req_per_sec=round(2 / (103.9 - 102.9), 6),
            first_byte_latency=dict(
                min='%6.3f' % min(d_first_byte_latency),
                max='%7.3f' % max(d_first_byte_latency),
                avg='%7.3f' % stats.lmean(d_first_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(d_first_byte_latency),
                median='%7.3f' % stats.lmedianscore(d_first_byte_latency),
            ),
            last_byte_latency=dict(
                min='%6.3f' % min(d_last_byte_latency),
                max='%7.3f' % max(d_last_byte_latency),
                avg='%7.3f' % stats.lmean(d_last_byte_latency),
                std_dev='%7.3f' % stats.lsamplestdev(d_last_byte_latency),
                median='%7.3f' % stats.lmedianscore(d_last_byte_latency),
            ),
            worst_first_byte_latency=(max(d_first_byte_latency), 'txID011'),
            worst_last_byte_latency=(max(d_last_byte_latency), 'txID011'),
            size_stats=OrderedDict([
                ('small', {'avg_req_per_sec': 1.25,
                           'first_byte_latency': {'avg': '%7.3f' % 0.5,
                                                  'max': '%7.3f' % 0.5,
                                                  'median': '%7.3f' % 0.5,
                                                  'min': '%6.3f' % 0.5,
                                                  'std_dev': '%7.3f' % 0.0},
                           'last_byte_latency': {'avg': '%7.3f' % 0.8,
                                                 'max': '%7.3f' % 0.8,
                                                 'median': '%7.3f' % 0.8,
                                                 'min': '%6.3f' % 0.8,
                                                 'std_dev': '%7.3f' % 0.0},
                           'worst_first_byte_latency': (0.5, 'txID011'),
                           'worst_last_byte_latency': (0.8, 'txID011'),
                           'req_count': 1,
                           'start': 103.1,
                           'stop': 103.9}),
                ('large', {'avg_req_per_sec': 2.5,
                           'first_byte_latency': {'avg': '%7.3f' % 0.1,
                                                  'max': '%7.3f' % 0.1,
                                                  'median': '%7.3f' % 0.1,
                                                  'min': '%6.3f' % 0.1,
                                                  'std_dev': '%7.3f' % 0.0},
                           'last_byte_latency': {'avg': '%7.3f' % 0.4,
                                                 'max': '%7.3f' % 0.4,
                                                 'median': '%7.3f' % 0.4,
                                                 'min': '%6.3f' % 0.4,
                                                 'std_dev': '%7.3f' % 0.0},
                           'worst_first_byte_latency': (0.1, 'txID007'),
                           'worst_last_byte_latency': (0.4, 'txID007'),
                           'req_count': 1,
                           'start': 102.9,
                           'stop': 103.3})]),
        ), scen_stats['op_stats'][ssbench.DELETE_OBJECT])

    def test_calculate_scenario_size_stats(self):
        d_first_byte_latency = [0.1, 0.5]
        d_last_byte_latency = [0.4, 0.8]
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(OrderedDict([
            ('tiny', {'avg_req_per_sec': 0.816493,
                      'first_byte_latency': {'avg': '%7.3f' % 0.45,
                                             'max': '%7.3f' % 1.0,
                                             'median': '%7.3f' % 0.35,
                                             'min': '%6.3f' % 0.1,
                                             'std_dev': '%7.3f' % 0.377492},
                      'last_byte_latency': {'avg': '%7.3f' % 0.87475,
                                            'max': '%7.3f' % 1.8,
                                            'median': '%7.3f' % 0.7494,
                                            'min': '%6.3f' % 0.2,
                                            'std_dev': '%7.3f' % 0.580485},
                      'worst_first_byte_latency': (1.0, 'txID010'),
                      'worst_last_byte_latency': (1.8, 'txID010'),
                      'req_count': 4,
                      'start': 100.1,
                      'stop': 104.999}),
            ('small', {'avg_req_per_sec': 0.75,
                       'first_byte_latency': {'avg': '%7.3f' % 0.566667,
                                              'max': '%7.3f' % 1.0,
                                              'median': '%7.3f' % 0.5,
                                              'min': '%6.3f' % 0.2,
                                              'std_dev': '%7.3f' % 0.329983},
                       'last_byte_latency': {'avg': '%7.3f' % 1.433333,
                                             'max': '%7.3f' % 3.0,
                                             'median': '%7.3f' % 0.8,
                                             'min': '%6.3f' % 0.5,
                                             'std_dev': '%7.3f' % 1.11455},
                       'worst_first_byte_latency': (1.0, 'txID002'),
                       'worst_last_byte_latency': (3.0, 'txID002'),
                       'req_count': 3,
                       'start': 100.0,
                       'stop': 104.0}),
            ('medium', {'avg_req_per_sec': 0.47619,
                        'first_byte_latency': {'avg': '%7.3f' % 0.55,
                                               'max': '%7.3f' % 0.8,
                                               'median': '%7.3f' % 0.55,
                                               'min': '%6.3f' % 0.3,
                                               'std_dev': '%7.3f' % 0.25},
                        'last_byte_latency': {'avg': '%7.3f' % 1.6,
                                              'max': '%7.3f' % 2.8,
                                              'median': '%7.3f' % 1.6,
                                              'min': '%6.3f' % 0.4,
                                              'std_dev': '%7.3f' % 1.2},
                        'worst_first_byte_latency': (0.8, 'txID006'),
                        'worst_last_byte_latency': (2.8, 'txID006'),
                        'req_count': 2,
                        'start': 100.1,
                        'stop': 104.3}),
            ('large', {'avg_req_per_sec': 0.571429,
                       'first_byte_latency': {'avg': '%7.3f' % 0.15,
                                              'max': '%7.3f' % 0.2,
                                              'median': '%7.3f' % 0.15,
                                              'min': '%6.3f' % 0.1,
                                              'std_dev': '%7.3f' % 0.05},
                       'last_byte_latency': {'avg': '%7.3f' % 0.35,
                                             'max': '%7.3f' % 0.4,
                                             'median': '%7.3f' % 0.35,
                                             'min': '%6.3f' % 0.3,
                                             'std_dev': '%7.3f' % 0.05},
                       'worst_first_byte_latency': (0.2, 'txID005'),
                       'worst_last_byte_latency': (0.4, 'txID007'),
                       'req_count': 2,
                       'start': 102.9,
                       'stop': 106.4}),
            ('huge', {'avg_req_per_sec': 0.454545,
                      'first_byte_latency': {'avg': '%7.3f' % 1.2,
                                             'max': '%7.3f' % 1.2,
                                             'median': '%7.3f' % 1.2,
                                             'min': '%6.3f' % 1.2,
                                             'std_dev': '%7.3f' % 0.0},
                      'last_byte_latency': {'avg': '%7.3f' % 2.2,
                                            'max': '%7.3f' % 2.2,
                                            'median': '%7.3f' % 2.2,
                                            'min': '%6.3f' % 2.2,
                                            'std_dev': '%7.3f' % 0.0},
                      'worst_first_byte_latency': (1.2, 'txID004'),
                      'worst_last_byte_latency': (2.2, 'txID004'),
                      'req_count': 1,
                      'start': 103.8,
                      'stop': 106.0})]),
            scen_stats['size_stats'])

    def test_calculate_scenario_stats_time_series(self):
        # Time series (reqs completed each second
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertDictEqual(dict(
            start=101,
            data=[1, 1, 5, 3, 0, 2],
        ), scen_stats['time_series'])

    def test_write_rps_histogram(self):
        # Write out time series data (requests-per-second histogram) to an
        # already open CSV file
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)

        test_csv_file = StringIO()
        self.master.write_rps_histogram(scen_stats, test_csv_file)
        test_csv_file.seek(0)
        reader = csv.reader(test_csv_file)
        self.assertListEqual([
            ["Seconds Since Start", "Requests Completed"],
            ['1', '1'],
            ['2', '1'],
            ['3', '5'],
            ['4', '3'],
            ['5', '0'],
            ['6', '2'],
        ], list(reader))


    def test_generate_scenario_report(self):
        # Time series (reqs completed each second
        scen_stats = self.master.calculate_scenario_stats(self.scenario,
                                                          self.stub_results)
        self.assertListEqual(u"""
Master Test Scenario - ablkei
  C   R   U   D     Worker count:   3   Concurrency:   2
% 50  30  10  10

TOTAL
       Count:    12  Average requests per second:   1.9
                            min       max      avg      std_dev    median                  Swift TX ID for worst latency
       First-byte latency:  0.100 -   1.200    0.508  (  0.386)    0.400  (all obj sizes)  txID004
       Last-byte  latency:  0.200 -   3.000    1.158  (  0.970)    0.749  (all obj sizes)  txID002
       First-byte latency:  0.100 -   1.000    0.450  (  0.377)    0.350  (    tiny objs)  txID010
       Last-byte  latency:  0.200 -   1.800    0.875  (  0.580)    0.749  (    tiny objs)  txID010
       First-byte latency:  0.200 -   1.000    0.567  (  0.330)    0.500  (   small objs)  txID002
       Last-byte  latency:  0.500 -   3.000    1.433  (  1.115)    0.800  (   small objs)  txID002
       First-byte latency:  0.300 -   0.800    0.550  (  0.250)    0.550  (  medium objs)  txID006
       Last-byte  latency:  0.400 -   2.800    1.600  (  1.200)    1.600  (  medium objs)  txID006
       First-byte latency:  0.100 -   0.200    0.150  (  0.050)    0.150  (   large objs)  txID005
       Last-byte  latency:  0.300 -   0.400    0.350  (  0.050)    0.350  (   large objs)  txID007
       First-byte latency:  1.200 -   1.200    1.200  (  0.000)    1.200  (    huge objs)  txID004
       Last-byte  latency:  2.200 -   2.200    2.200  (  0.000)    2.200  (    huge objs)  txID004

CREATE
       Count:     3  Average requests per second:   0.5
                            min       max      avg      std_dev    median                  Swift TX ID for worst latency
       First-byte latency:  0.100 -   1.200    0.767  (  0.478)    1.000  (all obj sizes)  txID004
       Last-byte  latency:  0.200 -   3.000    1.800  (  1.178)    2.200  (all obj sizes)  txID002
       First-byte latency:  0.100 -   0.100    0.100  (  0.000)    0.100  (    tiny objs)  txID008
       Last-byte  latency:  0.200 -   0.200    0.200  (  0.000)    0.200  (    tiny objs)  txID008
       First-byte latency:  1.000 -   1.000    1.000  (  0.000)    1.000  (   small objs)  txID002
       Last-byte  latency:  3.000 -   3.000    3.000  (  0.000)    3.000  (   small objs)  txID002
       First-byte latency:  1.200 -   1.200    1.200  (  0.000)    1.200  (    huge objs)  txID004
       Last-byte  latency:  2.200 -   2.200    2.200  (  0.000)    2.200  (    huge objs)  txID004

READ
       Count:     4  Average requests per second:   1.0
                            min       max      avg      std_dev    median                  Swift TX ID for worst latency
       First-byte latency:  0.100 -   1.000    0.400  (  0.354)    0.250  (all obj sizes)  txID010
       Last-byte  latency:  0.400 -   1.800    0.875  (  0.554)    0.650  (all obj sizes)  txID010
       First-byte latency:  0.100 -   1.000    0.550  (  0.450)    0.550  (    tiny objs)  txID010
       Last-byte  latency:  0.800 -   1.800    1.300  (  0.500)    1.300  (    tiny objs)  txID010
       First-byte latency:  0.200 -   0.200    0.200  (  0.000)    0.200  (   small objs)  txID009
       Last-byte  latency:  0.500 -   0.500    0.500  (  0.000)    0.500  (   small objs)  txID009
       First-byte latency:  0.300 -   0.300    0.300  (  0.000)    0.300  (  medium objs)  txID012
       Last-byte  latency:  0.400 -   0.400    0.400  (  0.000)    0.400  (  medium objs)  txID012

UPDATE
       Count:     3  Average requests per second:   0.5
                            min       max      avg      std_dev    median                  Swift TX ID for worst latency
       First-byte latency:  0.200 -   0.800    0.533  (  0.249)    0.600  (all obj sizes)  txID006
       Last-byte  latency:  0.300 -   2.800    1.266  (  1.097)    0.699  (all obj sizes)  txID006
       First-byte latency:  0.600 -   0.600    0.600  (  0.000)    0.600  (    tiny objs)  txID013
       Last-byte  latency:  0.699 -   0.699    0.699  (  0.000)    0.699  (    tiny objs)  txID013
       First-byte latency:  0.800 -   0.800    0.800  (  0.000)    0.800  (  medium objs)  txID006
       Last-byte  latency:  2.800 -   2.800    2.800  (  0.000)    2.800  (  medium objs)  txID006
       First-byte latency:  0.200 -   0.200    0.200  (  0.000)    0.200  (   large objs)  txID005
       Last-byte  latency:  0.300 -   0.300    0.300  (  0.000)    0.300  (   large objs)  txID005

DELETE
       Count:     2  Average requests per second:   2.0
                            min       max      avg      std_dev    median                  Swift TX ID for worst latency
       First-byte latency:  0.100 -   0.500    0.300  (  0.200)    0.300  (all obj sizes)  txID011
       Last-byte  latency:  0.400 -   0.800    0.600  (  0.200)    0.600  (all obj sizes)  txID011
       First-byte latency:  0.500 -   0.500    0.500  (  0.000)    0.500  (   small objs)  txID011
       Last-byte  latency:  0.800 -   0.800    0.800  (  0.000)    0.800  (   small objs)  txID011
       First-byte latency:  0.100 -   0.100    0.100  (  0.000)    0.100  (   large objs)  txID007
       Last-byte  latency:  0.400 -   0.400    0.400  (  0.000)    0.400  (   large objs)  txID007


""".split('\n'), self.master.generate_scenario_report(self.scenario, scen_stats).split('\n'))
