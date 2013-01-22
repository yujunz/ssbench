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
                min=0.1, max=1.2,
                avg=round(stats.lmean(first_byte_latency_all), 6),
                std_dev=round(stats.lsamplestdev(first_byte_latency_all), 6),
                median=round(stats.lmedianscore(first_byte_latency_all), 6),
            ),
            last_byte_latency=dict(
                min=0.2, max=3,
                avg=round(stats.lmean(last_byte_latency_all), 6),
                std_dev=round(stats.lsamplestdev(last_byte_latency_all), 6),
                median=round(stats.lmedianscore(last_byte_latency_all), 6),
            ),
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
                min=min(w1_first_byte_latency), max=max(w1_first_byte_latency),
                avg=round(stats.lmean(w1_first_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(w1_first_byte_latency), 6),
                median=round(stats.lmedianscore(w1_first_byte_latency), 6),
            ),
            last_byte_latency=dict(
                min=min(w1_last_byte_latency), max=max(w1_last_byte_latency),
                avg=round(stats.lmean(w1_last_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(w1_last_byte_latency), 6),
                median=round(stats.lmedianscore(w1_last_byte_latency), 6),
            ),
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
                min=min(w2_first_byte_latency), max=max(w2_first_byte_latency),
                avg=round(stats.lmean(w2_first_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(w2_first_byte_latency), 6),
                median=round(stats.lmedianscore(w2_first_byte_latency), 6),
            ),
            last_byte_latency=dict(
                min=min(w2_last_byte_latency), max=max(w2_last_byte_latency),
                avg=round(stats.lmean(w2_last_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(w2_last_byte_latency), 6),
                median=round(stats.lmedianscore(w2_last_byte_latency), 6),
            ),
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
                min=min(w3_first_byte_latency), max=max(w3_first_byte_latency),
                avg=round(stats.lmean(w3_first_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(w3_first_byte_latency), 6),
                median=round(stats.lmedianscore(w3_first_byte_latency), 6),
            ),
            last_byte_latency=dict(
                min=min(w3_last_byte_latency), max=max(w3_last_byte_latency),
                avg=round(stats.lmean(w3_last_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(w3_last_byte_latency), 6),
                median=round(stats.lmedianscore(w3_last_byte_latency), 6),
            ),
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
                min=min(c_first_byte_latency), max=max(c_first_byte_latency),
                avg=round(stats.lmean(c_first_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(c_first_byte_latency), 6),
                median=round(stats.lmedianscore(c_first_byte_latency), 6),
            ),
            last_byte_latency=dict(
                min=min(c_last_byte_latency), max=max(c_last_byte_latency),
                avg=round(stats.lmean(c_last_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(c_last_byte_latency), 6),
                median=round(stats.lmedianscore(c_last_byte_latency), 6),
            ),
            size_stats=OrderedDict([
                ('tiny', {'avg_req_per_sec': 5.0,
                        'first_byte_latency': {'avg': 0.1,
                                               'max': 0.1,
                                               'median': 0.1,
                                               'min': 0.1,
                                               'std_dev': 0.0},
                        'last_byte_latency': {'avg': 0.2,
                                              'max': 0.2,
                                              'median': 0.2,
                                              'min': 0.2,
                                              'std_dev': 0.0},
                        'req_count': 1,
                        'start': 103.3,
                        'stop': 103.5}),
                ('small', {'avg_req_per_sec': 0.333333,
                          'first_byte_latency': {'avg': 1.0,
                                                 'max': 1.0,
                                                 'median': 1.0,
                                                 'min': 1.0,
                                                 'std_dev': 0.0},
                          'last_byte_latency': {'avg': 3.0,
                                                'max': 3.0,
                                                'median': 3.0,
                                                'min': 3.0,
                                                'std_dev': 0.0},
                          'req_count': 1,
                          'start': 100.0,
                          'stop': 103.0}),
                ('huge', {'avg_req_per_sec': 0.454545,
                             'first_byte_latency': {'avg': 1.2,
                                                    'max': 1.2,
                                                    'median': 1.2,
                                                    'min': 1.2,
                                                    'std_dev': 0.0},
                             'last_byte_latency': {'avg': 2.2,
                                                   'max': 2.2,
                                                   'median': 2.2,
                                                   'min': 2.2,
                                                   'std_dev': 0.0},
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
                min=min(r_first_byte_latency), max=max(r_first_byte_latency),
                avg=round(stats.lmean(r_first_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(r_first_byte_latency), 6),
                median=round(stats.lmedianscore(r_first_byte_latency), 6),
            ),
            last_byte_latency=dict(
                min=min(r_last_byte_latency), max=max(r_last_byte_latency),
                avg=round(stats.lmean(r_last_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(r_last_byte_latency), 6),
                median=round(stats.lmedianscore(r_last_byte_latency), 6),
            ),
            size_stats=OrderedDict([
                ('tiny', {'avg_req_per_sec': 0.540541,
                        'first_byte_latency': {'avg': 0.55,
                                               'max': 1.0,
                                               'median': 0.55,
                                               'min': 0.1,
                                               'std_dev': 0.45},
                        'last_byte_latency': {'avg': 1.3,
                                              'max': 1.8,
                                              'median': 1.3,
                                              'min': 0.8,
                                              'std_dev': 0.5},
                        'req_count': 2,
                        'start': 100.1,
                        'stop': 103.8}),
                ('small', {'avg_req_per_sec': 2.0,
                          'first_byte_latency': {'avg': 0.2,
                                                 'max': 0.2,
                                                 'median': 0.2,
                                                 'min': 0.2,
                                                 'std_dev': 0.0},
                          'last_byte_latency': {'avg': 0.5,
                                                'max': 0.5,
                                                'median': 0.5,
                                                'min': 0.5,
                                                'std_dev': 0.0},
                          'req_count': 1,
                          'start': 103.5,
                          'stop': 104.0}),
                ('medium', {'avg_req_per_sec': 2.5,
                          'first_byte_latency': {'avg': 0.3,
                                                 'max': 0.3,
                                                 'median': 0.3,
                                                 'min': 0.3,
                                                 'std_dev': 0.0},
                          'last_byte_latency': {'avg': 0.4,
                                                'max': 0.4,
                                                'median': 0.4,
                                                'min': 0.4,
                                                'std_dev': 0.0},
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
                min=min(u_first_byte_latency), max=max(u_first_byte_latency),
                avg=round(stats.lmean(u_first_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(u_first_byte_latency), 6),
                median=round(stats.lmedianscore(u_first_byte_latency), 6),
            ),
            last_byte_latency=dict(
                min=min(u_last_byte_latency), max=max(u_last_byte_latency),
                avg=round(stats.lmean(u_last_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(u_last_byte_latency), 6),
                median=round(stats.lmedianscore(u_last_byte_latency), 6),
            ),
            size_stats=OrderedDict([
                ('tiny', {'avg_req_per_sec': 1.430615,
                        'first_byte_latency': {'avg': 0.6,
                                               'max': 0.6,
                                               'median': 0.6,
                                               'min': 0.6,
                                               'std_dev': 0.0},
                        'last_byte_latency': {'avg': 0.699,
                                              'max': 0.699,
                                              'median': 0.699,
                                              'min': 0.699,
                                              'std_dev': 0.0},
                        'req_count': 1,
                        'start': 104.3,
                        'stop': 104.999}),
                ('medium', {'avg_req_per_sec': 0.357143,
                          'first_byte_latency': {'avg': 0.8,
                                                 'max': 0.8,
                                                 'median': 0.8,
                                                 'min': 0.8,
                                                 'std_dev': 0.0},
                          'last_byte_latency': {'avg': 2.8,
                                                'max': 2.8,
                                                'median': 2.8,
                                                'min': 2.8,
                                                'std_dev': 0.0},
                          'req_count': 1,
                          'start': 100.1,
                          'stop': 102.9}),
                ('large', {'avg_req_per_sec': 3.333333,
                            'first_byte_latency': {'avg': 0.2,
                                                   'max': 0.2,
                                                   'median': 0.2,
                                                   'min': 0.2,
                                                   'std_dev': 0.0},
                            'last_byte_latency': {'avg': 0.3,
                                                  'max': 0.3,
                                                  'median': 0.3,
                                                  'min': 0.3,
                                                  'std_dev': 0.0},
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
                min=min(d_first_byte_latency), max=max(d_first_byte_latency),
                avg=round(stats.lmean(d_first_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(d_first_byte_latency), 6),
                median=round(stats.lmedianscore(d_first_byte_latency), 6),
            ),
            last_byte_latency=dict(
                min=min(d_last_byte_latency), max=max(d_last_byte_latency),
                avg=round(stats.lmean(d_last_byte_latency), 6),
                std_dev=round(stats.lsamplestdev(d_last_byte_latency), 6),
                median=round(stats.lmedianscore(d_last_byte_latency), 6),
            ),
            size_stats=OrderedDict([
                ('small', {'avg_req_per_sec': 1.25,
                          'first_byte_latency': {'avg': 0.5,
                                                 'max': 0.5,
                                                 'median': 0.5,
                                                 'min': 0.5,
                                                 'std_dev': 0.0},
                          'last_byte_latency': {'avg': 0.8,
                                                'max': 0.8,
                                                'median': 0.8,
                                                'min': 0.8,
                                                'std_dev': 0.0},
                          'req_count': 1,
                          'start': 103.1,
                          'stop': 103.9}),
                ('large', {'avg_req_per_sec': 2.5,
                            'first_byte_latency': {'avg': 0.1,
                                                   'max': 0.1,
                                                   'median': 0.1,
                                                   'min': 0.1,
                                                   'std_dev': 0.0},
                            'last_byte_latency': {'avg': 0.4,
                                                  'max': 0.4,
                                                  'median': 0.4,
                                                  'min': 0.4,
                                                  'std_dev': 0.0},
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
                    'first_byte_latency': {'avg': 0.45,
                                           'max': 1.0,
                                           'median': 0.35,
                                           'min': 0.1,
                                           'std_dev': 0.377492},
                    'last_byte_latency': {'avg': 0.87475,
                                          'max': 1.8,
                                          'median': 0.7495,
                                          'min': 0.2,
                                          'std_dev': 0.580485},
                    'req_count': 4,
                    'start': 100.1,
                    'stop': 104.999}),
            ('small', {'avg_req_per_sec': 0.75,
                      'first_byte_latency': {'avg': 0.566667,
                                             'max': 1.0,
                                             'median': 0.5,
                                             'min': 0.2,
                                             'std_dev': 0.329983},
                      'last_byte_latency': {'avg': 1.433333,
                                            'max': 3.0,
                                            'median': 0.8,
                                            'min': 0.5,
                                            'std_dev': 1.11455},
                      'req_count': 3,
                      'start': 100.0,
                      'stop': 104.0}),
            ('medium', {'avg_req_per_sec': 0.47619,
                      'first_byte_latency': {'avg': 0.55,
                                             'max': 0.8,
                                             'median': 0.55,
                                             'min': 0.3,
                                             'std_dev': 0.25},
                      'last_byte_latency': {'avg': 1.6,
                                            'max': 2.8,
                                            'median': 1.6,
                                            'min': 0.4,
                                            'std_dev': 1.2},
                      'req_count': 2,
                      'start': 100.1,
                      'stop': 104.3}),
            ('large', {'avg_req_per_sec': 0.571429,
                        'first_byte_latency': {'avg': 0.15,
                                               'max': 0.2,
                                               'median': 0.15,
                                               'min': 0.1,
                                               'std_dev': 0.05},
                        'last_byte_latency': {'avg': 0.35,
                                              'max': 0.4,
                                              'median': 0.35,
                                              'min': 0.3,
                                              'std_dev': 0.05},
                        'req_count': 2,
                        'start': 102.9,
                        'stop': 106.4}),
            ('huge', {'avg_req_per_sec': 0.454545,
                         'first_byte_latency': {'avg': 1.2,
                                                'max': 1.2,
                                                'median': 1.2,
                                                'min': 1.2,
                                                'std_dev': 0.0},
                         'last_byte_latency': {'avg': 2.2,
                                               'max': 2.2,
                                               'median': 2.2,
                                               'min': 2.2,
                                               'std_dev': 0.0},
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
                           min      max     avg     std_dev   median
       First-byte latency:  0.10 -   1.20    0.51  (  0.39)    0.40  (  all obj sizes)
       Last-byte  latency:  0.20 -   3.00    1.16  (  0.97)    0.75  (  all obj sizes)
       First-byte latency:  0.10 -   1.00    0.45  (  0.38)    0.35  (tiny objs)
       Last-byte  latency:  0.20 -   1.80    0.87  (  0.58)    0.75  (tiny objs)
       First-byte latency:  0.20 -   1.00    0.57  (  0.33)    0.50  (small objs)
       Last-byte  latency:  0.50 -   3.00    1.43  (  1.11)    0.80  (small objs)
       First-byte latency:  0.30 -   0.80    0.55  (  0.25)    0.55  (medium objs)
       Last-byte  latency:  0.40 -   2.80    1.60  (  1.20)    1.60  (medium objs)
       First-byte latency:  0.10 -   0.20    0.15  (  0.05)    0.15  (large objs)
       Last-byte  latency:  0.30 -   0.40    0.35  (  0.05)    0.35  (large objs)
       First-byte latency:  1.20 -   1.20    1.20  (  0.00)    1.20  (huge objs)
       Last-byte  latency:  2.20 -   2.20    2.20  (  0.00)    2.20  (huge objs)

CREATE
       Count:     3  Average requests per second:   0.5
                           min      max     avg     std_dev   median
       First-byte latency:  0.10 -   1.20    0.77  (  0.48)    1.00  (  all obj sizes)
       Last-byte  latency:  0.20 -   3.00    1.80  (  1.18)    2.20  (  all obj sizes)
       First-byte latency:  0.10 -   0.10    0.10  (  0.00)    0.10  (tiny objs)
       Last-byte  latency:  0.20 -   0.20    0.20  (  0.00)    0.20  (tiny objs)
       First-byte latency:  1.00 -   1.00    1.00  (  0.00)    1.00  (small objs)
       Last-byte  latency:  3.00 -   3.00    3.00  (  0.00)    3.00  (small objs)
       First-byte latency:  1.20 -   1.20    1.20  (  0.00)    1.20  (huge objs)
       Last-byte  latency:  2.20 -   2.20    2.20  (  0.00)    2.20  (huge objs)

READ
       Count:     4  Average requests per second:   1.0
                           min      max     avg     std_dev   median
       First-byte latency:  0.10 -   1.00    0.40  (  0.35)    0.25  (  all obj sizes)
       Last-byte  latency:  0.40 -   1.80    0.88  (  0.55)    0.65  (  all obj sizes)
       First-byte latency:  0.10 -   1.00    0.55  (  0.45)    0.55  (tiny objs)
       Last-byte  latency:  0.80 -   1.80    1.30  (  0.50)    1.30  (tiny objs)
       First-byte latency:  0.20 -   0.20    0.20  (  0.00)    0.20  (small objs)
       Last-byte  latency:  0.50 -   0.50    0.50  (  0.00)    0.50  (small objs)
       First-byte latency:  0.30 -   0.30    0.30  (  0.00)    0.30  (medium objs)
       Last-byte  latency:  0.40 -   0.40    0.40  (  0.00)    0.40  (medium objs)

UPDATE
       Count:     3  Average requests per second:   0.5
                           min      max     avg     std_dev   median
       First-byte latency:  0.20 -   0.80    0.53  (  0.25)    0.60  (  all obj sizes)
       Last-byte  latency:  0.30 -   2.80    1.27  (  1.10)    0.70  (  all obj sizes)
       First-byte latency:  0.60 -   0.60    0.60  (  0.00)    0.60  (tiny objs)
       Last-byte  latency:  0.70 -   0.70    0.70  (  0.00)    0.70  (tiny objs)
       First-byte latency:  0.80 -   0.80    0.80  (  0.00)    0.80  (medium objs)
       Last-byte  latency:  2.80 -   2.80    2.80  (  0.00)    2.80  (medium objs)
       First-byte latency:  0.20 -   0.20    0.20  (  0.00)    0.20  (large objs)
       Last-byte  latency:  0.30 -   0.30    0.30  (  0.00)    0.30  (large objs)

DELETE
       Count:     2  Average requests per second:   2.0
                           min      max     avg     std_dev   median
       First-byte latency:  0.10 -   0.50    0.30  (  0.20)    0.30  (  all obj sizes)
       Last-byte  latency:  0.40 -   0.80    0.60  (  0.20)    0.60  (  all obj sizes)
       First-byte latency:  0.50 -   0.50    0.50  (  0.00)    0.50  (small objs)
       Last-byte  latency:  0.80 -   0.80    0.80  (  0.00)    0.80  (small objs)
       First-byte latency:  0.10 -   0.10    0.10  (  0.00)    0.10  (large objs)
       Last-byte  latency:  0.40 -   0.40    0.40  (  0.00)    0.40  (large objs)


""".split('\n'), self.master.generate_scenario_report(self.scenario, scen_stats).split('\n'))
