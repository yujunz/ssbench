from unittest import TestCase
from flexmock import flexmock
import yaml
from pprint import pprint
from statlib import stats

from ssbench.constants import *
from ssbench.scenario import Scenario
from ssbench.scenario_file import ScenarioFile
from ssbench.master import Master
import ssbench.master

from ssbench.tests.test_scenario import ScenarioFixture

class TestMaster(ScenarioFixture, TestCase):
    def setUp(self):
        self.stub_queue = flexmock()
        self.stub_queue.should_receive('watch').with_args(STATS_TUBE).once
        self.stub_queue.should_receive('ignore').with_args(DEFAULT_TUBE).once
        self.master = flexmock(Master(self.stub_queue))

        self.result_index = 1

        # Set our test scenario differently from the default
        super(TestMaster, self).setUp()
        self.scenario_dict = dict(
            name='Master Test Scenario - ablkei',
            initial_files=dict(
                tiny=300, small=300, medium=300, large=100, huge=70,
            ),
            # From first POC input, all file size percentages can be derived
            # directly from the distribution of initial files.  So we take that
            # shortcut in the definition of scenarios.
            file_count=5000,
            #             C  R  U  D
            crud_profile=[5, 3, 1, 1], # maybe make this a dict?
            user_count=2,
        )

    def gen_result(self, worker_id, operation_type, size_str, start, \
                   first_byte, last_byte):
        scenario_file = ScenarioFile('S', size_str, self.result_index)
        self.result_index += 1

        return {
            # There are other keys in a "result", but these are the only ones
            # used for the reporting.
            'worker_id': worker_id,
            'type': operation_type,
            'object_size': scenario_file.size,
            'first_byte_latency': first_byte - start,
            'last_byte_latency': last_byte - start,
            'completed_at': last_byte,
        }



    def tearDown(self):
        super(TestMaster, self).tearDown()

    def test_calculate_scenario_stats(self):
        self.write_scenario_file(**self.scenario_dict)
        scenario = Scenario(self.stub_scenario_file)
        stub_results = [
            self.gen_result(1, CREATE_OBJECT, 'small', 100.0, 101.0, 103.0),
            self.gen_result(1, READ_OBJECT, 'small', 103.0, 103.1, 103.8),
            self.gen_result(1, CREATE_OBJECT, 'small', 103.8, 105.0, 106.0),
            self.gen_result(1, UPDATE_OBJECT, 'small', 106.1, 106.3, 106.4),
            #
            self.gen_result(2, UPDATE_OBJECT, 'small', 100.1, 100.9, 102.9),
            self.gen_result(2, DELETE_OBJECT, 'small', 102.9, 103.0, 103.3),
            self.gen_result(2, CREATE_OBJECT, 'small', 103.3, 103.4, 103.5),
            self.gen_result(2, READ_OBJECT, 'small', 103.5, 103.7, 104.0),
            #
            self.gen_result(3, READ_OBJECT, 'small', 100.1, 101.1, 101.9),
            # worker 3 took a while (observer lower concurrency in second 102
            self.gen_result(3, DELETE_OBJECT, 'small', 103.1, 103.6, 103.9),
            self.gen_result(3, READ_OBJECT, 'small', 103.9, 104.2, 104.3),
            self.gen_result(3, UPDATE_OBJECT, 'small', 104.3, 104.9, 104.999),
        ]
        first_byte_latency_all = [1, 0.1, 1.2, 0.2, 0.8, 0.1, 0.1, 0.2, 1, 0.5, 0.3, 0.6]
        last_byte_latency_all = [3, 0.8, 2.2, 0.3, 2.8, 0.4, 0.2, 0.5, 1.8, 0.8, 0.4, 0.699]
        scen_stats = self.master.calculate_scenario_stats(stub_results)
        self.maxDiff = None
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
        # Stats for worker_id == 1
        w1_first_byte_latency = [1.0, 0.1, 1.2, 0.2]
        w1_last_byte_latency = [3.0, 0.8, 2.2, 0.3]
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
        # Stats for worker_id == 2
        w2_first_byte_latency = [0.8, 0.1, 0.1, 0.2]
        w2_last_byte_latency = [2.8, 0.4, 0.2, 0.5]
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
        # Stats for worker_id == 3
        w3_first_byte_latency = [1, 0.5, 0.3, 0.6]
        w3_last_byte_latency = [1.8, 0.8, 0.4, 0.699]
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
        # Stats for Create
        c_first_byte_latency = [1, 1.2, 0.1]
        c_last_byte_latency = [3, 2.2, 0.2]
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
        ), scen_stats['op_stats'][CREATE_OBJECT])
        # Stats for Read
        r_first_byte_latency = [0.1, 0.2, 1.0, 0.3]
        r_last_byte_latency = [0.8, 0.5, 1.8, 0.4]
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
        ), scen_stats['op_stats'][READ_OBJECT])
        # Stats for Update
        u_first_byte_latency = [0.2, 0.8, 0.6]
        u_last_byte_latency = [0.3, 2.8, 0.699]
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
        ), scen_stats['op_stats'][UPDATE_OBJECT])
        # Stats for Delete
        d_first_byte_latency = [0.1, 0.5]
        d_last_byte_latency = [0.4, 0.8]
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
        ), scen_stats['op_stats'][DELETE_OBJECT])

#            SERIES_STATS = {
#                'min': 1.1,
#                'max': 1.1,
#                'avg': 1.1,
#                'std_dev': 1.1,
#                'median': 1.1,
#            }
#                'agg_stats': {
#                    'worker_count': 1,
#                    'start': 1.1,
#                    'stop': 1.1,
#                    'req_count': 1,
#                    'avg_req_per_sec': 1.1, # req_count / (stop - start)?
#                    'first_byte_latency': SERIES_STATS,
#                    'last_byte_latency': SERIES_STATS,
#                },

