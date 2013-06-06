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

import os
import json
import time
import signal
import msgpack
from cStringIO import StringIO
from nose.tools import *
from exceptions import OSError
from collections import Counter

import ssbench
from ssbench.scenario import Scenario, ScenarioNoop


class ScenarioFixture(object):
    def setUp(self):
        superclass = super(ScenarioFixture, self)
        if hasattr(superclass, 'setUp'):
            superclass.setUp()

        if not hasattr(self, 'stub_scenario_file'):
            self.stub_scenario_file = '/tmp/.430gjf.test_scenario.py'

        if not getattr(self, 'scenario_dict', None):
            self.scenario_dict = dict(
                name='Test1 - Happy go lucky',
                sizes=[
                    dict(name='tiny', size_min=99, size_max=100),
                    dict(name='small', size_min=199, size_max=200,
                         crud_profile=[73, 12, 5, 10]),
                    dict(name='medium', size_min=299, size_max=300),
                    dict(name='red herring', size_min=9999, size_max=9999),
                    dict(name='large', size_min=399, size_max=400,
                         crud_profile=[13, 17, 19, 51])],
                initial_files=dict(
                    tiny=700, small=400, medium=200, large=100),
                # From first POC input, all file size percentages can be derived
                # directly from the distribution of initial files.  So we take that
                # shortcut in the definition of scenarios.
                operation_count=20000,
                #             C  R  U  D
                crud_profile=[10, 7, 4, 1],  # maybe make this a dict?
                user_count=1,
            )
        self.write_scenario_file()
        self.scenario = Scenario(self.stub_scenario_file)
        self.scenario_noop = ScenarioNoop(self.stub_scenario_file)

    def tearDown(self):
        try:
            os.unlink(self.stub_scenario_file)
        except OSError:
            pass  # don't care if it didn't get created
        superclass = super(ScenarioFixture, self)
        if hasattr(superclass, 'tearDown'):
            superclass.tearDown()

    def write_scenario_file(self):
        """Generates a scenario file on disk (in /tmp).

        The tearDown() method will delete the created file.  Note that
        only one scenario file created by this method can exist at any
        time (a static path is reused).  Change this behavior if needed.

        :**contents: Contents of the JSON object which is the scenario data.
        :returns: (nothing)
        """

        fp = open(self.stub_scenario_file, 'w')
        json.dump(self.scenario_dict, fp)


class TestScenario(ScenarioFixture):
    def setUp(self):
        super(TestScenario, self).setUp()

    def tearDown(self):
        super(TestScenario, self).tearDown()

    def test_basic_instantiation(self):
        # very whitebox:
        assert_dict_equal(self.scenario_dict, self.scenario._scenario_data)
        assert_equal(ssbench.version, self.scenario.version)

    def test_packb_unpackb(self):
        packed = self.scenario.packb()
        assert_is_instance(packed, bytes)
        unpacked = Scenario.unpackb(packed)
        assert_is_instance(unpacked, Scenario)
        for attr in ['name', '_scenario_data', 'user_count', 'operation_count',
                     'run_seconds', 'container_base', 'container_count',
                     'containers', 'container_concurrency', 'sizes_by_name',
                     'version', 'bench_size_thresholds']:
            assert_equal(getattr(unpacked, attr), getattr(self.scenario, attr))

    def test_packb_unpackb_with_run_seconds(self):
        self.scenario_dict['run_seconds'] = 27
        self.write_scenario_file()
        scenario = Scenario(self.stub_scenario_file, version='0.1.1')
        assert_equal(27, scenario.run_seconds)
        assert_equal(None, scenario.operation_count)
        packed = scenario.packb()
        assert_is_instance(packed, bytes)
        unpacked = Scenario.unpackb(packed)
        assert_is_instance(unpacked, Scenario)
        for attr in ['name', '_scenario_data', 'user_count', 'operation_count',
                     'run_seconds', 'container_base', 'container_count',
                     'containers', 'container_concurrency', 'sizes_by_name',
                     'version', 'bench_size_thresholds']:
            assert_equal(getattr(unpacked, attr), getattr(scenario, attr))

        scenario = Scenario(self.stub_scenario_file, run_seconds=88,
                            operation_count=99)
        assert_equal(88, scenario.run_seconds)
        assert_equal(None, scenario.operation_count)
        packed = scenario.packb()
        assert_is_instance(packed, bytes)
        unpacked = Scenario.unpackb(packed)
        assert_is_instance(unpacked, Scenario)
        for attr in ['name', '_scenario_data', 'user_count', 'operation_count',
                     'run_seconds', 'container_base', 'container_count',
                     'containers', 'container_concurrency', 'sizes_by_name',
                     'version', 'bench_size_thresholds']:
            assert_equal(getattr(unpacked, attr), getattr(scenario, attr))

    def test_unpackb_given_unpacker(self):
        packed = self.scenario.packb()
        assert_is_instance(packed, bytes)
        file_like = StringIO(packed + msgpack.packb({'red': 'herring'}))
        unpacker = msgpack.Unpacker(file_like=file_like)
        unpacked = Scenario.unpackb(unpacker)
        assert_is_instance(unpacked, Scenario)
        for attr in ['name', '_scenario_data', 'user_count', 'operation_count',
                     'run_seconds', 'container_base', 'container_count',
                     'containers', 'container_concurrency', 'sizes_by_name',
                     'version', 'bench_size_thresholds']:
            assert_equal(getattr(unpacked, attr), getattr(self.scenario, attr))

    def test_open_fails(self):
        with assert_raises(IOError):
            # It also logs, but I'm too lazy to test that
            Scenario('some file which will not be present!')

    def test_no_filename_or__scenario_data(self):
        with assert_raises(ValueError):
            self.write_scenario_file()
            Scenario()

    def test_no_op_count_or_run_seconds(self):
        with assert_raises(ValueError):
            self.scenario_dict.pop('run_seconds', None)
            self.scenario_dict.pop('operation_count', None)
            self.write_scenario_file()
            Scenario(self.stub_scenario_file)

    def test_constructor_overrides(self):
        scenario = Scenario(self.stub_scenario_file, container_count=21,
                            user_count=37, operation_count=101)
        assert_equal(21, len(scenario.containers))
        assert_equal(37, scenario.user_count)
        assert_equal(101, scenario.operation_count)

    def test_invalid_user_count(self):
        self.scenario_dict['user_count'] = -1
        self.write_scenario_file()
        with assert_raises(ValueError):
            Scenario(self.stub_scenario_file)
        with assert_raises(ValueError):
            Scenario(self.stub_scenario_file, user_count=0)

    def test_containers_default(self):
        assert_list_equal(self.scenario.containers,
                          ['ssbench_%06d' % i for i in xrange(100)])
        assert_equal(10, self.scenario.container_concurrency)

    def test_containers_custom(self):
        self.scenario_dict['container_base'] = 'iggy'
        self.scenario_dict['container_count'] = 77
        self.scenario_dict['container_concurrency'] = 13
        self.write_scenario_file()
        scenario = Scenario(self.stub_scenario_file)
        assert_list_equal(scenario.containers,
                          ['iggy_%06d' % i for i in xrange(77)])
        assert_equal(13, scenario.container_concurrency)

    def test_crud_pcts(self):
        assert_list_equal([10.0 / 22 * 100,
                           7.0 / 22 * 100,
                           4.0 / 22 * 100,
                           1.0 / 22 * 100], self.scenario.crud_pcts)

    def test_bench_jobs(self):
        jobs = list(self.scenario.bench_jobs())

        # count should equal the file_count (20000)
        assert_equal(20000, len(jobs))

        for job in jobs:
            assert_not_in('noop', job)

        err_pct = 0.15   # Expect +/- 15% for size/CRUD distribution

        # Expect count of sizes to be +/- 10% of expected proportions (which are
        # derived from the initial counts; 30%, 30%, 30%, 10% in this case)
        size_counter = Counter([_['size_str'] for _ in jobs])
        assert_almost_equal(700.0 / 1400 * 20000, size_counter['tiny'],
                            delta=err_pct * 700.0 / 1400 * 20000)
        assert_almost_equal(400.0 / 1400 * 20000, size_counter['small'],
                            delta=err_pct * 400.0 / 1400 * 20000)
        assert_almost_equal(200.0 / 1400 * 20000, size_counter['medium'],
                            delta=err_pct * 200.0 / 1400 * 20000)
        assert_almost_equal(100.0 / 1400 * 20000, size_counter['large'],
                            delta=err_pct * 100.0 / 1400 * 20000)
        assert_not_in('huge', size_counter)

        # CRUD profiles (can be) per-size, so analyze them that way.
        # tiny CRUD profile inherits "top-level" [10, 7, 4, 1]
        tiny_job_types = [j['type'] for j in jobs if j['size_str'] == 'tiny']
        tiny_counter = Counter(tiny_job_types)
        jcount = len(tiny_job_types)
        assert_almost_equal(10 / 22.0 * jcount,
                            tiny_counter[ssbench.CREATE_OBJECT],
                            delta=err_pct * 10 / 22.0 * jcount)
        assert_almost_equal(7 / 22.0 * jcount,
                            tiny_counter[ssbench.READ_OBJECT],
                            delta=err_pct * 7 / 22.0 * jcount)
        assert_almost_equal(4 / 22.0 * jcount,
                            tiny_counter[ssbench.UPDATE_OBJECT],
                            delta=err_pct * 4 / 22.0 * jcount)
        assert_almost_equal(1 / 22.0 * jcount,
                            tiny_counter[ssbench.DELETE_OBJECT],
                            delta=err_pct * 1 / 22.0 * jcount)

        # CRUD profiles (can be) per-size, so analyze them that way.
        # small CRUD profile is [73, 12, 5, 10]
        small_job_types = [j['type'] for j in jobs if j['size_str'] == 'small']
        small_counter = Counter(small_job_types)
        jcount = len(small_job_types)
        assert_almost_equal(0.73 * jcount,
                            small_counter[ssbench.CREATE_OBJECT],
                            delta=err_pct * 0.73 * jcount)
        assert_almost_equal(0.12 * jcount,
                            small_counter[ssbench.READ_OBJECT],
                            delta=err_pct * 0.12 * jcount)
        assert_almost_equal(0.05 * jcount,
                            small_counter[ssbench.UPDATE_OBJECT],
                            delta=err_pct * 0.05 * jcount)
        assert_almost_equal(0.1 * jcount, small_counter[ssbench.DELETE_OBJECT],
                            delta=err_pct * 0.1 * jcount)

        # CRUD profiles (can be) per-size, so analyze them that way.
        # medium CRUD profile inherits "top-level" [10, 7, 4, 1]
        medium_job_types = [j['type'] for j in jobs if j[
            'size_str'] == 'medium']
        medium_counter = Counter(medium_job_types)
        jcount = len(medium_job_types)
        assert_almost_equal(10 / 22.0 * jcount,
                            medium_counter[ssbench.CREATE_OBJECT],
                            delta=err_pct * 10 / 22.0 * jcount)
        assert_almost_equal(7 / 22.0 * jcount,
                            medium_counter[ssbench.READ_OBJECT],
                            delta=err_pct * 7 / 22.0 * jcount)
        assert_almost_equal(4 / 22.0 * jcount,
                            medium_counter[ssbench.UPDATE_OBJECT],
                            delta=err_pct * 4 / 22.0 * jcount)
        assert_almost_equal(1 / 22.0 * jcount,
                            medium_counter[ssbench.DELETE_OBJECT],
                            delta=err_pct * 1 / 22.0 * jcount)

        # CRUD profiles (can be) per-size, so analyze them that way.
        # large CRUD profile is [13, 17, 19, 51]
        large_job_types = [j['type'] for j in jobs if j['size_str'] == 'large']
        large_counter = Counter(large_job_types)
        jcount = len(large_job_types)
        assert_almost_equal(0.13 * jcount,
                            large_counter[ssbench.CREATE_OBJECT],
                            delta=err_pct * 0.13 * jcount)
        assert_almost_equal(0.17 * jcount,
                            large_counter[ssbench.READ_OBJECT],
                            delta=err_pct * 0.17 * jcount)
        assert_almost_equal(0.19 * jcount,
                            large_counter[ssbench.UPDATE_OBJECT],
                            delta=err_pct * 0.19 * jcount)
        assert_almost_equal(0.51 * jcount,
                            large_counter[ssbench.DELETE_OBJECT],
                            delta=err_pct * 0.51 * jcount)

    def test_bench_jobs_noop(self):
        jobs = list(self.scenario_noop.bench_jobs())

        # count should equal the file_count (20000)
        assert_equal(20000, len(jobs))

        for job in jobs:
            assert_true(job['noop'])

    def test_bench_jobs_with_run_seconds(self):
        initial_handler = lambda s, f: 17
        signal.signal(signal.SIGALRM, initial_handler)

        self.scenario_dict['operation_count'] = 1
        self.scenario_dict['run_seconds'] = 1
        self.write_scenario_file()
        scenario = Scenario(self.stub_scenario_file)

        start_time = time.time()
        jobs = list(scenario.bench_jobs())
        delta_t = time.time() - start_time

        # Count should be greater than 1, for sure...
        assert_greater(len(jobs), 1)
        # +/- 10ms seems good:
        assert_almost_equal(delta_t, scenario.run_seconds, delta=0.01)

        restored_handler = signal.signal(signal.SIGALRM, signal.SIG_DFL)
        assert_equal(restored_handler, initial_handler)

    def test_bench_job_0(self):
        bench_job = self.scenario.bench_job('small', 0, 31)
        assert_in(bench_job['container'], self.scenario.containers)
        assert_equal('small_000031', bench_job['name'])
        assert_in(bench_job['size'], [199, 200])
        assert_equal(ssbench.CREATE_OBJECT, bench_job['type'])

    def test_bench_job_1(self):
        bench_job = self.scenario.bench_job('large', 1, 492)
        assert_dict_equal(dict(
            type=ssbench.READ_OBJECT,
            size_str='large',
        ), bench_job)

    def test_bench_job_2(self):
        bench_job = self.scenario.bench_job('tiny', 2, 9329)
        assert_in(bench_job.pop('size'), [99, 100])
        assert_dict_equal(dict(
            type=ssbench.UPDATE_OBJECT,
            size_str='tiny',
        ), bench_job)

    def test_bench_job_3(self):
        bench_job = self.scenario.bench_job('huge', 3, 30230)
        assert_dict_equal(dict(
            type=ssbench.DELETE_OBJECT,
            size_str='huge',
        ), bench_job)

    def test_initial_jobs(self):
        jobs = list(self.scenario.initial_jobs())

        # count should equal initial files (1000)
        assert_equal(sum(self.scenario_dict['initial_files'].values()),
                     len(jobs))

        # no need to be clever with these, the implementation will just stripe
        # across the sizes; we'll spot-check some here.
        assert_in(jobs[0].pop('container'), self.scenario.containers)
        assert_in(jobs[0].pop('size'), [99, 100])
        assert_dict_equal({
            # Note that the scenario stays out of the business of which cluster
            # you're using and authentication tokens.
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'tiny',
            'name': 'tiny_000001',
        }, jobs[0])
        assert_in(jobs[1].pop('container'), self.scenario.containers)
        assert_in(jobs[1].pop('size'), [199, 200])
        assert_dict_equal({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'small',
            'name': 'small_000001',
        }, jobs[1])
        assert_in(jobs[2].pop('container'), self.scenario.containers)
        assert_in(jobs[2].pop('size'), [299, 300])
        assert_dict_equal({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'medium',
            'name': 'medium_000001',
        }, jobs[2])
        assert_in(jobs[3].pop('container'), self.scenario.containers)
        assert_in(jobs[3].pop('size'), [399, 400])
        assert_dict_equal({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'large',
            'name': 'large_000001',
        }, jobs[3])
        # This scenario called for no initial "huge" files, so we wrapped back
        # to tiny (#2)
        assert_in(jobs[4].pop('container'), self.scenario.containers)
        assert_in(jobs[4].pop('size'), [99, 100])
        assert_dict_equal({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'tiny',
            'name': 'tiny_000002',  # <Usage><Type>######
        }, jobs[4])

        size_counter = Counter([_['size_str'] for _ in jobs])
        assert_equal(700, size_counter['tiny'])
        assert_equal(400, size_counter['small'])
        assert_equal(200, size_counter['medium'])
        assert_equal(100, size_counter['large'])
