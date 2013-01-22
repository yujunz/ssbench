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
from exceptions import OSError
from collections import Counter
from nose.tools import *

import ssbench
from ssbench.scenario import Scenario

class ScenarioFixture(object):
    def setUp(self):
        superclass = super(ScenarioFixture, self)
        if hasattr(superclass, 'setUp'):
            superclass.setUp()
        self.stub_scenario_file = '/tmp/.430gjf.test_scenario.py'

        if not getattr(self, 'scenario_dict', None):
            self.scenario_dict = dict(
                name='Test1 - Happy go lucky',
                sizes=[
                    dict(name='tiny', size_min=99, size_max=100),
                    dict(name='small', size_min=199, size_max=200),
                    dict(name='medium', size_min=299, size_max=300),
                    dict(name='large', size_min=399, size_max=400)],
                initial_files=dict(
                    tiny=300, small=300, medium=300, large=100),
                # From first POC input, all file size percentages can be derived
                # directly from the distribution of initial files.  So we take that
                # shortcut in the definition of scenarios.
                operation_count=5000,
                #             C  R  U  D
                crud_profile=[6, 0, 0, 1], # maybe make this a dict?
                user_count=1,
            )
        self.write_scenario_file()
        self.scenario = Scenario(self.stub_scenario_file)
 
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

    def test_crud_pcts(self):
        assert_list_equal([6.0/7*100,0.0,0.0,1.0/7*100],
                          self.scenario.crud_pcts)

    def test_bench_jobs(self):
        jobs = list(self.scenario.bench_jobs())

        # count should equal the file_count (5000)
        assert_equal(5000, len(jobs))

        # Expect count of sizes to be +/- 10% of expected proportions (which are
        # derived from the initial counts; 30%, 30%, 30%, 10% in this case)
        size_counter = Counter([_['size_str'] for _ in jobs])
        assert_almost_equal(1500, size_counter['tiny'], delta=0.10*1500)
        assert_almost_equal(1500, size_counter['small'], delta=0.10*1500)
        assert_almost_equal(1500, size_counter['medium'], delta=0.10*1500)
        assert_almost_equal(500, size_counter['large'], delta=0.10*500)
        assert_not_in('huge', size_counter)

        # From the CRUD profile, we should have 85.7% Create (6/7), and 14.3%
        # Delete (1/7).
        type_counter = Counter([_['type'] for _ in jobs])
        assert_almost_equal(6 * 5000 / 7, type_counter[ssbench.CREATE_OBJECT],
                            delta=0.10*6*5000/7)
        assert_almost_equal(5000 / 7, type_counter[ssbench.DELETE_OBJECT],
                            delta=0.10*5000/7)

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
        assert_equal(1000, len(jobs))

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
            'name': 'tiny_000002', # <Usage><Type>######
        }, jobs[4])

        size_counter = Counter([_['size_str'] for _ in jobs])
        assert_equal(300, size_counter['tiny'])
        assert_equal(300, size_counter['small'])
        assert_equal(300, size_counter['medium'])
        assert_equal(100, size_counter['large'])

