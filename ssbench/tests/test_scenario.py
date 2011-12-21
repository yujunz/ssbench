import os
import json
from exceptions import OSError
from nose.tools import *
from collections import Counter

from ssbench.constants import *
from ssbench.scenario import Scenario

class ScenarioFixture(object):
    def setUp(self):
        superclass = super(ScenarioFixture, self)
        if hasattr(superclass, 'setUp'):
            superclass.setUp()
        self.stub_scenario_file = '/tmp/.430gjf.test_scenario.py'
        self.scenario_dict = dict(
            name='Test1 - Happy go lucky',
            initial_files=dict(
                tiny=300, small=300, medium=300, large=100, huge=0,
            ),
            # From first POC input, all file size percentages can be derived
            # directly from the distribution of initial files.  So we take that
            # shortcut in the definition of scenarios.
            file_count=5000,
            #             C  R  U  D
            crud_profile=[6, 0, 0, 1], # maybe make this a dict?
            user_count=1,
        )
 
    def tearDown(self):
        try:
            os.unlink(self.stub_scenario_file)
        except OSError:
            pass  # don't care if it didn't get created
        superclass = super(ScenarioFixture, self)
        if hasattr(superclass, 'tearDown'):
            superclass.tearDown()

    def write_scenario_file(self, **contents):
        """Generates a scenario file on disk (in /tmp).
        
        The tearDown() method will delete the created file.  Note that
        only one scenario file created by this method can exist at any
        time (a static path is reused).  Change this behavior if needed.
        
        :**contents: Contents of the JSON object which is the scenario data.
        :returns: (nothing)
        """
    
        fp = open(self.stub_scenario_file, 'w')
        json.dump(contents, fp)



class TestScenario(ScenarioFixture):
    def setUp(self):
        super(TestScenario, self).setUp()

    def tearDown(self):
        super(TestScenario, self).tearDown()

    def test_basic_instantiation(self):
        self.write_scenario_file(**self.scenario_dict)
        scenario = Scenario(self.stub_scenario_file)
        assert_dict_equal(self.scenario_dict, scenario._scenario_data) # very whitebox

    def test_bench_jobs(self):
        self.write_scenario_file(**self.scenario_dict)
        scenario = Scenario(self.stub_scenario_file)
        jobs = scenario.bench_jobs()

        # count should equal the file_count (5000)
        assert_equal(5000, len(jobs))

        # Expect count of sizes to be +/- 10% of expected proportions (which are
        # derived from the initial counts; 30%, 30%, 30%, 10% in this case)
        size_counter = Counter([_['container'] for _ in jobs])
        assert_almost_equal(1500, size_counter['Picture'], delta=0.10*1500)
        assert_almost_equal(1500, size_counter['Audio'], delta=0.10*1500)
        assert_almost_equal(1500, size_counter['Document'], delta=0.10*1500)
        assert_almost_equal(500, size_counter['Video'], delta=0.10*500)
        assert_equal(0, size_counter['Application'])

        # From the CRUD profile, we should have 85.7% Create (6/7), and 14.3%
        # Delete (1/7).
        type_counter = Counter([_['type'] for _ in jobs])
        assert_almost_equal(6 * 5000 / 7, type_counter[CREATE_OBJECT],
                            delta=0.10*6*5000/7)
        assert_almost_equal(5000 / 7, type_counter[DELETE_OBJECT],
                            delta=0.10*5000/7)

    def test_bench_job_0(self):
        self.write_scenario_file(**self.scenario_dict)
        scenario = Scenario(self.stub_scenario_file)

        bench_job = scenario.bench_job('small', 0, 31)
        assert_dict_equal(dict(
            type=CREATE_OBJECT,
            container='Audio',
            object_name='PA000031',
            object_size=4900000,
        ), bench_job)

    def test_bench_job_1(self):
        self.write_scenario_file(**self.scenario_dict)
        scenario = Scenario(self.stub_scenario_file)

        bench_job = scenario.bench_job('large', 1, 492)
        assert_dict_equal(dict(
            type=READ_OBJECT,
            container='Video',
            object_size=101000000,
        ), bench_job)

    def test_bench_job_2(self):
        self.write_scenario_file(**self.scenario_dict)
        scenario = Scenario(self.stub_scenario_file)

        bench_job = scenario.bench_job('tiny', 2, 9329)
        assert_dict_equal(dict(
            type=UPDATE_OBJECT,
            container='Picture',
            object_size=99000,
        ), bench_job)

    def test_bench_job_3(self):
        self.write_scenario_file(**self.scenario_dict)
        scenario = Scenario(self.stub_scenario_file)

        bench_job = scenario.bench_job('huge', 3, 30230)
        assert_dict_equal(dict(
            type=DELETE_OBJECT,
            container='Application',
            object_size=1100000000,
        ), bench_job)

    def test_initial_jobs(self):
        self.write_scenario_file(**self.scenario_dict)
        scenario = Scenario(self.stub_scenario_file)
        jobs = scenario.initial_jobs()

        # count should equal initial files (1000)
        assert_equal(1000, len(jobs))

        # no need to be clever with these, the implementation will just stripe
        # across the sizes; we'll spot-check some here.
        assert_dict_equal({
            # Note that the scenario stays out of the business of which cluster
            # you're using and authentication tokens.
            'type': CREATE_OBJECT,
            'container': 'Picture', # i.e. tiny
            'object_name': 'SP000001', # <Usage><Type>######
            'object_size': 99000, # tiny file size
        }, jobs[0])
        assert_dict_equal({
            'type': CREATE_OBJECT,
            'container': 'Audio', # i.e. small
            'object_name': 'SA000001', # <Usage><Type>######
            'object_size': 4900000, # small file size
        }, jobs[1])
        assert_dict_equal({
            'type': CREATE_OBJECT,
            'container': 'Document', # i.e. medium
            'object_name': 'SD000001', # <Usage><Type>######
            'object_size': 9900000, # medium file size
        }, jobs[2])
        assert_dict_equal({
            'type': CREATE_OBJECT,
            'container': 'Video', # i.e. tiny
            'object_name': 'SV000001', # <Usage><Type>######
            'object_size': 101000000, # tiny file size
        }, jobs[3])
        # This scenario called for no initial "huge" files, so we wrapped back
        # to tiny (#2)
        assert_dict_equal({
            'type': CREATE_OBJECT,
            'container': 'Picture', # i.e. tiny
            'object_name': 'SP000002', # <Usage><Type>######
            'object_size': 99000, # tiny file size
        }, jobs[4])

        size_counter = Counter([_['container'] for _ in jobs])
        assert_equal(300, size_counter['Picture'])
        assert_equal(300, size_counter['Audio'])
        assert_equal(300, size_counter['Document'])
        assert_equal(100, size_counter['Video'])

