from nose.tools import *

from ssbench.scenario_file import ScenarioFile

class TestScenarioFile(object):
    def test_initial_file_tiny(self):
        sfile = ScenarioFile('S', 'tiny', 72)
        # Test a bunch of read-only derived properties
        assert_equal('Picture', sfile.container)
        assert_equal('SP000072', sfile.name)
        assert_equal(99000, sfile.size)

    def test_initial_file_small(self):
        sfile = ScenarioFile('S', 'small', 9492)
        # Test a bunch of read-only derived properties
        assert_equal('Audio', sfile.container)
        assert_equal('SA009492', sfile.name)
        assert_equal(4900000, sfile.size)

    def test_initial_file_medium(self):
        sfile = ScenarioFile('S', 'medium', 2)
        # Test a bunch of read-only derived properties
        assert_equal('Document', sfile.container)
        assert_equal('SD000002', sfile.name)
        assert_equal(9900000, sfile.size)

    def test_initial_file_large(self):
        sfile = ScenarioFile('S', 'large', 492)
        # Test a bunch of read-only derived properties
        assert_equal('Video', sfile.container)
        assert_equal('SV000492', sfile.name)
        assert_equal(101000000, sfile.size)

    def test_initial_file_huge(self):
        sfile = ScenarioFile('S', 'huge', 23)
        # Test a bunch of read-only derived properties
        assert_equal('Application', sfile.container)
        assert_equal('SL000023', sfile.name)
        assert_equal(1100000000, sfile.size)

    def test_pop_file_tiny(self):
        sfile = ScenarioFile('P', 'tiny', 117)
        # Test a bunch of read-only derived properties
        assert_equal('Picture', sfile.container)
        assert_equal('PP000117', sfile.name)
        assert_equal(99000, sfile.size)

    def test_pop_file_small(self):
        sfile = ScenarioFile('P', 'small', 999999)
        # Test a bunch of read-only derived properties
        assert_equal('Audio', sfile.container)
        assert_equal('PA999999', sfile.name)
        assert_equal(4900000, sfile.size)

    def test_pop_file_medium(self):
        sfile = ScenarioFile('P', 'medium', 49)
        # Test a bunch of read-only derived properties
        assert_equal('Document', sfile.container)
        assert_equal('PD000049', sfile.name)
        assert_equal(9900000, sfile.size)

    def test_pop_file_large(self):
        sfile = ScenarioFile('P', 'large', 292)
        # Test a bunch of read-only derived properties
        assert_equal('Video', sfile.container)
        assert_equal('PV000292', sfile.name)
        assert_equal(101000000, sfile.size)

    def test_pop_file_huge(self):
        sfile = ScenarioFile('P', 'huge', 3049)
        # Test a bunch of read-only derived properties
        assert_equal('Application', sfile.container)
        assert_equal('PL003049', sfile.name)
        assert_equal(1100000000, sfile.size)
