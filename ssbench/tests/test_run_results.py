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
import time
import shutil
import msgpack
import tempfile
import subprocess
from nose.tools import *

from ssbench.scenario import Scenario
from ssbench.run_results import RunResults

from ssbench.tests.test_scenario import ScenarioFixture


class TestRunResults(ScenarioFixture):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.result_file_path = os.path.join(self.temp_dir, 'some_name.dat')
        self.stub_scenario_file = os.path.join(self.temp_dir,
                                               'some_scenario.json')
        self.run_results = RunResults(self.result_file_path)
        super(TestRunResults, self).setUp()

    def tearDown(self):
        super(TestRunResults, self).tearDown()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _current_size(self):
        if not self.run_results.output_file.closed:
            self.run_results.output_file.flush()
        return os.path.getsize(self.result_file_path)

    def test_basic_instantiation(self):
        # Just instantiating the object shouldn't create anything
        assert_false(os.path.exists(self.result_file_path))
        assert_equal(1000000, self.run_results.write_threshold)  # 1 MB

        # A file path is required, though
        with assert_raises(TypeError):
            RunResults()

    def test_start_run_requires_scenario(self):
        with assert_raises(TypeError):
            self.run_results.start_run()

    def test_start_run(self):
        # Just instantiating the object shouldn't create anything
        assert_false(os.path.exists(self.result_file_path))

        self.run_results.start_run(self.scenario)

        # start_run opens the file for writing and dumps out the scenario.
        assert_equal(len(self.scenario.packb()), self._current_size())

        # This is a bit white-box, but that's what we're here for...
        assert_greater(self._current_size(), 0)

        with open(self.result_file_path, 'rb') as f:
            unpacker = msgpack.Unpacker(file_like=f)
            got_scenario = Scenario.unpackb(unpacker)
            for attr in ['name', '_scenario_data', 'user_count',
                         'operation_count', 'run_seconds', 'container_base',
                         'container_count', 'containers',
                         'container_concurrency', 'sizes_by_name', 'version',
                         'bench_size_thresholds']:
                assert_equal(getattr(got_scenario, attr),
                             getattr(self.scenario, attr))

    def test_process_raw_results(self):
        self.run_results.start_run(self.scenario)
        start_size = self._current_size()
        assert_greater(start_size, 0)  # sanity check

        self.run_results.write_threshold = 1024

        res1 = msgpack.packb([{'one': 1.0}])
        res2 = msgpack.packb([{'two-1': 2.1}, {'two-2': 2.2}])
        self.run_results.process_raw_results(res1)
        self.run_results.process_raw_results(res2)

        # Still nothing flushed to disk yet (those two sets of messages
        # shouldn't be over the threshold of 1024 bytesnot over threshold)
        assert_equal(self._current_size() - start_size, 0)

        res3 = msgpack.packb([{'three': '3' * 1025}])
        assert_greater(len(res3), 1024)
        self.run_results.process_raw_results(res3)
        time.sleep(0.01)  # give the writer thread time to write()

        # Now we should have written out the buffer (in the writer thread).
        assert_greater(self._current_size() - start_size, 1024)

    def test_finalize(self):
        self.run_results.start_run(self.scenario)
        start_size = self._current_size()
        assert_greater(start_size, 0)  # sanity check

        self.run_results.write_threshold = 1024

        res1 = msgpack.packb([{'three': '3' * 1025}])
        self.run_results.process_raw_results(res1)
        time.sleep(0.01)  # give the writer thread time to write()

        # Now we should have written out the buffer (in the writer thread).
        new_size = self._current_size()
        assert_greater(new_size - start_size, 1024)

        # Send down a couple small ones, then finalize.  They should have
        # been written by the time finalize returns.
        res2 = msgpack.packb([{'two-1': 2.1}, {'two-2': 2.2}])
        res3 = msgpack.packb([{'three': '3'}])
        self.run_results.process_raw_results(res2)
        self.run_results.process_raw_results(res3)

        self.run_results.finalize()
        assert_equal(len(res2) + len(res3),
                     self._current_size() - new_size)

    def test_read_results(self):
        self.run_results.start_run(self.scenario)
        res1 = msgpack.packb([{'three': '3' * 1025}])
        res2 = msgpack.packb([{'two-1': 2.1}, {'two-2': 2.2}])
        res3 = msgpack.packb([{'three': '3'}])
        self.run_results.process_raw_results(res1)
        self.run_results.process_raw_results(res2)
        self.run_results.process_raw_results(res3)
        self.run_results.finalize()

        got_scenario, unpacker = RunResults(
            self.result_file_path).read_results()

        for attr in ['name', '_scenario_data', 'user_count', 'operation_count',
                     'run_seconds', 'container_base', 'container_count',
                     'containers', 'container_concurrency', 'sizes_by_name',
                     'version', 'bench_size_thresholds']:
            assert_equal(getattr(got_scenario, attr),
                         getattr(self.scenario, attr))

        assert_equal(list(unpacker), [
            [{'three': '3' * 1025}],
            [{'two-1': 2.1}, {'two-2': 2.2}],
            [{'three': '3'}],
        ])

        subprocess.check_call(['gzip', self.result_file_path])

        got_scenario, unpacker = RunResults(
            self.result_file_path + '.gz').read_results()

        for attr in ['name', '_scenario_data', 'user_count', 'operation_count',
                     'run_seconds', 'container_base', 'container_count',
                     'containers', 'container_concurrency', 'sizes_by_name',
                     'version', 'bench_size_thresholds']:
            assert_equal(getattr(got_scenario, attr),
                         getattr(self.scenario, attr))

        assert_equal(list(unpacker), [
            [{'three': '3' * 1025}],
            [{'two-1': 2.1}, {'two-2': 2.2}],
            [{'three': '3'}],
        ])
