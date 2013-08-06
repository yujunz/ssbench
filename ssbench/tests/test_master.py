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

import sys
import textwrap
import tempfile
import StringIO
from unittest import TestCase
from flexmock import flexmock
from gevent_zeromq import zmq

import msgpack

from ssbench.master import Master
from ssbench.run_results import RunResults
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
                dict(name='small', size_min=1990, size_max=1990,
                     crud_profile=[71, 9, 12, 8]),
                dict(name='medium', size_min=2990, size_max=3000),
                dict(name='unused', size_min=9876543, size_max=9876543),
                dict(name='large', size_min=399000, size_max=400000,
                     crud_profile=[16, 61, 7, 16]),
                dict(name='huge', size_min=49900000, size_max=71499999)],
            initial_files=dict(
                tiny=300, small=400, medium=500, large=200, huge=70,
            ),
            operation_count=5000,
            #             C  R  U  D
            crud_profile=[5, 3, 1, 1],
            user_count=2,
        )
        super(TestMaster, self).setUp()

        self.zmq_host = 'slick.queue.com'
        self.zmq_work_port = 7482
        self.zmq_results_port = 18398
        self.work_endpoint = 'tcp://%s:%d' % (self.zmq_host,
                                              self.zmq_work_port)
        self.results_endpoint = 'tcp://%s:%d' % (self.zmq_host,
                                                 self.zmq_results_port)

        self.mock_context = flexmock()
        flexmock(zmq.Context).new_instances(self.mock_context).once

        self.mock_work_push = flexmock(send=self._send)
        self.mock_context.should_receive('socket').with_args(
            zmq.PUSH,
        ).and_return(self.mock_work_push).once
        self.mock_work_push.should_receive('bind').with_args(
            self.work_endpoint,
        ).once

        self.mock_results_pull = flexmock(recv=self._recv)
        self.mock_context.should_receive('socket').with_args(
            zmq.PULL,
        ).and_return(self.mock_results_pull).once
        self.mock_results_pull.should_receive('bind').with_args(
            self.results_endpoint,
        ).once

        self.master = Master(self.zmq_host, self.zmq_work_port,
                             self.zmq_results_port,
                             connect_timeout=3.14159,
                             network_timeout=2.71828)

        self._send_calls = []
        self._recv_returns = []

    def tearDown(self):
        super(TestMaster, self).tearDown()

    def _send(self, data):
        self._send_calls.append(data)

    def _recv(self):
        value = self._recv_returns.pop(0)
        return value

    def assert_bench_output(self, output, expected):
        expected_stderr = '''\
        Benchmark Run:
          X    work job raised an exception
          .  <  1s first-byte-latency
          o  <  3s first-byte-latency
          O  < 10s first-byte-latency
          * >= 10s first-byte-latency
          _  <  1s last-byte-latency  (CREATE or UPDATE)
          |  <  3s last-byte-latency  (CREATE or UPDATE)
          ^  < 10s last-byte-latency  (CREATE or UPDATE)
          @ >= 10s last-byte-latency  (CREATE or UPDATE)
        '''
        expected_stderr = textwrap.dedent(expected_stderr)
        expected_stderr += expected + '\n'
        self.assertEqual(output, expected_stderr)

    def test_run_scenario_with_noop(self):
        bench_jobs = list(self.scenario.bench_jobs())

        job_result = dict(
            type='type',
            container='container',
            name='john.smith',
            first_byte_latency=0,
        )
        recvs = [[job_result] for _ in range(len(bench_jobs))]
        self._recv_returns = map(msgpack.dumps, recvs)

        process_raw_results_calls = []

        def mock_process_raw_results(raw_results):
            process_raw_results_calls.append(raw_results)

        # create a mock run result object
        temp_file = tempfile.NamedTemporaryFile()
        mock_run_results = flexmock(RunResults(temp_file.name))
        mock_run_results \
            .should_receive('process_raw_results') \
            .replace_with(mock_process_raw_results) \
            .times(len(bench_jobs))

        ori_stderr = sys.stderr
        stderr = StringIO.StringIO()
        sys.stderr = stderr
        try:
            self.master.run_scenario(self.scenario, auth_kwargs={},
                                     noop=True, run_results=mock_run_results)
            sys.stderr.flush()
        finally:
            sys.stderr = ori_stderr

        # make sure we get expected result in the RunResults
        parsed_calls = map(lambda d: msgpack.loads(d)[0], process_raw_results_calls)
        expected_results = [job_result] * len(bench_jobs)
        self.assertEqual(parsed_calls[0], expected_results[0])

    def test_run_scenario_output(self):
        bench_jobs = list(self.scenario.bench_jobs())

        def run_with_args(**kwargs):
            job_result = dict(
                type='type',
                container='container',
                name='john.smith',
            )
            job_result.update(kwargs)
            recvs = [[job_result] for _ in range(len(bench_jobs))]
            self._recv_returns = map(msgpack.dumps, recvs)

            ori_stderr = sys.stderr
            stderr = StringIO.StringIO()
            sys.stderr = stderr
            try:
                self.master.run_scenario(self.scenario, auth_kwargs={},
                                         noop=True, run_results=None)
                sys.stderr.flush()
            finally:
                sys.stderr = ori_stderr
            stderr_output = stderr.getvalue()
            return stderr_output

        # Test frist byte latency output
        first_byte_0s = run_with_args(first_byte_latency=0)
        self.assert_bench_output(first_byte_0s, '.' * len(bench_jobs))

        first_byte_lt3s = run_with_args(first_byte_latency=2)
        self.assert_bench_output(first_byte_lt3s, 'o' * len(bench_jobs))

        first_byte_lt10s = run_with_args(first_byte_latency=8)
        self.assert_bench_output(first_byte_lt10s, 'O' * len(bench_jobs))

        first_byte_ge10s = run_with_args(first_byte_latency=12)
        self.assert_bench_output(first_byte_ge10s, '*' * len(bench_jobs))

        # Test last byte latency output
        last_byte_0s = run_with_args(last_byte_latency=0)
        self.assert_bench_output(last_byte_0s, '_' * len(bench_jobs))

        last_byte_lt3s = run_with_args(last_byte_latency=2)
        self.assert_bench_output(last_byte_lt3s, '|' * len(bench_jobs))

        last_byte_lt10s = run_with_args(last_byte_latency=8)
        self.assert_bench_output(last_byte_lt10s, '^' * len(bench_jobs))

        last_byte_ge10s = run_with_args(last_byte_latency=12)
        self.assert_bench_output(last_byte_ge10s, '@' * len(bench_jobs))

        # Test exception output
        exception_output = run_with_args(exception=1)
        self.assert_bench_output(exception_output, 'X' * len(bench_jobs))
