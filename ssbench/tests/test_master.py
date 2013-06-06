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

from unittest import TestCase
from flexmock import flexmock
from gevent_zeromq import zmq

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

        self.mock_work_push = flexmock()
        self.mock_context.should_receive('socket').with_args(
            zmq.PUSH,
        ).and_return(self.mock_work_push).once
        self.mock_work_push.should_receive('bind').with_args(
            self.work_endpoint,
        ).once

        self.mock_results_pull = flexmock()
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

    def tearDown(self):
        super(TestMaster, self).tearDown()
