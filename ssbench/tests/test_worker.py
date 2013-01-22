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

import time
import yaml
import socket
from argparse import Namespace
from collections import Counter
from flexmock import flexmock
from nose.tools import *

import ssbench
from ssbench import worker
from ssbench import swift_client as client

from ssbench.worker import beanstalkc


class TestWorker(object):
    def setUp(self):
        self.qhost = 'some.host'
        self.qport = 8530
        self.max_retries = 9
        self.concurrency = 12
        self.worker_id = 3

        self.mock_queue = flexmock()
        self.mock_connection = flexmock(beanstalkc.Connection)
        self.mock_connection.new_instances(self.mock_queue).with_args(
            beanstalkc.Connection, host=self.qhost, port=self.qport,
        ).times(1 + 12)
        # ^^--once for self.work_queue and <concurrecy> times by
        # self.result_queue_pool(?)
        self.mock_queue.should_receive('watch').with_args(
            ssbench.WORK_TUBE).once
        self.mock_queue.should_receive('use').with_args(
            ssbench.STATS_TUBE).times(12)

        self.worker = worker.Worker(self.qhost, self.qport, self.worker_id,
                                    self.max_retries, self.concurrency)
        self.mock_worker = flexmock(self.worker)

        self.stub_time = 98438243.3921
        self.time_expectation = flexmock(time).should_receive(
            'time'
        ).and_return(self.stub_time)

    def test_handle_upload_object(self):
        object_name = '/foo/bar/SP000001'
        object_info = {
            'type': ssbench.CREATE_OBJECT,
            'container': 'Picture',
            'name': object_name,
            'size': 99000,
        }
        self.mock_worker.should_receive(
            'ignoring_http_responses'
        ).with_args(
            (503,), client.put_object, object_info,
            content_length=99000,
            contents=worker.ChunkedReader('A', 99000),
        ).and_return({
            'x-swiftstack-first-byte-latency': 0.492393,
            'x-swiftstack-last-byte-latency': 8.23283,
        }).once
        self.time_expectation.once
        self.mock_queue.should_receive('put').with_args(
            yaml.dump(worker.add_dicts(object_info,
                                       worker_id=self.worker_id,
                                       first_byte_latency=0.492393,
                                       last_byte_latency=8.23283,
                                       completed_at=self.stub_time)),
        ).once
        self.mock_worker.handle_upload_object(object_info)

    def test_handle_delete_object(self):
        object_info = {
            'type': ssbench.DELETE_OBJECT,
            'container': 'Document',
            'name': 'some name',
        }
        self.mock_worker.should_receive(
            'ignoring_http_responses',
        ).with_args(
            (404, 503,), client.delete_object, object_info,
        ).and_return({
            'x-swiftstack-first-byte-latency': 0.94932,
            'x-swiftstack-last-byte-latency': 8.3273,
        }).once
        self.mock_queue.should_receive('put').with_args(
            yaml.dump(worker.add_dicts(object_info,
                                worker_id=self.worker_id,
                                first_byte_latency=0.94932,
                                last_byte_latency=8.3273,
                                completed_at=self.stub_time)),
        ).once
        self.mock_worker.handle_delete_object(object_info)
        

    def test_handle_update_object(self):
        object_info = {
            'type': ssbench.UPDATE_OBJECT,
            'container': 'Picture',
            'name': 'BestObjEvar',
            'size': 483213,
        }
        self.mock_worker.should_receive(
            'ignoring_http_responses',
        ).with_args(
            (503,), client.put_object, object_info,
            content_length=483213,
            contents=worker.ChunkedReader('B', 483213),
        ).and_return({
            'x-swiftstack-first-byte-latency': 4.45,
            'x-swiftstack-last-byte-latency': 23.283,
        }).once
        self.mock_queue.should_receive('put').with_args(
            yaml.dump(worker.add_dicts(object_info,
                                worker_id=self.worker_id,
                                completed_at=self.stub_time,
                                first_byte_latency=4.45,
                                last_byte_latency=23.283)),
        ).once

        self.mock_worker.handle_update_object(object_info)

    def test_handle_get_object(self):
        object_info = {
            'type': ssbench.READ_OBJECT,
            'container': 'Document',
            'name': 'SuperObject',
            'size': 483213,
        }
        self.mock_worker.should_receive(
            'ignoring_http_responses',
        ).with_args(
            (404, 503), client.get_object, object_info,
            resp_chunk_size=65536,
        ).and_return(({
            'x-swiftstack-first-byte-latency': 5.33,
            'x-swiftstack-last-byte-latency': 9.99,
        }, ['object_data'])).once
        self.mock_queue.should_receive('put').with_args(
            yaml.dump(worker.add_dicts(object_info,
                                worker_id=self.worker_id,
                                completed_at=self.stub_time,
                                first_byte_latency=5.33,
                                last_byte_latency=9.99)),
        ).once

        self.mock_worker.handle_get_object(object_info)

    def test_dispatching_socket_exception(self):
        info = {'type': ssbench.CREATE_OBJECT, 'a': 1}
        self.mock_worker.should_receive('handle_upload_object').with_args(info).and_raise(
            socket.error('slap happy')
        ).once
        self.mock_queue.should_receive('put').with_args(
            yaml.dump(worker.add_dicts(info,
                                worker_id=self.worker_id,
                                completed_at=self.stub_time,
                                exception=repr(socket.error('slap happy')))),
        ).once
        self.mock_worker.handle_job(info)

    def test_dispatching_client_exception(self):
        info = {'type': ssbench.READ_OBJECT, 'container': 'fun', 'a': 2}
        self.mock_worker.should_receive('handle_get_object').with_args(info).and_raise(
            client.ClientException('slam bam')
        ).once
        self.mock_queue.should_receive('put').with_args(
            yaml.dump(worker.add_dicts(info,
                                worker_id=self.worker_id,
                                completed_at=self.stub_time,
                                exception=repr(client.ClientException('slam bam')))),
        ).once
        self.mock_worker.handle_job(info)

    def test_dispatching_value_error_exception(self):
        info = {'type': ssbench.READ_OBJECT, 'container': 'fun', 'a': 2}
        self.mock_worker.should_receive('handle_get_object').with_args(info).and_raise(
            ValueError('ve'),
        ).once
        self.mock_queue.should_receive('put').with_args(
            yaml.dump(worker.add_dicts(info,
                                worker_id=self.worker_id,
                                completed_at=self.stub_time,
                                exception=repr(ValueError('ve')))),
        ).once
        self.mock_worker.handle_job(info)

    def test_dispatching_upload_object(self):
        # CREATE_OBJECT = 'upload_object' # includes obj name
        info = {'type': ssbench.CREATE_OBJECT, 'a': 1}
        self.mock_worker.should_receive('handle_upload_object').with_args(info).once
        self.mock_worker.handle_job(info)

    def test_dispatching_get_object(self):
        # READ_OBJECT = 'get_object'       # does NOT include obj name to get
        info = {'type': ssbench.READ_OBJECT, 'b': 2}
        self.mock_worker.should_receive('handle_get_object').with_args(info).once
        self.mock_worker.handle_job(info)

    def test_dispatching_update_object(self):
        # UPDATE_OBJECT = 'update_object' # does NOT include obj name to update
        info = {'type': ssbench.UPDATE_OBJECT, 'c': 3}
        self.mock_worker.should_receive('handle_update_object').with_args(info).once
        self.mock_worker.handle_job(info)

    def test_dispatching_delete_object(self):
        # DELETE_OBJECT = 'delete_object' # may or may not include obj name to delete
        info = {'type': ssbench.DELETE_OBJECT, 'd': 4}
        self.mock_worker.should_receive('handle_delete_object').with_args(info).once
        self.mock_worker.handle_job(info)
