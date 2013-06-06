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
import socket
from flexmock import flexmock
from nose.tools import *
import gevent.queue
from gevent_zeromq import zmq

import ssbench
from ssbench import worker
from ssbench import swift_client as client


class TestWorker(object):
    def setUp(self):
        self.zmq_host = 'some.host'
        self.zmq_work_port = 9372
        self.zmq_results_port = 48292
        self.work_endpoint = 'tcp://%s:%d' % (self.zmq_host,
                                              self.zmq_work_port)
        self.results_endpoint = 'tcp://%s:%d' % (self.zmq_host,
                                                 self.zmq_results_port)
        self.max_retries = 9
        self.concurrency = 223
        self.worker_id = 3

        self.mock_context = flexmock()
        flexmock(zmq.Context).new_instances(self.mock_context).once

        self.mock_work_pull = flexmock()
        self.mock_context.should_receive('socket').with_args(
            zmq.PULL,
        ).and_return(self.mock_work_pull).once
        self.mock_work_pull.should_receive('connect').with_args(
            self.work_endpoint,
        ).once

        self.mock_results_push = flexmock()
        self.mock_context.should_receive('socket').with_args(
            zmq.PUSH,
        ).and_return(self.mock_results_push).once
        self.mock_results_push.should_receive('connect').with_args(
            self.results_endpoint,
        ).once

        self.result_queue = flexmock()
        self.mock_Queue = flexmock(gevent.queue.Queue)
        self.mock_Queue.new_instances(self.result_queue).with_args(
            gevent.queue.Queue,
        ).once

        self.worker = worker.Worker(self.zmq_host, self.zmq_work_port,
                                    self.zmq_results_port, self.worker_id,
                                    self.max_retries)
        self.mock_worker = flexmock(self.worker)

        self.stub_time = 98438243.3921
        self.time_expectation = flexmock(time).should_receive(
            'time'
        ).and_return(self.stub_time)

        self.stub_fn_return = 'slambammer!'
        self.stub_fn_calls = []

        self.mock_token_data_lock = flexmock(self.worker.token_data_lock)
        self.mock_conn_pools_lock = flexmock(self.worker.conn_pools_lock)
        self.mock_client = flexmock(client)

    def stub_fn(self, *args, **kwargs):
        self.stub_fn_calls.append((args, kwargs))
        return self.stub_fn_return

    def test_create_connection_pool_still_not_there(self):
        stub_url = 'http://someAuthUrl'
        self.mock_conn_pools_lock.should_receive('acquire').ordered.once
        mock_conn = flexmock()
        flexmock(worker.ConnectionPool).new_instances(mock_conn).with_args(
            worker.ConnectionPool, client.http_connection,
            dict(url=stub_url, connect_timeout=3.142),
            self.worker.concurrency, network_timeout=2.718,
        ).ordered.once
        self.mock_conn_pools_lock.should_receive('release').ordered.once

        self.worker._create_connection_pool(stub_url, connect_timeout=3.142,
                                            network_timeout=2.718)

        assert_equal(mock_conn, self.worker.conn_pools[stub_url])

    def test_create_connection_pool_someone_beat_us(self):
        stub_url = 'http://someAuthUrl'
        self.mock_conn_pools_lock.should_receive('acquire').ordered.once
        flexmock(worker.ConnectionPool).new_instances(flexmock()).never
        self.mock_conn_pools_lock.should_receive('release').ordered.once

        self.worker.conn_pools[stub_url] = 'foobar'
        self.worker._create_connection_pool(stub_url)

    def test_ignoring_http_responses_with_storage_url(self):
        call_info = {
            'container': 'someContainer',
            'name': 'someName',
            'auth_kwargs': {
                'storage_url': 'someUrl',
                'token': 'someToken',
            },
            'connect_timeout': 3.142,
            'network_timeout': 2.718,
        }
        self.mock_token_data_lock.should_receive('acquire').never
        self.mock_token_data_lock.should_receive('release').never
        self.mock_client.should_receive('get_auth').never
        mock_pool = flexmock()

        def _insert_mock_pool(url, ignored1, ignored2):
            self.worker.conn_pools[url] = mock_pool

        self.mock_worker.should_receive('_create_connection_pool').with_args(
            'someUrl', 3.142, 2.718,
        ).replace_with(_insert_mock_pool).once
        mock_conn = flexmock()
        mock_pool.should_receive('get').and_return(mock_conn).ordered.once
        mock_pool.should_receive('put').with_args(mock_conn).ordered.once
        flexmock(gevent).should_receive('sleep').never

        got = self.worker.ignoring_http_responses([], self.stub_fn, call_info,
                                                  extra_key='extra value')

        assert_equal(got, self.stub_fn_return)
        assert_equal([((), dict(
            container='someContainer',
            name='someName',
            url='someUrl',
            token='someToken',
            http_conn=mock_conn,
            extra_key='extra value',
        ))], self.stub_fn_calls)

    def test_ignoring_http_responses_with_no_auth_info(self):
        call_info = {
            'container': 'someContainer',
            'name': 'someName',
        }
        self.mock_token_data_lock.should_receive('acquire').never
        self.mock_token_data_lock.should_receive('release').never
        self.mock_client.should_receive('get_auth').never
        self.mock_worker.should_receive('_create_connection_pool').never
        flexmock(gevent).should_receive('sleep').never

        assert_raises(ValueError, self.worker.ignoring_http_responses,
                      [], self.stub_fn, call_info, extra_key='extra value')

    def test_ignoring_http_responses_fresh_auth(self):
        call_info = {
            'container': 'someContainer',
            'name': 'someName',
            'auth_kwargs': {
                'auth_url': 'http://someAuthUrl',
                'user': 'someUser',
                'key': 'someKey',
            },
            'connect_timeout': 3.142,
            'network_timeout': 2.718,
        }
        self.mock_token_data_lock.should_receive('acquire').ordered.once
        self.mock_token_data_lock.should_receive('release').ordered.once
        self.mock_client.should_receive('get_auth').with_args(
            **call_info['auth_kwargs']
        ).and_return(('someStorageUrl', 'someStorageToken')).once
        mock_pool = flexmock()

        def _insert_mock_pool(url, ignored1, ignored2):
            self.worker.conn_pools[url] = mock_pool

        self.mock_worker.should_receive('_create_connection_pool').with_args(
            'someStorageUrl', 3.142, 2.718,
        ).replace_with(_insert_mock_pool).once
        mock_conn = flexmock()
        mock_pool.should_receive('get').and_return(mock_conn).ordered.once
        mock_pool.should_receive('put').with_args(mock_conn).ordered.once
        flexmock(gevent).should_receive('sleep').never

        got = self.worker.ignoring_http_responses([], self.stub_fn, call_info,
                                                  extra_key='extra value')

        assert_equal(got, self.stub_fn_return)
        assert_equal([((), dict(
            container='someContainer',
            name='someName',
            url='someStorageUrl',
            token='someStorageToken',
            http_conn=mock_conn,
            extra_key='extra value',
        ))], self.stub_fn_calls)

    def test_ignoring_http_responses_reauth_collision(self):
        call_info = {
            'container': 'someContainer',
            'name': 'someName',
            'auth_kwargs': {
                'auth_url': 'http://someAuthUrl',
                'user': 'someUser',
                'key': 'someKey',
            },
            'connect_timeout': 3.142,
            'network_timeout': 2.718,
        }
        token_key = self.worker._token_key(call_info['auth_kwargs'])

        def _insert_auth():
            self.worker.token_data[token_key] = ('otherUrl', 'otherToken')

        self.mock_token_data_lock.should_receive('acquire').replace_with(
            _insert_auth,
        ).ordered.once
        self.mock_token_data_lock.should_receive('release').ordered.once
        self.mock_client.should_receive('get_auth').never
        mock_pool = flexmock()

        def _insert_mock_pool(url, ignored1, ignored2):
            self.worker.conn_pools[url] = mock_pool

        self.mock_worker.should_receive('_create_connection_pool').with_args(
            'otherUrl', 3.142, 2.718,
        ).replace_with(_insert_mock_pool).once
        mock_conn = flexmock()
        mock_pool.should_receive('get').and_return(mock_conn).ordered.once
        mock_pool.should_receive('put').with_args(mock_conn).ordered.once
        flexmock(gevent).should_receive('sleep').with_args(0.005).once

        got = self.worker.ignoring_http_responses([], self.stub_fn, call_info,
                                                  extra_key='extra value')

        assert_equal(got, self.stub_fn_return)
        assert_equal([((), dict(
            container='someContainer',
            name='someName',
            url='otherUrl',
            token='otherToken',
            http_conn=mock_conn,
            extra_key='extra value',
        ))], self.stub_fn_calls)

    def test_ignoring_http_responses_cached_auth(self):
        pass

    def test_ignoring_http_responses_handles_401(self):
        pass

    def test_ignoring_http_responses_after_some_retries(self):
        pass

    def test_ignoring_http_responses_too_many_retries(self):
        pass

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
            chunk_size=worker.BLOCK_SIZE, contents='A' * worker.BLOCK_SIZE,
        ).and_return({
            'x-swiftstack-first-byte-latency': 0.492393,
            'x-swiftstack-last-byte-latency': 8.23283,
            'x-trans-id': 'abcdef',
        }).once
        self.time_expectation.once
        self.result_queue.should_receive('put').with_args(
            worker.add_dicts(
                object_info, worker_id=self.worker_id,
                first_byte_latency=0.492393, last_byte_latency=8.23283,
                trans_id='abcdef', completed_at=self.stub_time),
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
            'x-trans-id': '9bjkk',
        }).once
        self.result_queue.should_receive('put').with_args(
            worker.add_dicts(
                object_info, worker_id=self.worker_id,
                first_byte_latency=0.94932, last_byte_latency=8.3273,
                trans_id='9bjkk', completed_at=self.stub_time),
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
            chunk_size=worker.BLOCK_SIZE, contents='B' * worker.BLOCK_SIZE,
        ).and_return({
            'x-swiftstack-first-byte-latency': 4.45,
            'x-swiftstack-last-byte-latency': 23.283,
            'x-trans-id': 'biejs',
        }).once
        self.result_queue.should_receive('put').with_args(
            worker.add_dicts(
                object_info, worker_id=self.worker_id,
                completed_at=self.stub_time, trans_id='biejs',
                first_byte_latency=4.45, last_byte_latency=23.283),
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
            resp_chunk_size=worker.BLOCK_SIZE,
        ).and_return({
            'x-swiftstack-first-byte-latency': 5.33,
            'x-swiftstack-last-byte-latency': 9.99,
            'x-trans-id': 'bies',
        }).once
        self.result_queue.should_receive('put').with_args(
            worker.add_dicts(
                object_info, worker_id=self.worker_id,
                completed_at=self.stub_time, trans_id='bies',
                first_byte_latency=5.33, last_byte_latency=9.99),
        ).once

        self.mock_worker.handle_get_object(object_info)

    def test_dispatching_bad_job_type(self):
        info = {'type': 'zomg,what?', 'a': 1}
        assert_raises(NameError, self.mock_worker.handle_job, info)

    def test_dispatching_socket_exception(self):
        info = {'type': ssbench.CREATE_OBJECT, 'a': 1}
        self.mock_worker.should_receive('handle_upload_object').with_args(info).and_raise(
            socket.error('slap happy')
        ).once
        got = []
        self.result_queue.should_receive('put').replace_with(
            lambda value: got.append(value)).once

        self.mock_worker.handle_job(info)
        assert_equal(1, len(got), repr(got))
        traceback = got[0].pop('traceback')
        assert_true(traceback.startswith('Traceback'),
                    'Traceback did not start with Traceback: %s' % traceback)
        assert_equal(
            worker.add_dicts(
                info, worker_id=self.worker_id, completed_at=self.stub_time,
                exception=repr(socket.error('slap happy'))),
            got[0])

    def test_dispatching_client_exception(self):
        info = {'type': ssbench.READ_OBJECT, 'container': 'fun', 'a': 2}
        self.mock_worker.should_receive('handle_get_object').with_args(info).and_raise(
            client.ClientException('slam bam')
        ).once
        got = []
        self.result_queue.should_receive('put').replace_with(
            lambda value: got.append(value)).once

        self.mock_worker.handle_job(info)
        assert_equal(1, len(got), repr(got))
        traceback = got[0].pop('traceback')
        assert_true(traceback.startswith('Traceback'),
                    'Traceback did not start with Traceback: %s' % traceback)
        assert_equal(
            worker.add_dicts(
                info, worker_id=self.worker_id, completed_at=self.stub_time,
                exception=repr(client.ClientException('slam bam'))),
            got[0])

    def test_dispatching_value_error_exception(self):
        info = {'type': ssbench.READ_OBJECT, 'container': 'fun', 'a': 2}
        self.mock_worker.should_receive('handle_get_object').with_args(info).and_raise(
            ValueError('ve'),
        ).once
        got = []
        self.result_queue.should_receive('put').replace_with(
            lambda value: got.append(value)).once

        self.mock_worker.handle_job(info)
        assert_equal(1, len(got), repr(got))
        traceback = got[0].pop('traceback')
        assert_true(traceback.startswith('Traceback'),
                    'Traceback did not start with Traceback: %s' % traceback)
        assert_equal(
            worker.add_dicts(
                info, worker_id=self.worker_id, completed_at=self.stub_time,
                exception=repr(ValueError('ve'))),
            got[0])

    def test_dispatching_noop(self):
        info = {'type': 'zomg,what?', 'a': 1, 'noop': 'truthy'}
        self.mock_worker.should_receive(
            'handle_noop').with_args(info).once
        self.mock_worker.handle_job(info)

    def test_dispatching_upload_object(self):
        # CREATE_OBJECT = 'upload_object' # includes obj name
        info = {'type': ssbench.CREATE_OBJECT, 'a': 1}
        self.mock_worker.should_receive(
            'handle_upload_object').with_args(info).once
        self.mock_worker.handle_job(info)

    def test_dispatching_get_object(self):
        # READ_OBJECT = 'get_object'       # does NOT include obj name to get
        info = {'type': ssbench.READ_OBJECT, 'b': 2}
        self.mock_worker.should_receive(
            'handle_get_object').with_args(info).once
        self.mock_worker.handle_job(info)

    def test_dispatching_update_object(self):
        # UPDATE_OBJECT = 'update_object' # does NOT include obj name to update
        info = {'type': ssbench.UPDATE_OBJECT, 'c': 3}
        self.mock_worker.should_receive(
            'handle_update_object').with_args(info).once
        self.mock_worker.handle_job(info)

    def test_dispatching_delete_object(self):
        # DELETE_OBJECT = 'delete_object' # may or may not include obj name to
        # delete
        info = {'type': ssbench.DELETE_OBJECT, 'd': 4}
        self.mock_worker.should_receive(
            'handle_delete_object').with_args(info).once
        self.mock_worker.handle_job(info)
