from nose.tools import *
from flexmock import flexmock
import yaml
from argparse import Namespace
from collections import Counter

from ssbench.constants import *
from ssbench.worker import Worker, add_dicts
import ssbench.worker
from ssbench import swift_client as client

class TestWorker(object):
    def setUp(self):
        self.stub_queue = flexmock()
        self.stub_queue.should_receive('use').with_args(STATS_TUBE).once
        # Workers should look at tubes >= worker_id
        for i in range(3, MAX_WORKERS + 1):
            self.stub_queue.should_receive('watch').with_args(WORK_TUBE_FORMAT % i).once
        self.stub_worker_id = 3
        self.worker = Worker(self.stub_queue, self.stub_worker_id)

    def test_get_object_name_with_no_types(self):
        assert_equal(None, self.worker.get_object_name('foo', 'bar'))

    def test_get_object_name_with_no_names(self):
        assert_equal(None, self.worker.get_object_name('stock', 'foo'))

    def test_add_get_object_name(self):
        # Stored object names:
        # stock
        #   container1
        #     3 names: sc1a, sc1b, sc1c
        #   container2
        #     3 names: sc2a, sc2b, sc2c
        # population
        #   container1
        #     3 names: pc1a, pc1b, pc1c
        #
        # Expect that we can get them back out randomly with a flat probability
        # distribution.
        self.worker.add_object_name('stock', 'container1', 'sc1a')
        self.worker.add_object_name('stock', 'container1', 'sc1b')
        self.worker.add_object_name('stock', 'container1', 'sc1c')
        self.worker.add_object_name('stock', 'container2', 'sc2a')
        self.worker.add_object_name('stock', 'container2', 'sc2b')
        self.worker.add_object_name('stock', 'container2', 'sc2c')
        self.worker.add_object_name('population', 'container1', 'pc1a')
        self.worker.add_object_name('population', 'container1', 'pc1b')
        self.worker.add_object_name('population', 'container1', 'pc1c')

        name_count = 1000
        exp_count = name_count / 3.0
        exp_delta = name_count * 0.1 # expect distribution to be even w/in 10%
        sc1_name_count = Counter(
            [self.worker.get_object_name('stock', 'container1') for _ in range(name_count)]
        )
        assert_almost_equal(exp_count, sc1_name_count['sc1a'], delta=exp_delta)
        assert_almost_equal(exp_count, sc1_name_count['sc1b'], delta=exp_delta)
        assert_almost_equal(exp_count, sc1_name_count['sc1c'], delta=exp_delta)
        sc2_name_count = Counter(
            [self.worker.get_object_name('stock', 'container2') for _ in range(name_count)]
        )
        assert_almost_equal(exp_count, sc2_name_count['sc2a'], delta=exp_delta)
        assert_almost_equal(exp_count, sc2_name_count['sc2b'], delta=exp_delta)
        assert_almost_equal(exp_count, sc2_name_count['sc2c'], delta=exp_delta)
        pc1_name_count = Counter(
            [self.worker.get_object_name('population', 'container1') for _ in range(name_count)]
        )
        assert_almost_equal(exp_count, pc1_name_count['pc1a'], delta=exp_delta)
        assert_almost_equal(exp_count, pc1_name_count['pc1b'], delta=exp_delta)
        assert_almost_equal(exp_count, pc1_name_count['pc1c'], delta=exp_delta)

    def test_remove_object_name(self):
        # pass these by not raising any Exceptions
        self.worker.remove_object_name('bad type', 'not a container', 'foo')
        self.worker.remove_object_name('population', 'not a container', 'foo')

        self.worker.add_object_name('stock', 'container1', 'abcdef')
        self.worker.remove_object_name('stock', 'container1', 'foo')
        assert_equal('abcdef', self.worker.get_object_name('stock', 'container1'))

        self.worker.add_object_name('stock', 'container1', 'xyz')
        assert_equal('abcdef', self.worker.remove_object_name('stock', 'container1', 'abcdef'))
        assert_equal('xyz', self.worker.get_object_name('stock', 'container1'))


    def test_handle_upload_population_object(self):
        object_name = '/foo/bar/PA000001'
        object_info = {
            'type': CREATE_OBJECT,
            'container': 'Application',
            'object_name': object_name,
            'object_size': 3493284.0,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.head_object, object_info,
            name=object_name,
        ).never
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.put_object, object_info,
            name=object_name,
            contents=worker.ChunkedReader('A', 3493284),
        ).and_return({
            'x-swiftstack-first-byte-latency': 0.30239,
            'x-swiftstack-last-byte-latency': 32.435,
        }).once
        worker.should_receive('add_object_name').with_args(
            'population', 'Application', object_name,
        ).once
        stub_time = 49392932.949
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                first_byte_latency=0.30239,
                                last_byte_latency=32.435,
                                completed_at=stub_time)),
        ).once
        worker.handle_upload_object(object_info)

    def test_handle_upload_stock_object_not_existing(self):
        object_name = '/foo/bar/SP000001'
        object_info = {
            'type': CREATE_OBJECT,
            'container': 'Picture',
            'object_name': object_name,
            'object_size': 99000.0,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.head_object, object_info,
            name=object_name,
        ).and_raise(client.ClientException('Object HEAD failed')).once
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.put_object, object_info,
            name=object_name,
            contents=worker.ChunkedReader('A', 99000),
        ).and_return({
            'x-swiftstack-first-byte-latency': 0.492393,
            'x-swiftstack-last-byte-latency': 8.23283,
        }).once
        worker.should_receive('add_object_name').with_args(
            'stock', 'Picture', object_name,
        ).once
        stub_time = 98438243.3921
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                first_byte_latency=0.492393,
                                last_byte_latency=8.23283,
                                completed_at=stub_time)),
        ).once
        worker.handle_upload_object(object_info)

    def test_handle_upload_stock_object_existing(self):
        object_name = '/foo/bar/SP000001'
        object_info = {
            'type': CREATE_OBJECT,
            'container': 'Picture',
            'object_name': object_name,
            'object_size': 99000.0,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.head_object, object_info,
            name=object_name,
        ).and_return(dict(foo='bar')).once
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.put_object, object_info,
            name=object_name,
            contents=worker.ChunkedReader('A', 99000),
        ).never
        worker.should_receive('add_object_name').with_args(
            'stock', 'Picture', object_name,
        ).once
        stub_time = 98438243.3921
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                # Existing stock object; skipped upload, so no stats
                                first_byte_latency=None,
                                last_byte_latency=None,
                                completed_at=stub_time)),
        ).once
        worker.handle_upload_object(object_info)

    def test_delete_object_with_name(self):
        object_name = '/lcj/nn/SD000843'
        object_info = {
            'type': DELETE_OBJECT,
            'container': 'Document',
            'object_name': object_name,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (404, 503,), client.delete_object, object_info,
            name=object_name,
        ).and_return({
            'x-swiftstack-first-byte-latency': 0.94932,
            'x-swiftstack-last-byte-latency': 8.3273,
        }).once
        worker.should_receive('remove_object_name').with_args(
            'stock', 'Document', object_name,
        ).once
        stub_time = 98438243.3921
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                first_byte_latency=0.94932,
                                last_byte_latency=8.3273,
                                completed_at=stub_time)),
        ).once
        worker.handle_delete_object(object_info)
        

    def test_delete_object_without_name_have_population(self):
        stock_object_name = '/kabo/qq/SA000038'
        population_object_name = '/bls/.bo/PA000492'
        object_info = {
            'type': DELETE_OBJECT,
            'container': 'Audio',
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (404, 503,), client.delete_object, object_info,
            name=population_object_name,
        ).and_return({
            'x-swiftstack-first-byte-latency': 2.43923,
            'x-swiftstack-last-byte-latency': 3.1,
        }).once
        worker.should_receive('remove_object_name').with_args(
            'population', 'Audio', population_object_name,
        ).once
        stub_time = 4939320.233
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                completed_at=stub_time,
                                first_byte_latency=2.43923,
                                last_byte_latency=3.1,
                                object_name=population_object_name)),
        ).once
        self.worker.add_object_name('stock', 'Audio', stock_object_name)
        self.worker.add_object_name('population', 'Audio', population_object_name)

        worker.handle_delete_object(object_info)

    def test_delete_object_without_name_have_no_population_or_stock(self):
        object_info = {
            'type': DELETE_OBJECT,
            'container': 'Audio',
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').never
        worker.should_receive('remove_object_name').never
        self.stub_queue.should_receive('put').never

        worker.handle_delete_object(object_info)

    def test_delete_object_without_name_have_no_population(self):
        stock_object_name = '/kabo/qq/SA000038'
        object_info = {
            'type': DELETE_OBJECT,
            'container': 'Audio',
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (404, 503,), client.delete_object, object_info,
            name=stock_object_name,
        ).and_return({
            'x-swiftstack-first-byte-latency': 2.44,
            'x-swiftstack-last-byte-latency': 5.1,
        }).once
        worker.should_receive('remove_object_name').with_args(
            'stock', 'Audio', stock_object_name,
        ).once
        stub_time = 4939320.233
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                first_byte_latency=2.44,
                                last_byte_latency=5.1,
                                completed_at=stub_time,
                                object_name=stock_object_name)),
        ).once
        self.worker.add_object_name('stock', 'Audio', stock_object_name)

        worker.handle_delete_object(object_info)
 

    def test_update_object_without_name_have_population(self):
        stock_object_name = '/lsv/op/SP000392'
        population_object_name = '/eavl/ovjs/PP000192'
        object_info = {
            'type': UPDATE_OBJECT,
            'container': 'Picture',
            'object_size': 483213.0,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.put_object, object_info,
            name=population_object_name,
            contents=worker.ChunkedReader('B',483213),
        ).and_return({
            'x-swiftstack-first-byte-latency': 4.45,
            'x-swiftstack-last-byte-latency': 23.283,
        }).once
        worker.should_receive('remove_object_name').never
        stub_time = 48238328.234
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                completed_at=stub_time,
                                first_byte_latency=4.45,
                                last_byte_latency=23.283,
                                object_name=population_object_name)),
        ).once
        self.worker.add_object_name('stock', 'Picture', stock_object_name)
        self.worker.add_object_name('population', 'Picture', population_object_name)

        worker.handle_update_object(object_info)

    def test_update_object_without_name_have_no_population_or_stock(self):
        object_info = {
            'type': UPDATE_OBJECT,
            'container': 'Picture',
            'object_size': 39928438,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').never
        worker.should_receive('remove_object_name').never
        self.stub_queue.should_receive('put').never

        worker.handle_update_object(object_info)

    def test_update_object_without_name_have_no_population(self):
        stock_object_name = '/bjsl/gfc/SP006546'
        object_info = {
            'type': UPDATE_OBJECT,
            'container': 'Picture',
            'object_size': 8492391,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.put_object, object_info,
            name=stock_object_name,
            contents=worker.ChunkedReader('B', 8492391),
        ).and_return({
            'x-swiftstack-first-byte-latency': 4.88,
            'x-swiftstack-last-byte-latency': 23.88,
        }).once
        worker.should_receive('remove_object_name').never
        stub_time = 2948293293.382
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                completed_at=stub_time,
                                first_byte_latency=4.88,
                                last_byte_latency=23.88,
                                object_name=stock_object_name)),
        ).once
        assert_false(object_info.has_key('object_name')) # sanity check
        self.worker.add_object_name('stock', 'Picture', stock_object_name)

        worker.handle_update_object(object_info)
        assert_false(object_info.has_key('object_name')) # sanity check

    def test_get_object_without_name_have_population(self):
        stock_object_name = 'SD000034'
        population_object_name = '/PD009123'
        object_info = {
            'type': READ_OBJECT,
            'container': 'Document',
            'object_size': 483213,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.get_object, object_info,
            name=population_object_name,
            resp_chunk_size=65536,
        ).and_return(({
            'x-swiftstack-first-byte-latency': 5.33,
            'x-swiftstack-last-byte-latency': 9.99,
        }, 'object_data')).once
        worker.should_receive('remove_object_name').never
        stub_time = 48238328.234
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                completed_at=stub_time,
                                first_byte_latency=5.33,
                                last_byte_latency=9.99,
                                object_name=population_object_name)),
        ).once
        self.worker.add_object_name('stock', 'Document', stock_object_name)
        self.worker.add_object_name('population', 'Document', population_object_name)

        worker.handle_get_object(object_info)

    def test_get_object_without_name_have_no_population_or_stock(self):
        object_info = {
            'type': READ_OBJECT,
            'container': 'Document',
            'object_size': 39928438,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').never
        worker.should_receive('remove_object_name').never
        self.stub_queue.should_receive('put').never

        worker.handle_get_object(object_info)

    def test_get_object_without_name_have_no_population(self):
        stock_object_name = '/0glwvm//SD006546'
        object_info = {
            'type': READ_OBJECT,
            'container': 'Document',
            'object_size': 8492391,
        }
        worker = flexmock(self.worker)
        worker.should_receive('ignoring_http_responses').with_args(
            (503,), client.get_object, object_info,
            name=stock_object_name,
            resp_chunk_size=65536,
        ).and_return(({
            'x-swiftstack-first-byte-latency': 6.66,
            'x-swiftstack-last-byte-latency': 8.88,
        }, 'object_data')).once
        worker.should_receive('remove_object_name').never
        stub_time = 2948293293.382
        mock_worker_module = flexmock(ssbench.worker)
        mock_worker_module.should_receive('time').and_return(stub_time)
        self.stub_queue.should_receive('put').with_args(
            yaml.dump(add_dicts(object_info,
                                worker_id=self.stub_worker_id,
                                completed_at=stub_time,
                                first_byte_latency=6.66,
                                last_byte_latency=8.88,
                                object_name=stock_object_name)),
        ).once
        self.worker.add_object_name('stock', 'Document', stock_object_name)

        worker.handle_get_object(object_info)
 

    def test_dispatching_upload_object(self):
        # CREATE_OBJECT = 'upload_object' # includes obj name
        info = {'type': CREATE_OBJECT, 'a': 1}
        job = Namespace(body=yaml.dump(info))
        worker = flexmock(self.worker)
        worker.should_receive('handle_upload_object').with_args(info).once
        worker.handle_job(job)

    def test_dispatching_get_object(self):
        # READ_OBJECT = 'get_object'       # does NOT include obj name to get
        info = {'type': READ_OBJECT, 'b': 2}
        job = Namespace(body=yaml.dump(info))
        worker = flexmock(self.worker)
        worker.should_receive('handle_get_object').with_args(info).once
        worker.handle_job(job)

    def test_dispatching_update_object(self):
        # UPDATE_OBJECT = 'update_object' # does NOT include obj name to update
        info = {'type': UPDATE_OBJECT, 'c': 3}
        job = Namespace(body=yaml.dump(info))
        worker = flexmock(self.worker)
        worker.should_receive('handle_update_object').with_args(info).once
        worker.handle_job(job)

    def test_dispatching_delete_object(self):
        # DELETE_OBJECT = 'delete_object' # may or may not include obj name to delete
        info = {'type': DELETE_OBJECT, 'd': 4}
        job = Namespace(body=yaml.dump(info))
        worker = flexmock(self.worker)
        worker.should_receive('handle_delete_object').with_args(info).once
        worker.handle_job(job)

    def test_dispatching_create_container(self):
        # CREATE_CONTAINER = 'create_container'
        info = {'type': CREATE_CONTAINER, 'e': 5}
        job = Namespace(body=yaml.dump(info))
        worker = flexmock(self.worker)
        worker.should_receive('handle_create_container').with_args(info).once
        worker.handle_job(job)

    def test_dispatching_delete_container(self):
        # DELETE_CONTAINER = 'delete_container'
        info = {'type': DELETE_CONTAINER, 'f': 6}
        job = Namespace(body=yaml.dump(info))
        worker = flexmock(self.worker)
        worker.should_receive('handle_delete_container').with_args(info).once
        worker.handle_job(job)

