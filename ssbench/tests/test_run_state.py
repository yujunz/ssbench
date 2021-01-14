#
#Copyright (c) 2012-2021, NVIDIA CORPORATION.
#SPDX-License-Identifier: Apache-2.0

from nose.tools import assert_equal, assert_set_equal
from collections import deque

import ssbench
from ssbench.run_state import RunState


class TestRunState(object):
    def setUp(self):
        self.run_state = RunState()

    def _fill_initial_results(self):
        initial_results = [{
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket0',
            'name': 'obj1',
            'size': 88,
        }]
        # non-CREATE_OBJECT results are saved but not added to the deque
        for t in [ssbench.READ_OBJECT, ssbench.UPDATE_OBJECT,
                  ssbench.DELETE_OBJECT]:
            initial_results.append({
                'type': t,
                'size_str': 'obtuse',
                'container': 'bucket0',
                'name': 'obj1',
            })
        initial_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket1',
            'name': 'obj1',
            'size': 89,
        })
        # exception results are saved but not added to the deque
        initial_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket0',
            'name': 'obj1',
            'exception': 'oh noes!',
        })
        initial_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'round',
            'container': 'bucket0',
            'name': 'obj2',
            'size': 90,
        })
        for r in initial_results:
            self.run_state.handle_initialization_result(r)

    def _fill_run_results(self):
        run_results = [{
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'round',
            'container': 'bucket0',
            'name': 'obj3',
            'size': 77,
        }]
        # non-CREATE_OBJECT results are saved but not added to the deque
        for t in [ssbench.READ_OBJECT, ssbench.UPDATE_OBJECT,
                  ssbench.DELETE_OBJECT]:
            run_results.append({
                'type': t,
                'size_str': 'obtuse',
                'container': 'bucket0',
                'name': 'obj8',
            })
        run_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket3',
            'name': 'obj4',
            'size': 89,
        })
        # exception results are saved but not added to the deque
        run_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket3',
            'name': 'obj5',
            'exception': 'oh noes!',
        })
        run_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'round',
            'container': 'bucket1',
            'name': 'obj6',
            'size': 90,
        })
        for r in run_results:
            self.run_state.handle_run_result(r)

    def test_handle_initialization_result(self):
        self._fill_initial_results()
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([
                ('bucket0', 'obj1', True),
                ('bucket1', 'obj1', True)]),
            'round': deque([('bucket0', 'obj2', True)]),
        })

    def test_handle_run_result(self):
        self._fill_initial_results()
        self._fill_run_results()
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([
                ('bucket0', 'obj1', True),
                ('bucket1', 'obj1', True),
                ('bucket3', 'obj4', False)]),  # not "initial"
            'round': deque([
                ('bucket0', 'obj2', True),
                ('bucket0', 'obj3', False),
                ('bucket1', 'obj6', False)]),
        })

    def test_cleanup_object_infos(self):
        self._fill_initial_results()
        self._fill_run_results()
        cleanups = list(self.run_state.cleanup_object_infos())

        assert_equal(cleanups, [('bucket3', 'obj4', False),
                                ('bucket0', 'obj3', False),
                                ('bucket1', 'obj6', False)])
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([
                ('bucket0', 'obj1', True),
                ('bucket1', 'obj1', True)]),  # not "initial"
            'round': deque([
                ('bucket0', 'obj2', True)]),
        })

    def test_cleanup_object_infos_with_no_initials(self):
        self._fill_run_results()
        cleanups = set(self.run_state.cleanup_object_infos())

        assert_set_equal(cleanups, set([('bucket3', 'obj4', False),
                                        ('bucket0', 'obj3', False),
                                        ('bucket1', 'obj6', False)]))
        # There were no initials, so there's nothing left:
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque(),
            'round': deque(),
        })

    def test_fill_in_job_for_create_object(self):
        self._fill_initial_results()
        self._fill_run_results()
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.CREATE_OBJECT,
            'creates': 'are ignored',
        }), {
            'type': ssbench.CREATE_OBJECT,
            'creates': 'are ignored',
        })
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([
                ('bucket0', 'obj1', True),
                ('bucket1', 'obj1', True),
                ('bucket3', 'obj4', False)]),
            'round': deque([
                ('bucket0', 'obj2', True),
                ('bucket0', 'obj3', False),
                ('bucket1', 'obj6', False)]),
        })

    def test_fill_in_job_for_delete_object(self):
        self._fill_initial_results()
        self._fill_run_results()
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'obtuse',
        }), {
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket0',
            'name': 'obj1',
        })
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([
                ('bucket1', 'obj1', True),
                ('bucket3', 'obj4', False)]),
            'round': deque([
                ('bucket0', 'obj2', True),
                ('bucket0', 'obj3', False),
                ('bucket1', 'obj6', False)]),
        })

    def test_fill_in_job_for_update_object(self):
        self._fill_initial_results()
        self._fill_run_results()
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.UPDATE_OBJECT,
            'size_str': 'round',
            'size': 991,
        }), {
            'type': ssbench.UPDATE_OBJECT,
            'size_str': 'round',
            'size': 991,
            'container': 'bucket0',
            'name': 'obj2',
        })
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([
                ('bucket0', 'obj1', True),
                ('bucket1', 'obj1', True),
                ('bucket3', 'obj4', False)]),
            'round': deque([
                ('bucket0', 'obj3', False),  # (got rotated)
                ('bucket1', 'obj6', False),
                ('bucket0', 'obj2', True)]),
        })

    def test_fill_in_job_for_read_object(self):
        self._fill_initial_results()
        self._fill_run_results()
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.READ_OBJECT,
            'size_str': 'obtuse',
        }), {
            'type': ssbench.READ_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket0',
            'name': 'obj1',
        })
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([
                ('bucket1', 'obj1', True),  # (got rotated)
                ('bucket3', 'obj4', False),
                ('bucket0', 'obj1', True)]),
            'round': deque([
                ('bucket0', 'obj2', True),
                ('bucket0', 'obj3', False),
                ('bucket1', 'obj6', False)]),
        })

    def test_fill_in_job_when_empty(self):
        self._fill_initial_results()
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'obtuse',
        }), {
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket0',
            'name': 'obj1',
        })
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'obtuse',
        }), {
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket1',
            'name': 'obj1',
        })
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'obtuse',
        }), None)
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.UPDATE_OBJECT,
            'size_str': 'obtuse',
            'size': 31,
        }), None)

        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'round',
        }), {
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'round',
            'container': 'bucket0',
            'name': 'obj2',
        })
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.DELETE_OBJECT,
            'size_str': 'round',
        }), None)
        assert_equal(self.run_state.fill_in_job({
            'type': ssbench.READ_OBJECT,
            'size_str': 'round',
        }), None)

        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([]),
            'round': deque([]),
        })
