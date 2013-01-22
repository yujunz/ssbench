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

from nose.tools import *
from collections import deque

import ssbench
from ssbench.run_state import RunState


class TestRunState(object):
    def setUp(self):
        self.run_state = RunState()

    def _fill_initial_results(self):
        self.initial_results = [{
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket0',
            'name': 'obj1',
            'size': 88,
        }]
        # non-CREATE_OBJECT results are saved but not added to the deque
        for t in [ssbench.READ_OBJECT, ssbench.UPDATE_OBJECT,
                  ssbench.DELETE_OBJECT]:
            self.initial_results.append({
                'type': t,
                'size_str': 'obtuse',
                'container': 'bucket0',
                'name': 'obj1',
            })
        self.initial_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket1',
            'name': 'obj1',
            'size': 89,
        })
        # exception results are saved but not added to the deque
        self.initial_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket0',
            'name': 'obj1',
            'exception': 'oh noes!',
        })
        self.initial_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'round',
            'container': 'bucket0',
            'name': 'obj2',
            'size': 90,
        })
        for r in self.initial_results:
            self.run_state.handle_initialization_result(r)

    def _fill_run_results(self):
        self.run_results = [{
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'round',
            'container': 'bucket0',
            'name': 'obj3',
            'size': 77,
        }]
        # non-CREATE_OBJECT results are saved but not added to the deque
        for t in [ssbench.READ_OBJECT, ssbench.UPDATE_OBJECT,
                  ssbench.DELETE_OBJECT]:
            self.run_results.append({
                'type': t,
                'size_str': 'obtuse',
                'container': 'bucket0',
                'name': 'obj8',
            })
        self.run_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket3',
            'name': 'obj4',
            'size': 89,
        })
        # exception results are saved but not added to the deque
        self.run_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'obtuse',
            'container': 'bucket3',
            'name': 'obj5',
            'exception': 'oh noes!',
        })
        self.run_results.append({
            'type': ssbench.CREATE_OBJECT,
            'size_str': 'round',
            'container': 'bucket1',
            'name': 'obj6',
            'size': 90,
        })
        for r in self.run_results:
            self.run_state.handle_run_result(r)

    def test_handle_initialization_result(self):
        self._fill_initial_results()
        assert_equal(self.run_state.initialization_results,
                     self.initial_results)
        assert_equal(self.run_state.objs_by_size, {
            'obtuse': deque([
                ('bucket0', 'obj1', True),
                ('bucket1', 'obj1', True)]),
            'round': deque([('bucket0', 'obj2', True)]),
        })

    def test_handle_run_result(self):
        self._fill_initial_results()
        self._fill_run_results()
        assert_equal(self.run_state.run_results,
                     self.run_results)
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
