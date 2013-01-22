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

import json
import random
import logging
from collections import defaultdict, deque

import ssbench

from pprint import pprint, pformat


class RunState(object):
    """
    An object to track the dynamic "state" of a benchmark run.
    """

    def __init__(self):
        self.initialization_results = []
        self.run_results = []

        # Stores one deque of (container_name, obj_name) tuples per size_str.
        # This stores the contents of the cluster during the benchmark run.
        # Objects are always accessed in the context of a "size_str".
        #
        # A request for an object CREATE doesn't do anything with the deque.
        # A request for an object DELETE is serviced with popleft().
        # A READ or UPDATE request is serviced with [0], then the deque is
        # rotated to the left (the serviced item goes to the back).
        #
        # A result for an object CREATE is added (to the right of the deque)
        # with append().
        # A result for READ, UPDATE, DELETE does nothing with the deque.
        self.objs_by_size = defaultdict(deque)

    def _handle_result(self, result, initial=False):
        if 'exception' not in result and \
                result['type'] == ssbench.CREATE_OBJECT:
            # Succeeded
            self.objs_by_size[result['size_str']].append(
                (result['container'], result['name'], initial))

    def handle_initialization_result(self, result):
        self.initialization_results.append(result)
        self._handle_result(result, initial=True)

    def handle_run_result(self, result):
        self.run_results.append(result)
        self._handle_result(result)

    def fill_in_job(self, job):
        obj_info = None
        if job['type'] == ssbench.DELETE_OBJECT:
            try:
                obj_info = self.objs_by_size[job['size_str']].popleft()
            except IndexError:
                # Nothing (of this size) to delete... bummer.
                return None
        elif job['type'] != ssbench.CREATE_OBJECT:
            try:
                obj_info = self.objs_by_size[job['size_str']][0]
                self.objs_by_size[job['size_str']].rotate(-1)
            except IndexError:
                # Empty?  bummer
                return None
        if obj_info:
            job['container'], job['name'], _ = obj_info
        return job

    def cleanup_object_infos(self):
        for q in sorted(self.objs_by_size.values()):
            first_initial = None
            try:
                while not first_initial or q[0] != first_initial:
                    obj_info = q[0]
                    if obj_info[2]:
                        if not first_initial:
                            first_initial = obj_info
                        q.rotate(-1)
                    else:
                        yield q.popleft()
            except IndexError:
                pass
