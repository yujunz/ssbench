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

import copy
import json
import random
import logging

import ssbench
from ssbench.ordered_dict import OrderedDict

from pprint import pprint


class Scenario(object):
    """Encapsulation of a benchmark "CRUD" scenario."""

    def __init__(self, scenario_filename, container_count=None,
                 user_count=None, operation_count=None):
        """Initializes the object from a scenario file on disk.

        :scenario_filename: path to a scenario file
        """

        try:
            fp = open(scenario_filename)
            self._scenario_data = json.load(fp)
        except:
            logging.exception('Error loading scenario file %r',
                              scenario_filename)
            raise

        # Sanity-check user_count
        if user_count is not None:
            self.user_count = user_count
        else:
            self.user_count = self._scenario_data['user_count']
        if self.user_count < 1:
            raise ValueError('user_count must be > 1')

        if operation_count is not None:
            self.operation_count = operation_count
        else:
            self.operation_count = self._scenario_data['operation_count']

        self.name = self._scenario_data['name']
        self.container_base = self._scenario_data.get('container_base',
                                                      'ssbench')
        if container_count is not None:
            self.container_count = container_count
        else:
            self.container_count = self._scenario_data.get(
                'container_count', 100)
        self.containers = ['%s_%06d' % (self.container_base, i)
                           for i in xrange(self.container_count)]
        self.container_concurrency = self._scenario_data.get(
            'container_concurrency', 10)

        # Set up sizes
        self.sizes_by_name = OrderedDict()
        for size_data in self._scenario_data['sizes']:
            size_data_copy = copy.deepcopy(size_data)
            self.sizes_by_name[size_data_copy['name']] = size_data_copy
            crud_profile = size_data_copy.get(
                'crud_profile', self._scenario_data['crud_profile'])
            crud_total = sum(crud_profile)
            size_data_copy['crud_pcts'] = [
                float(c) / crud_total * 100 for c in crud_profile]
            # Calculate probability thresholds for each CRUD element for this
            # object size category (defaulting to global crud profile).
            size_data_copy['crud_thresholds'] = [1, 1, 1, 1]
            self._thresholds_for(size_data_copy['crud_thresholds'],
                                 range(4), crud_profile)

        # Calculate probability thresholds for each size (from the
        # initial_files)
        self.bench_size_thresholds = OrderedDict()
        self._thresholds_for(
            self.bench_size_thresholds,
            filter(lambda n: n in self._scenario_data['initial_files'],
                   self.sizes_by_name.keys()),
            self._scenario_data['initial_files'])

    @property
    def crud_pcts(self):
        total = sum(self._scenario_data['crud_profile'])
        return [float(c) / total * 100
                for c in self._scenario_data['crud_profile']]

    def _thresholds_for(self, target, indices, data):
        initial_sum = sum(map(lambda i: data[i], indices))
        last = 0
        for idx in indices:
            last = last + float(data[idx]) / initial_sum
            target[idx] = last

    def job(self, size_str, **kwargs):
        job = {'size_str': size_str}
        job.update(kwargs)
        return job

    def create_job(self, size_str, i):
        """
        Creates job dict which will create an object.
        """

        return self.job(size_str,
                        type=ssbench.CREATE_OBJECT,
                        container=random.choice(self.containers),
                        name='%s_%06d' % (size_str, i),
                        size=random.randint(
                            self.sizes_by_name[size_str]['size_min'],
                            self.sizes_by_name[size_str]['size_max']))

    def bench_job(self, size_str, crud_index, i):
        """Creates a benchmark work job dict of a given size and crud "index"
        (where 0 is Create, 1 is Read, etc.).

        :size_str: One of the size strings defined in the scenario file
        :crud_index: An index into the CRUD array (0 is Create, etc.)
        :i: The job index
        :returns: A dictionary representing benchmark work job
        """

        if crud_index == 0:
            return self.create_job(size_str, i)
        elif crud_index == 1:
            return self.job(size_str, type=ssbench.READ_OBJECT)
        elif crud_index == 2:
            return self.job(
                size_str, type=ssbench.UPDATE_OBJECT,
                size=random.randint(
                    self.sizes_by_name[size_str]['size_min'],
                    self.sizes_by_name[size_str]['size_max']))
        elif crud_index == 3:
            return self.job(size_str, type=ssbench.DELETE_OBJECT)

    def initial_jobs(self):
        """
        Generator for the worker jobs necessary to initialize the cluster
        contents for the scenario.

        :returns: A generator which yields job objects (dicts)
        """

        count_by_size = copy.copy(self._scenario_data['initial_files'])
        index_per_size = dict.fromkeys(count_by_size.iterkeys(), 1)

        yielded = True
        while yielded:
            yielded = False
            for size_str in filter(
                    lambda n: n in self._scenario_data['initial_files'],
                    self.sizes_by_name.keys()):
                if count_by_size[size_str]:
                    yield self.create_job(size_str, index_per_size[size_str])
                    count_by_size[size_str] -= 1
                    index_per_size[size_str] += 1
                    yielded = True

    def bench_jobs(self):
        """
        Generator for the worker jobs necessary to actually run the scenario.

        :returns: A generator which yields job objects (dicts)
        """

        max_index_size = max(self._scenario_data['initial_files'].itervalues())
        for index in xrange(max_index_size + 1,
                            max_index_size + self.operation_count + 1):
            r = random.random()  # uniform on [0, 1)
            for size_str, prob in self.bench_size_thresholds.iteritems():
                if r < prob:
                    this_size_str = size_str
                    break
            # Determine which C/R/U/D type this job will be
            size_crud = self.sizes_by_name[this_size_str]['crud_thresholds']
            r = random.random()  # uniform on [0, 1)
            for crud_index, prob in enumerate(size_crud):
                if r < prob:
                    this_crud_index = crud_index
                    break
            yield self.bench_job(this_size_str, this_crud_index, index)


class ScenarioNoop(Scenario):
    """
    A subclass of Scenario which just yields up NOP jobs.
    """

    def job(self, size_str, **kwargs):
        job = {
            'size_str': size_str,
            'noop': True,
        }
        job.update(kwargs)
        return job
