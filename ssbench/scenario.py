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
from collections import OrderedDict

import ssbench

from pprint import pprint


class Scenario(object):
    """Encapsulation of a benchmark "CRUD" scenario."""

    def __init__(self, scenario_filename):
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
        if self._scenario_data['user_count'] < 1:
            raise ValueError('user_count must be > 1')

        self.user_count = self._scenario_data['user_count']
        self.operation_count = self._scenario_data['operation_count']
        self.name = self._scenario_data['name']
        self.container_base = self._scenario_data.get('container_base',
                                                      'ssbench')
        self.container_count = self._scenario_data.get('container_count', 100)
        self.containers = ['%s_%06d' % (self.container_base, i)
                           for i in xrange(self.container_count)]
        self.container_concurrency = self._scenario_data.get(
            'controller_concurrency', 10)

        # Set up sizes
        self.sizes_by_name = OrderedDict()
        for size_data in self._scenario_data['sizes']:
            self.sizes_by_name[size_data['name']] = size_data

        # Calculate probability thresholds for each size (from the
        # initial_files)
        initial_sum = sum(self._scenario_data['initial_files'].itervalues())
        last, self.bench_size_thresholds = 0, OrderedDict()
        for size_str in self.sizes_by_name.iterkeys():
            last = last + float(
                self._scenario_data['initial_files'][size_str]) / initial_sum
            self.bench_size_thresholds[size_str] = last

        # Calculate probability thresholds for each CRUD element
        initial_sum = sum(self._scenario_data['crud_profile'])
        last, self.bench_crud_thresholds = 0, [1, 1, 1, 1]
        for i in xrange(4):
            last = last + float(
                self._scenario_data['crud_profile'][i]) / initial_sum
            self.bench_crud_thresholds[i] = last

    @property
    def crud_pcts(self):
        total = sum(self._scenario_data['crud_profile'])
        return [float(c) / total * 100
                for c in self._scenario_data['crud_profile']]

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
            for size_str in self.sizes_by_name.iterkeys():
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
            r = random.random()  # uniform on [0, 1)
            for crud_index, prob in enumerate(self.bench_crud_thresholds):
                if r < prob:
                    this_crud_index = crud_index
                    break
            yield self.bench_job(this_size_str, this_crud_index, index)
