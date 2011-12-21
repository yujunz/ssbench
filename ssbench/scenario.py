import json
import random

import logging
from ssbench.constants import *
from ssbench.scenario_file import ScenarioFile, SIZE_STRS

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
            logging.exception('Error loading scenario file %r', scenario_filename)
            raise

        # Sanity-check user_count
        if self._scenario_data['user_count'] < 1 or self._scenario_data['user_count'] > MAX_WORKERS:
            raise ValueError('user_count must be between 1 and %d' % MAX_WORKERS)

        # Calculate probability thresholds for each size (from the initial_files)
        initial_sum = sum([self._scenario_data['initial_files'][size_str] for
                           size_str in SIZE_STRS])
        last, self.bench_size_thresholds = 0, {}
        for size_str in SIZE_STRS:
            last = last + float(self._scenario_data['initial_files'][size_str]) / initial_sum
            self.bench_size_thresholds[size_str] = last

        # Calculate probability thresholds for each CRUD element
        initial_sum = sum(self._scenario_data['crud_profile'])
        last, self.bench_crud_thresholds = 0, [1, 1, 1, 1]
        for i in range(4):
            last = last + float(self._scenario_data['crud_profile'][i]) / initial_sum
            self.bench_crud_thresholds[i] = last

    @property
    def user_count(self):
        return self._scenario_data['user_count']

    @property
    def name(self):
        return self._scenario_data['name']


    def initial_job(self, size_str, i):
        """Creates an initializing job dict of a given size.
        
        :size_str: One of 'tiny', 'small', etc.
        :i: The job index (for this size)
        :returns: A dictionary representing the initialization job
        """

        sfile = ScenarioFile('S', size_str, i)
        return {
            "type": CREATE_OBJECT,
            "container": sfile.container,
            "object_name": sfile.name,
            "object_size": sfile.size,
        }

    def bench_job(self, size_str, crud_index, i):
        """Creates a benchmark work job dict of a given size and crud "index"
        (where 0 is Create, 1 is Read, etc.).
        
        :size_str: One of 'tiny', 'small', etc.
        :crud_index: An index into the CRUD array (0 is Create, etc.)
        :i: The job index
        :returns: A dictionary representing benchmark work job
        """
   
        sfile = ScenarioFile('P', size_str, i)
        if crud_index == 0:
            # Create
            return dict(
                type=CREATE_OBJECT,
                container=sfile.container,
                object_name=sfile.name,
                object_size=sfile.size,
            )
        elif crud_index == 1:
            # Read
            return dict(
                type=READ_OBJECT,
                container=sfile.container,
                object_size=sfile.size,
            )
        elif crud_index == 2:
            # Update
            return dict(
                type=UPDATE_OBJECT,
                container=sfile.container,
                object_size=sfile.size,
            )
        elif crud_index == 3:
            # Delete
            return dict(
                type=DELETE_OBJECT,
                container=sfile.container,
                object_size=sfile.size,
            )


    def initial_jobs(self):
        """Returns the worker jobs necessary to initialize the cluster contents
        for the scenario.

        :returns: A list of job objects (dicts)
        """
    
        counts, indexes = {}, {}
        for size in SIZE_STRS:
            counts[size] = self._scenario_data['initial_files'][size]
            indexes[size] = 1

        initial_jobs = []
        pushed = True
        while pushed:
            pushed = False
            for size in SIZE_STRS:
                if counts[size]:
                    initial_jobs.append(
                        self.initial_job(size, indexes[size])
                    )
                    counts[size] -= 1
                    indexes[size] += 1
                    pushed = True

        return initial_jobs


    def bench_jobs(self):
        """Returns the worker jobs necessary to actually run the scenario.

        :returns: A list of job objects (dicts)
        """
    
        bench_jobs = []
        for index in range(1, self._scenario_data['file_count'] + 1):
            r = random.random()  # uniform on [0, 1)
            for size_str in SIZE_STRS:
                if r < self.bench_size_thresholds[size_str]:
                    this_size_str = size_str
                    break
            # Determine which C/R/U/D type this job will be
            r = random.random()  # uniform on [0, 1)
            for crud_index in range(4):
                if r < self.bench_crud_thresholds[crud_index]:
                    this_crud_index = crud_index
                    break
            bench_jobs.append(self.bench_job(this_size_str, this_crud_index, index))
        return bench_jobs


