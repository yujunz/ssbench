import yaml

from ssbench.constants import *
from swift.common import client

class Master:
    def __init__(self, queue):
        queue.watch(STATS_TUBE)
        queue.ignore('default')
        self.queue = queue

    def generate_scenario_report(self, scenario, results):
        """Print a report based on the results from having run a scenario.  The
        results will cover only the "bench jobs", not the "initialization
        jobs".
        
        :results: A sequence of result records as returned by workers via the queue
        :returns: A report (string) suitable for printing, emailing, etc.
        """
    
        return repr(results)

    def run_scenario(self, auth_url, user, key, scenario):
        """Runs a CRUD scenario, given cluter parameters and a Scenario object.
        
        :auth_url: Authentication URL for the Swift cluster
        :user: Account/Username to use (format is <account>:<username>)
        :key: Password for the Account/Username
        :scenario: Scenario object describing the benchmark run
        :returns: Collected result records from workers
        """
    
        self.drain_stats_queue()
        url, token = client.get_auth(auth_url, user, key)

        # Ensure containers exist
        for container in ['Picture', 'Audio', 'Document', 'Video',
                          'Application']:
            if not self.container_exists(url, token, container):
                self.create_container(url, token, container)

        # Enqueue initialization jobs
        initial_jobs = scenario.initial_jobs()
        for initial_job in initial_jobs:
            initial_job.update(url=url, token=token)
            self.queue.put(yaml.dump(initial_job), priority=PRIORITY_SETUP)

        # Wait for them to all finish
        results = self.gather_results(len(initial_jobs))

        # Enqueue bench jobs
        bench_jobs = scenario.bench_jobs()
        for bench_job in bench_jobs:
            bench_job.update(url=url, token=token)
            self.queue.put(yaml.dump(bench_job), priority=PRIORITY_WORK)

        # Wait for them to all finish and return the results
        results = self.gather_results(len(bench_jobs))
        return results

    def bench_container_creation(self, auth_url, user, key, count):
        self.drain_stats_queue()
        url, token = client.get_auth(auth_url, user, key)

        for i in range(count):
            job = {
                "type": CREATE_CONTAINER,
                "url":  url,
                "token": token,
                "container": self.container_name(i),
                }
            self.queue.put(yaml.dump(job), priority=PRIORITY_WORK)

        results = self.gather_results(count)

        for i in range(count):
            job = {
                "type": DELETE_CONTAINER,
                "url":  url,
                "token": token,
                "container": self.container_name(i),
                }
            self.queue.put(yaml.dump(job), priority=PRIORITY_CLEANUP)

        return results

    def bench_object_creation(self, auth_url, user, key, containers, size, object_count):
        self.drain_stats_queue()
        url, token = client.get_auth(auth_url, user, key)

        for c in containers:
            if not self.container_exists(url, token, c):
                self.create_container(url, token, c)

        for i in range(object_count):
            job = {
                "type": UPLOAD_OBJECT,
                "url":  url,
                "token": token,
                "container": containers[i % len(containers)],
                "object_name": self.object_name(i),
                "object_size": size,
                }

            self.queue.put(yaml.dump(job), priority=PRIORITY_WORK)

        results = self.gather_results(object_count)

        for i in range(object_count):
            job = {
                "type": DELETE_OBJECT,
                "url":  url,
                "token": token,
                "container": containers[i % len(containers)],
                "object_name": self.object_name(i),
                }

            self.queue.put(yaml.dump(job), priority=PRIORITY_CLEANUP)

        return results

    def drain_stats_queue(self):
        self.gather_results(count=0,   # no limit
                            timeout=0) # no waiting

    def gather_results(self, count=0, timeout=15):
        results = []
        job = self.queue.reserve(timeout=timeout)
        while job:
            job.delete()
            results.append(yaml.load(job.body))
            if (count <= 0 or len(results) < count):
                job = self.queue.reserve(timeout=timeout)
            else:
                job = None
        return results

    def container_exists(self, url, token, container):
        try:
            client.head_container(url, token, container)
            return True
        except client.ClientException:
            return False

    def create_container(self, url, token, container):
        client.put_container(url, token, container)

    def container_name(self, index):
        return "ssbench-container%d" % (index,)

    def object_name(self, index):
        return "ssbench-obj%d" % (index,)
