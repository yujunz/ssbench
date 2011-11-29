import yaml

from ssbench.constants import *
from swift.common import client

class Master:
    def __init__(self, queue):
        queue.watch(STATS_TUBE)
        queue.ignore('default')
        self.queue = queue

    def bench_container_creation(self, auth_url, user, key, count):
        self.drain_stats_queue()
        url, token = client.get_auth(auth_url, user, key)

        for i in range(count):
            job = {
                "type": CREATE_CONTAINER,
                "url":  url,
                "token": token,
                "container_name": self.container_name(i),
                }
            self.queue.put(yaml.dump(job), priority=PRIORITY_WORK)

        results = self.gather_results(count)

        for i in range(count):
            job = {
                "type": DELETE_CONTAINER,
                "url":  url,
                "token": token,
                "container_name": self.container_name(i),
                }
            self.queue.put(yaml.dump(job), priority=PRIORITY_CLEANUP)

        return results

    def bench_object_creation(self, auth_url, user, key, container, size, count):
        self.drain_stats_queue()
        url, token = client.get_auth(auth_url, user, key)

        if not self.container_exists(url, token, container):
            self.create_container(url, token, container)

        for i in range(count):
            job = {
                "type": UPLOAD_OBJECT,
                "url":  url,
                "token": token,
                "container": container,
                "object_name": self.object_name(i),
                "object_size": size,
                }

            self.queue.put(yaml.dump(job), priority=PRIORITY_WORK)

        results = self.gather_results(count)

        for i in range(count):
            job = {
                "type": DELETE_OBJECT,
                "url":  url,
                "token": token,
                "container": container,
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
            job.delete
            results.append(yaml.load(job.body))
            job.delete
            if (count > 0 and len(results) < count):
                job = self.queue.reserve(timeout=15)
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
