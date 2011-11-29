import yaml

from ssbench.constants import *
from swift.common import client

class Master:
    def __init__(self, queue):
        queue.watch(STATS_TUBE)
        queue.ignore('default')
        self.queue = queue

    def bench_object_creation(self, auth_url, user, key, container, size, count):
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

        results = self.gather_results()

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

    def gather_results(self):
        results = []
        job = self.queue.reserve(timeout=15)
        while job:
            results.append(yaml.load(job.body))
            job.delete
            job = self.queue.reserve(timeout=15)
        return results

    def container_exists(self, url, token, container):
        try:
            client.head_container(url, token, container)
            return True
        except client.ClientException:
            return False

    def create_container(self, url, token, container):
        client.put_container(url, token, container)

    def object_name(self, index):
        return "obj%d" % (index,)
