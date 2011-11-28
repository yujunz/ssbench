import yaml

from ssbench.constants import *
from swift.common import client

class Master:
    def __init__(self, queue):
        self.queue = queue

    def bench_object_creation(self, auth_url, user, key, container, size, count):
        url, token = client.get_auth(auth_url, user, key)

        if not self.container_exists(url, token, container):
            self.create_container(url, token, container)

        for i in range(count):
            object_name = "obj%d" % (i,)
            upload = {
                "type": UPLOAD_OBJECT,
                "url":  url,
                "token": token,
                "container": container,
                "object_name": object_name,
                "object_size": size,
                }

            delete = {
                "type": DELETE_OBJECT,
                "url":  url,
                "token": token,
                "container": container,
                "object_name": object_name,
                }

            self.queue.put(yaml.dump(upload), priority=PRIORITY_WORK)
            self.queue.put(yaml.dump(delete), priority=PRIORITY_CLEANUP)

        # XXX gather stats

    def container_exists(self, url, token, container):
        try:
            client.head_container(url, token, container)
            return True
        except client.ClientException:
            return False

    def create_container(self, url, token, container):
        client.put_container(url, token, container)
