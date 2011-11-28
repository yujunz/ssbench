import yaml

from ssbench.constants import *
from swift.common import client

class Master:
    def __init__(self, queue):
        self.queue = queue

    def go(self):
        auth_url = "http://192.168.22.100:8080/auth/v1.0/"
        user = 'dev:alice'
        key = 'password'

        url, token = client.get_auth(auth_url, user, key)

        upload = {
            "type": UPLOAD_OBJECT,
            "url":  url,
            "token": token,
            # XXX make this configurable
            "container": "dev",
            # XXX make this configurable
            "object_name": "obj1",
            # XXX make this configurable
            "object_size": 2**20  # 1 MB
            }

        self.queue.put(yaml.dump(upload))
        delete = {
            "type": DELETE_OBJECT,
            "url":  url,
            "token": token,
            # XXX make this configurable
            "container": "dev",
            # XXX make this configurable
            "object_name": "obj1",
            }
        self.queue.put(yaml.dump(delete))
