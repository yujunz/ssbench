import re
import socket
import time
import yaml

from ssbench.constants import *
from swift.common import client

class Worker:
    MAX_RETRIES = 10

    def __init__(self, queue):
        queue.use(STATS_TUBE)
        self.queue = queue

    def go(self):
        job = self.queue.reserve()
        while job:
            job.delete()  # avoid any job-timeout nonsense
            self.handle_job(job)
            job = self.queue.reserve()


    def handle_job(self, job):
        job_data = yaml.load(job.body)

        print job_data # XXX

        tries = 0
        if job_data['type'] == UPLOAD_OBJECT:
            self.handle_upload_object(job_data)
        elif job_data['type'] == DELETE_OBJECT:
            self.handle_delete_object(job_data)
        else:
            raise NameError("Unknown job type %r" % (job_data['type'],))

    def ignoring_http_responses(self, statuses, do_stuff):
        tries = 0
        while True:
            try:
                do_stuff()
                break
            except socket.error:
                tries += 1
                if not (tries <= self.MAX_RETRIES):
                    raise
            except client.ClientException as error:
                print "oh shit an error: %r" % (error,)
                tries += 1
                if not (error.http_status in statuses and tries <= self.MAX_RETRIES):
                    raise

    def handle_delete_object(self, object_info):
        self.ignoring_http_responses(
            (404, 503),
            lambda *x: client.delete_object(
                url       = object_info['url'],
                token     = object_info['token'],
                container = object_info['container'],
                name      = object_info['object_name']))

    def handle_upload_object(self, object_info):
        self.ignoring_http_responses(
            (503,),
            lambda *x: client.put_object(
                url       = object_info['url'],
                token     = object_info['token'],
                container = object_info['container'],
                name      = object_info['object_name'],
                contents  = 'A' * object_info['object_size']))
        self.queue.put(yaml.dump({
                    "action": UPLOAD_OBJECT,
                    "completed_at": time.time()}))
