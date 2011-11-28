import yaml

from ssbench.constants import *
from swift.common import client

class Worker:
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
        if job_data['type'] == UPLOAD_OBJECT:
            self.handle_upload_object(job_data)
        else:
            raise NameError("Unknown job type %r" % (job_data['type'],))

    def handle_upload_object(self, object_info):
        client.put_object(
            url       = object_info['url'],
            token     = object_info['token'],
            container = object_info['container'],
            name      = object_info['object_name'],
            contents  = 'A' * object_info['object_size'])
        print "."
