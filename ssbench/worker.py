import yaml

from ssbench.constants import *

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
            print "WOO" # magic goes here
        else:
            raise NameError("Unknown job type %r" % (job_data['type'],))

