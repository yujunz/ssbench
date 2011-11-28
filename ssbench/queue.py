import beanstalkc

class Queue:
    def __init__(self, qhost, qport):
        self.beanq = beanstalkc.Connection(qhost, qport)
        self.qname = 'default'

    def put(self, job):
        beanq.put(job)

    def get(self, timeout=None):
        job = beanq.reserve(timeout)
        job.delete()
        return job.body

    def using(self, qname=None):
        if qname:
            # beanstalkd lets you use() a single tube. The use()-d
            # tube is the one into which put() will place jobs.
            #
            # Also, beanstalkd lets you watch() multiple tubes. When
            # you call reserve(), then beanstalkd will pull a job from
            # one of the tubes you are watch()-ing.
            #
            # The Queue interface restricts you to interacting with
            # one tube at a time. Queue.put() will put jobs into that
            # tube, and Queue.get() will reserve a job from that
            # queue. This keeps the Queue interface simple enough that
            # we can swap out the implementation later if it turns out
            # we don't like beanstalkd.
            beanq.use(qname)
            beanq.watch(qname)       # add new tube to watchlist
            beanq.ignore(self.qname) # remove old tube from watchlist

            self.qname = qname
        else:
            return self.qname
