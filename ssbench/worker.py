import re
import socket
from time import time
import yaml
import random
import logging

from ssbench.constants import *
from ssbench import swift_client as client

def add_dicts(*args, **kwargs):
    result = {}
    for d in args:
        result.update(d)
    result.update(kwargs)
    return result
 

class Worker:
    MAX_RETRIES = 50

    def __init__(self, queue, worker_id):
        queue.use(STATS_TUBE)
        for i in range(worker_id, MAX_WORKERS + 1):
            queue.watch('work_%04d' % i)
        self.queue = queue
        self.worker_id = worker_id
        self.object_names = {
            'stock': {},
            'population': {},
        }

    def go(self):
        logging.debug('Worker %s starting...', self.worker_id)
        job = self.queue.reserve()
        while job:
            job.delete()  # avoid any job-timeout nonsense
            self.handle_job(job)
            job = self.queue.reserve()


    def handle_job(self, job):
        job_data = yaml.load(job.body)

        logging.debug('%r', job_data)

        tries = 0
        # Dispatch type to a handler, if possible
        handler = getattr(self, 'handle_%s' % job_data['type'], None)
        if handler:
            handler(job_data)
        else:
            raise NameError("Unknown job type %r" % job_data['type'])

    def ignoring_http_responses(self, statuses, fn, call_info, **extra_keys):
        tries = 0
        args = dict(
            url=call_info['url'],
            token=call_info['token'],
            container=call_info['container'],
        )
        args.update(extra_keys)

        while True:
            try:
                fn_results = fn(**args)
                break
            # XXX The name of this method does not suggest that it
            # will also ignore socket-level errors. Regardless,
            # sometimes Swift refuses connections (probably when it's
            # way overloaded and the listen socket's connection queue
            # (in the kernel) is full, so the kernel just says RST).
            except socket.error:
                tries += 1
                if not (tries <= self.MAX_RETRIES):
                    raise
            except client.ClientException as error:
                tries += 1
                if error.http_status in statuses and tries <= self.MAX_RETRIES:
                    print "Retrying an error: %r" % (error,)
                else:
                    raise
        return fn_results

    def add_object_name(self, object_type, container, object_name):
        """Stores an added object (by type and container name) for later
        retrieval.
        
        :object_type: The type of object (eg. 'stock' or 'population')
        :container: The name of a container (generally implies size)
        :name: The name of an object
        :returns: (nothing)
        """

        logging.debug('add_object_name(%r, %r, %r)', object_type, container, object_name)
        if self.object_names[object_type].has_key(container):
            self.object_names[object_type][container].append(object_name)
        else:
            self.object_names[object_type][container] = [object_name]

    def remove_object_name(self, object_type, container, object_name):
        """Removes a stored object name (by type and container name).  If the
        name wasn't there, this is just a no-op. 
        
        :object_type: The type of object (eg. 'stock' or 'population')
        :container: The name of a container
        :object_name: The object name to remove
        :returns: (nothing)
        """
    
        names_by_container = self.object_names.get(object_type, None)
        if names_by_container:
            container_names = names_by_container.get(container, None)
            if container_names:
                try:
                    container_names.remove(object_name)
                except ValueError:
                    pass  # don't care if it wasn't there
                else:
                    return object_name

    def get_object_name(self, object_type, container):
        """Retrieve an object name (by type and container) at random, with a
        uniform probability distribution.
        
        :object_type: The type of object (eg. 'stock' or 'population')
        :container: The name of a container (generally implies size)
        :returns: The randomly-chosen name or None if no names were available
        """

        names_by_container = self.object_names.get(object_type, None)
        if names_by_container:
            container_names = names_by_container.get(container, None)
            if container_names:
                name = random.choice(container_names)
                logging.debug('get_object_name(%r, %r) = %r', object_type, container, name)
                return name
        logging.debug('get_object_name(%r, %r) = None', object_type, container)
        return None

    def object_name_type(self, object_name):
        """Returns the type string for an object name.  For now, this method
        examines the final "path" of an object, with / as a delimiter.  If the
        final "path" starts with "S", then the type is 'stock', otherwise the
        type returned is 'population'.
        
        :object_name: The name of an object
        :returns: An object type (eg. 'stock', or 'population')
        """
    
        if re.match('([^/]*/)*S[^/]+$', object_name):
            logging.debug('object_name_type(%r) = stock', object_name)
            return 'stock'
        else:
            logging.debug('object_name_type(%r) = population', object_name)
            return 'population'

    def put_results(self, *args, **kwargs):
        """Put work result into stats queue.  Given *args and **kwargs are
        combined per add_dicts().  This worker's "ID" and the time of completion
        are included in the results.
        
        :*args: An optional list of dicts (to be combined via add_dicts())
        :**kwargs: An optional set of key/value pairs (to be combined via
                   add_dicts())
        :returns: (nothing)
        """
    
        return self.queue.put(yaml.dump(add_dicts(*args, 
                                                 completed_at=time(),
                                                 worker_id=self.worker_id,
                                                 **kwargs)))

    def handle_upload_object(self, object_info):
        object_name = object_info['object_name']
        results = self.ignoring_http_responses((503,), client.put_object, object_info,
                                               name=object_name,
                                               contents='A' * int(object_info['object_size']))
        # Once we've stored an object, note that in case we need to update,
        # read, or delete (an unnamed) object in the future.
        # Furthermore, we'll assume that any object name starting with a
        # capital S, after any forward slashes, is a "stock" object, while
        # anything else is an element of the benchmark "population".  We track
        # them separately and prefer to later operate on population objects vs.
        # stock objects.

        self.add_object_name(
            self.object_name_type(object_name), object_info['container'], object_name,
        )
        self.put_results(object_info,
                         first_byte_latency=results['x-swiftstack-first-byte-latency'],
                         last_byte_latency=results['x-swiftstack-last-byte-latency'],
                        )


    def handle_delete_container(self, container_info):
        self.ignoring_http_responses((404, 503), client.delete_container, container_info)

    def handle_create_container(self, container_info):
        self.ignoring_http_responses((503,), client.put_container, container_info)
        self.put_results(container_info)

    def get_population_or_stock_object_name(self, container):
        # Try to get one from population
        object_name = self.get_object_name('population', container)
        if not object_name:
            # Fall back to stock
            object_name = self.get_object_name('stock', container)
        return object_name

    def handle_delete_object(self, object_info):
        object_name = object_info.get('object_name', None)
        if not object_name:
            object_name = self.get_population_or_stock_object_name(
                object_info['container'],
            )
            if not object_name:
                return

        results = self.ignoring_http_responses((404, 503),
                                               client.delete_object,
                                               object_info, name=object_name)
        self.remove_object_name(
            self.object_name_type(object_name), object_info['container'], object_name,
        )
        self.put_results(object_info,
                         object_name=object_name,
                         first_byte_latency=results['x-swiftstack-first-byte-latency'],
                         last_byte_latency=results['x-swiftstack-last-byte-latency'],
                        )

    def handle_update_object(self, object_info):
        object_name = self.get_population_or_stock_object_name(
            object_info['container'],
        )
        if not object_name:
            return
        results = self.ignoring_http_responses((503,), client.put_object,
                                               object_info, name=object_name,
                                               contents='B' * int(object_info['object_size']))
        self.put_results(object_info,
                         object_name=object_name,
                         first_byte_latency=results['x-swiftstack-first-byte-latency'],
                         last_byte_latency=results['x-swiftstack-last-byte-latency'],
                        )

    def handle_get_object(self, object_info):
        object_name = self.get_population_or_stock_object_name(
            object_info['container'],
        )
        if not object_name:
            return
        results = self.ignoring_http_responses((503,), client.get_object,
                                               object_info, name=object_name)
        self.put_results(object_info,
                         object_name=object_name,
                         first_byte_latency=results[0]['x-swiftstack-first-byte-latency'],
                         last_byte_latency=results[0]['x-swiftstack-last-byte-latency'],
                        )


