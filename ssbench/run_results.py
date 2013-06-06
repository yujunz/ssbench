# Copyright (c) 2012-2013 SwiftStack, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging
import msgpack
import threading
from gzip import GzipFile
from Queue import Queue
from cStringIO import StringIO

from ssbench.scenario import Scenario


def _thread_writer(queue, target_file):
    """
    Read blobs off the given queue, writing them to target_file.
    If an empty blob is read, that indicates we're done, and we exit.
    """
    blob = queue.get()
    while blob:
        target_file.write(blob)
        blob = queue.get()


class RunResults:
    def __init__(self, results_file_path):
        self.results_file_path = results_file_path
        self.write_threshold = 1000000  # 1 MB

    def read_results(self):
        if self.results_file_path.endswith('.gz'):
            file_like = GzipFile(self.results_file_path, 'rb')
        else:
            file_like = open(self.results_file_path, 'rb')
        unpacker = msgpack.Unpacker(file_like=file_like)
        scenario = Scenario.unpackb(unpacker)

        return scenario, unpacker

    def start_run(self, scenario):
        self.output_file = open(self.results_file_path, 'wb')
        self.output_file.write(scenario.packb())
        self.raw_results_buffer = StringIO()
        self.raw_results_q = Queue()
        self.raw_results_write_thread = threading.Thread(
            target=_thread_writer, args=(self.raw_results_q,
                                         self.output_file))
        self.raw_results_write_thread.daemon = True
        self.raw_results_write_thread.start()

    def process_raw_results(self, raw_results):
        self.raw_results_buffer.write(raw_results)
        # only call write() in our thread in chunks > self.write_threshold
        if self.raw_results_buffer.tell() > self.write_threshold:
            self.raw_results_q.put(self.raw_results_buffer.getvalue(True))
            self.raw_results_buffer.seek(0)

    def finalize(self):
        logging.debug('Waiting on results file flushing thread...')
        self.raw_results_q.put(self.raw_results_buffer.getvalue(True))
        self.raw_results_q.put('')
        self.raw_results_write_thread.join()
        os.fsync(self.output_file.fileno())
        self.output_file.close()
        self.raw_results_buffer.close()
        self.raw_results_buffer = None
        self.raw_results_q = None
        self.raw_results_write_thread = None
