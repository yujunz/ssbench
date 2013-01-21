#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)
logging.captureWarnings(True)

import sys
import argparse

from ssbench.worker import Worker

arg_parser = argparse.ArgumentParser(
    description='Benchmark your Swift installation')
arg_parser.add_argument('worker_id', type=int, help='An integer ID number; '
                        'must be unique among all workers')
arg_parser.add_argument(
    '--qhost', default="localhost", help='beanstalkd host (def: localhost)')
arg_parser.add_argument(
    '--qport', default=11300, type=int, help='beanstalkd port (def: 11300)')
arg_parser.add_argument(
    '--retries', default=10, type=int,
    help='Maximum number of times to retry a job.')
arg_parser.add_argument(
    '--concurrency', default=10, type=int,
    help='Number of concurrent connections for this worker')

args = arg_parser.parse_args(sys.argv[1:])

worker = Worker(args.qhost, args.qport, args.worker_id, args.retries,
                args.concurrency)
worker.go()
