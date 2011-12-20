#!/usr/bin/env python

import logging
import argparse
import beanstalkc
import ssbench
import sys

from ssbench.worker import Worker

arg_parser = argparse.ArgumentParser(description='Benchmark your Swift installation')
arg_parser.add_argument('worker_id')
arg_parser.add_argument('--qhost', default="localhost")
arg_parser.add_argument('--qport', default=11300, type=int)

args = arg_parser.parse_args(sys.argv[1:])

logging.basicConfig(
    level=logging.DEBUG
)
logging.captureWarnings(True)

beanq = beanstalkc.Connection(host=args.qhost, port=args.qport)
worker = Worker(beanq, args.worker_id)

worker.go()
