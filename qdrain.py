#!/usr/bin/env python
#
# When you're developing things and something goes wrong, your work
# queue can wind up full of bogus jobs. Use this to just wipe them all out.
import argparse
import beanstalkc
import sys

from ssbench.constants import *


arg_parser = argparse.ArgumentParser(description='Drain the queue')
arg_parser.add_argument('--qhost', default="localhost")
arg_parser.add_argument('--qport', default=11300, type=int)

args = arg_parser.parse_args(sys.argv[1:])

beanq = beanstalkc.Connection(host=args.qhost, port=args.qport)
beanq.watch(RESULTS_TUBE)
beanq.watch(STATS_TUBE)

job = beanq.reserve(timeout=1)
while job:
    job.delete()
    print ".",
    job = beanq.reserve(timeout=1)
