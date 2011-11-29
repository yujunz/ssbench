#!/usr/bin/env python

import argparse
import beanstalkc
import sys

from collections import Counter
from ssbench.master import Master

arg_parser = argparse.ArgumentParser(description='Benchmark your Swift installation')
arg_parser.add_argument('--qhost', default = "localhost")
arg_parser.add_argument('--qport', default = 11300, type = int)
arg_parser.add_argument('-A', '--auth-url', required = True)
arg_parser.add_argument('-C', '--container', default = "benchcontainer")
arg_parser.add_argument('-K', '--key', required = True)
arg_parser.add_argument('-U', '--user', required = True)
arg_parser.add_argument('-n', '--num-objects', default = 100, type = int)
arg_parser.add_argument('-s', '--object-size', default = 2**20, type = int)

args = arg_parser.parse_args(sys.argv[1:])

beanq = beanstalkc.Connection(host = args.qhost, port = args.qport)
master = Master(beanq)

results = master.bench_object_creation(auth_url  = args.auth_url,
                                       user      = args.user,
                                       key       = args.key,
                                       container = args.container,
                                       size      = args.object_size,
                                       count     = args.num_objects)

# Trim off the first and last 5% to account for spin-up time and
# tapering-off of the work; we only want the middle
#
# XXX this is broken; if we have 100 results at time t=1, we might
# chop off 50 of them, leaving a stoop-shouldered graph. Just show the
# damn thing and let someone stare at it.
# to_trim = int(round(0.05 * len(results)))
# results = results[to_trim + 1 : len(results) - to_trim - 1]

counted = Counter([int(item['completed_at']) for item in results])

print "unixtime,uploads"
for item in sorted(counted.elements()):
    print "%s,%s" % (item, counted[item])
