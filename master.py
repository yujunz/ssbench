#!/usr/bin/env python

import argparse
import beanstalkc
import sys

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

master.bench_object_creation(auth_url  = args.auth_url,
                             user      = args.user,
                             key       = args.key,
                             container = args.container,
                             size      = args.object_size,
                             count     = args.num_objects)
