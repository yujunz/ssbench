#!/usr/bin/env python

import argparse
import beanstalkc
import math
import sys

from collections import Counter
from ssbench.master import Master

DEFAULT_OBJECTS_PER_CONTAINER = 1000

def num_containers_for(object_count):
    return int(math.ceil(float(object_count) / DEFAULT_OBJECTS_PER_CONTAINER))

def container_names(container_count):
    return [("benchcontainer%d" % (x,)) for x in range(container_count)]

def create_objects(master, args):
    container_count = args.containers or num_containers_for(args.num_objects)
    names = container_names(container_count)

    results = master.bench_object_creation(auth_url     = args.auth_url,
                                           user         = args.user,
                                           key          = args.key,
                                           size         = args.object_size,
                                           containers   = names,
                                           object_count = args.num_objects)

    return Counter([int(item['completed_at']) for item in results])

def create_containers(master, args):
    results = master.bench_container_creation(auth_url  = args.auth_url,
                                              user      = args.user,
                                              key       = args.key,
                                              count     = args.num_containers)

    return Counter([int(item['completed_at']) for item in results])

arg_parser = argparse.ArgumentParser(description='Benchmark your Swift installation')
arg_parser.add_argument('--qhost', default = "localhost")
arg_parser.add_argument('--qport', default = 11300, type = int)
arg_parser.add_argument('-A', '--auth-url', required = True)
arg_parser.add_argument('-K', '--key', required = True)
arg_parser.add_argument('-U', '--user', required = True)

subparsers = arg_parser.add_subparsers(help="create-objects | create-containers")

create_objects_arg_parser = subparsers.add_parser("create-objects", help="create a bunch of objects")
create_objects_arg_parser.add_argument(
    '-C', '--containers',
    default = None,
    type = int,
    help = "Number of containers (default is #objects / %d)" % DEFAULT_OBJECTS_PER_CONTAINER)
create_objects_arg_parser.add_argument('-n', '--num-objects', default = 100, type = int)
create_objects_arg_parser.add_argument('-s', '--object-size', default = 2**20, type = int)
create_objects_arg_parser.set_defaults(func=create_objects)

create_containers_arg_parser = subparsers.add_parser("create-containers", help="create a bunch of containers")
create_containers_arg_parser.add_argument('-n', '--num-containers', default = 100, type = int)
create_containers_arg_parser.set_defaults(func=create_containers)

args = arg_parser.parse_args(sys.argv[1:])
beanq = beanstalkc.Connection(host = args.qhost, port = args.qport)
master = Master(beanq)

counted = args.func(master, args)

print "unixtime,count"
for item in sorted(set(counted.elements())):
    print "%s,%s" % (item, counted[item])
