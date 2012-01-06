#!/usr/bin/env python

import logging
import argparse
import beanstalkc
import math
import sys
import pickle
from datetime import datetime

from collections import Counter
from ssbench.master import Master
from ssbench.scenario import Scenario

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
    print_unixtime_histogram(results)

def create_containers(master, args):
    results = master.bench_container_creation(auth_url  = args.auth_url,
                                              user      = args.user,
                                              key       = args.key,
                                              count     = args.num_containers)
    print_unixtime_histogram(results)

def run_scenario(master, args):
    scenario = Scenario(args.scenario_file)

    # Possibly attempt open prior to benchmark run so we get errors earlier
    # if there's a problem.
    if not args.stats_file:
        args.stats_file = open('results/%s.stat' % scenario.name, 'w+')

    results = master.run_scenario(auth_url=args.auth_url, user=args.user,
                                  key=args.key, scenario=scenario)

    pickle.dump([scenario, results], args.stats_file)

    if not args.no_default_report:
        args.stats_file.flush()
        args.stats_file.seek(0)
        report_fname = 'results/%s_%s.txt' % (
          scenario.name, datetime.now().isoformat('_'))
        args.report_file = open(report_fname, 'w')
        args.rps_histogram = args.report_file
        report_scenario(master, args)
        args.report_file.close()  # we know we can close it
    args.stats_file.close()

def report_scenario(master, args):
    scenario, results = pickle.load(args.stats_file)
    stats = master.calculate_scenario_stats(results)
    args.report_file.write(master.generate_scenario_report(scenario, stats))
    if args.rps_histogram:
        master.write_rps_histogram(stats, args.rps_histogram)
        # Note: not explicitly closing here in case it's redirected to STDOUT (i.e. "-")

def add_swift_args(parser):
    parser.add_argument('-A', '--auth-url', required=True)
    parser.add_argument('-K', '--key', required=True)
    parser.add_argument('-U', '--user', required=True)

logging.basicConfig(level=logging.DEBUG)

arg_parser = argparse.ArgumentParser(description='Benchmark your Swift installation')
arg_parser.add_argument('--qhost', default="localhost", help='beanstalkd host (def: localhost)')
arg_parser.add_argument('--qport', default=11300, type=int, help='beanstalkd port (def: 11300)')

subparsers = arg_parser.add_subparsers()

run_scenario_arg_parser = subparsers.add_parser("run-scenario", help="Run CRUD scenario, saving statistics")
add_swift_args(run_scenario_arg_parser)
run_scenario_arg_parser.add_argument('-f', '--scenario-file', required=True, type=str)
run_scenario_arg_parser.add_argument('-s', '--stats-file',
                                     type=argparse.FileType('w+'),
                                     help='File into which benchmarking statistics will be saved (def: %s)' % (
                                         'results/<scenario_filename>.stat'
                                     ))
run_scenario_arg_parser.add_argument('-r', '--no-default-report',
                                     action='store_true',
                                     default=False,
                                     help="Immediately generate a report to STDOUT after saving --stats-file (def: True)")
run_scenario_arg_parser.set_defaults(func=run_scenario)

report_scenario_arg_parser = subparsers.add_parser("report-scenario",
                                                   help="Generate a report from saved scenario statistics")
report_scenario_arg_parser.add_argument('-s', '--stats-file',
                                        type=argparse.FileType('r'),
                                        required=True,
                                        help='An existing stats file from a previous --run-scenario invocation')
report_scenario_arg_parser.add_argument('-f', '--report-file',
                                        type=argparse.FileType('w'),
                                        default=sys.stdout,
                                        help='The file to which the report should be written (def: STDOUT)')
report_scenario_arg_parser.add_argument('-r', '--rps-histogram',
                                        type=argparse.FileType('w'),
                                        help='Also write a CSV file with requests completed per second histogram data')
report_scenario_arg_parser.set_defaults(func=report_scenario)


create_objects_arg_parser = subparsers.add_parser("create-objects", help="create a bunch of objects")
add_swift_args(create_objects_arg_parser)
create_objects_arg_parser.add_argument(
    '-C', '--containers',
    default=None,
    type=int,
    help="Number of containers (default is #objects / %d)" % DEFAULT_OBJECTS_PER_CONTAINER)
create_objects_arg_parser.add_argument('-n', '--num-objects', default=100, type=int)
create_objects_arg_parser.add_argument('-s', '--object-size', default=2**20, type=int)
create_objects_arg_parser.set_defaults(func=create_objects)

create_containers_arg_parser = subparsers.add_parser("create-containers", help="create a bunch of containers")
add_swift_args(create_containers_arg_parser)
create_containers_arg_parser.add_argument('-n', '--num-containers', default=100, type=int)
create_containers_arg_parser.set_defaults(func=create_containers)


args = arg_parser.parse_args(sys.argv[1:])
beanq = beanstalkc.Connection(host=args.qhost, port=args.qport)
master = Master(beanq)

args.func(master, args)

def print_unixtime_histogram(results):
    counted = Counter([int(item['completed_at']) for item in results])
    print "unixtime,count"
    for item in sorted(set(counted.elements())):
        print "%s,%s" % (item, counted[item])
