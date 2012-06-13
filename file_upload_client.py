#!/usr/bin/env python -u

import argparse
from itertools import imap
import os
import random
import subprocess
import sys
import time


def debug(args, msg, *fmt_args):
    if args.debug:
        print ' *** ' + msg % fmt_args


def run(args):
    if args.container_name_file is None:
        container_list = ['file_upload_client_%03d' % i for i in xrange(100)]
    else:
        with open(args.container_name_file, 'r') as container_file:
            container_list = container_file.readlines()

    auth_args = []
    if args.auth_url:
        auth_args.extend(['-A', args.auth_url])
    if args.key:
        auth_args.extend(['-K', args.key])
    if args.user:
        auth_args.extend(['-U', args.user])

    if args.create_containers:
        for container_name in container_list:
            _run_swift(auth_args, ['post', container_name],
                       subprocess.check_output)

    desired_period = 1.0 / args.rate_of_upload
    run_count = 0
    while args.number_of_objects > 0 and run_count <= args.number_of_objects:
        start_time = time.time()

        upload_object(run_count, auth_args, args, container_list)

        elapsed_time = time.time() - start_time
        sleep_time = desired_period - elapsed_time
        if sleep_time > 0:
            jitter_total = sleep_time * float(args.jitter_percent) / 100.0
            debug(args, 'sleep_time: %5.3fs\tjitter_total: %5.3fs',
                  sleep_time, jitter_total)
            sleep_time += 0.5 * jitter_total
            sleep_time -= random.random() * jitter_total
            debug(args, 'sleeping %5.3fs', sleep_time)
            time.sleep(sleep_time)
        else:
            debug(args, 'sleep_time was %5.3fs (skipping sleep!)', sleep_time)
        run_count += 1


def _run_swift(auth_args, extra_swift_args, runner_fn):
    swift_args = [
        args.swift_binary,
        '-v' if args.verbose else '-q',
    ]
    swift_args.extend(auth_args)
    swift_args.extend(extra_swift_args)
    debug(args, '%s', ' '.join(swift_args))
    return runner_fn(swift_args)


def upload_object(index, auth_args, args, container_list):
    container = random.choice(container_list)
    object_name = '%s_%05d' % (container, index)
    object_size = args.lower_file_size + int(
        (args.upper_file_size - args.lower_file_size) * random.random()
    )
    debug(args, 'writing %d bytes to %s', object_size, object_name)
    with open(object_name, 'wb', object_size) as object_file:
        object_file.writelines(imap(str, xrange(object_size)))
    swift_args = [
        'upload', container, object_name
    ]
    try:
        _run_swift(auth_args, swift_args, subprocess.check_call)
    finally:
        debug(args, 'unlinking %s', object_name)
        os.unlink(object_name)


# We need to be able to simulate an upload of a file of random size (between 2
# byte counts) at some specified average frequency (but make it a little
# random, so it's not completely synthetic?).
#
#These files should be uploaded across a random set of containers (numbering around 1 million).

arg_parser = argparse.ArgumentParser(
    description="""Object uploader:

    This script uploads <number-of-objects> objects (whose size is randomly chosen
    between <lower-file-size> and <upper-file-size>) into a Swift cluster at a
    frequency close to <rate-of-upload>, as randomly modified by a percentage
    of "jitter" (<jitter-percent>).

    Each file is uploaded into a container randomly chosen from a set of
    container names in the file specified by <container-name-file>.  If that
    option is not specified, a built-in static list of 100 containers will be
    used.
    """,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
arg_parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbosely print status output',
                        default=False)
arg_parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging',
                        default=False)
arg_parser.add_argument('-A', '--auth-url', type=str)
arg_parser.add_argument('-K', '--key', type=str)
arg_parser.add_argument('-U', '--user', type=str)
arg_parser.add_argument('-l', '--lower-file-size', type=int,
                        help='Lower-bound of upload file size',
                        default=3*1024)
arg_parser.add_argument('-u', '--upper-file-size', type=int,
                        help='Upper-bound of upload file size',
                        default=64*1024)
arg_parser.add_argument('-r', '--rate-of-upload', type=float,
                        help='Object-per-second rate of upload (floating '
                              'point ok)',
                        default=1.0)
arg_parser.add_argument('-j', '--jitter-percent', type=int,
                        help='Percent amount to randomly adjust upload timing',
                        default=5)
arg_parser.add_argument('-n', '--number-of-objects', type=int,
                        help='Number of objects to upload before exiting (0 '
                        'runs forever)', default=120)
arg_parser.add_argument('-s', '--swift-binary', type=str,
                        help='Path to the swift command-line client',
                        default='/usr/bin/swift')
arg_parser.add_argument('-c', '--container-name-file', type=str,
                        help='Path to a file containing container names; '
                             '(if not given, a built-in static list of 100 '
                             'containers will be used)',
                        default=None)
arg_parser.add_argument('-C', '--create-containers', action='store_true',
                        help='Create all containers in static list or '
                             '<container-name-file>',
                        default=False)

args = arg_parser.parse_args(sys.argv[1:])
run(args)

