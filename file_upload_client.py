#!/usr/bin/env python -u

import argparse
from itertools import imap
import os
from Queue import Queue, Empty
import random
import subprocess
import sys
from threading import Thread
import time


UPLOAD_COUNT = 0

def debug(args, msg, *fmt_args):
    if args.debug:
        print ' *** ' + msg % fmt_args


def run(args):
    if args.container_name_file is None:
        container_list = ['file_upload_client_%03d' % i for i in xrange(100)]
    else:
        with open(args.container_name_file, 'r') as container_file:
            container_list = imap(lambda c: c.rstrip(),
                                  container_file.readlines())

    auth_args = []
    if args.auth_url:
        auth_args.extend(['-A', args.auth_url])
    if args.key:
        auth_args.extend(['-K', args.key])
    if args.user:
        auth_args.extend(['-U', args.user])

    keep_running = True
    work_queue = Queue(args.concurrency)

    def thread_worker():
        while keep_running:
            work_item = None
            try:
                work_item = work_queue.get(timeout=1)
            except Empty:
                pass
            if work_item is not None:
                fn = work_item.pop(0)
                try:
                    fn(*work_item)
                except Exception as e:
                    print '### Worker thread got exception %r' % (e,)
                    raise
                finally:
                    work_queue.task_done()

    # Fire up worker threads
    worker_threads = []
    for _ in xrange(args.concurrency):
        work_thread = Thread(target=thread_worker)
        work_thread.start()
        worker_threads.append(work_thread)

    if args.delete_containers:
        print 'DELETEing %d containers...' % (len(container_list),)
        for container_name in container_list:
            debug(args, 'DELETEing to container %s', container_name)
            work_queue.put([
                _run_swift, auth_args, ['delete', container_name],
                subprocess.check_call])
        debug(args, 'joining on work_queue (for container POSTs)')
        work_queue.join()

    if args.create_containers:
        print 'Ensuring %d containers are created...' % (len(container_list),)
        for container_name in container_list:
            debug(args, 'POSTing to container %s', container_name)
            work_queue.put([
                _run_swift, auth_args, ['post', container_name],
                subprocess.check_call])
        debug(args, 'joining on work_queue (for container POSTs)')
        work_queue.join()

    run_start_time = time.time()
    desired_period = 1.0 / args.rate_of_upload
    global UPLOAD_COUNT
    UPLOAD_COUNT = 0
    run_count = 0
    while args.number_of_objects > 0 \
          and run_count <= args.number_of_objects - 1:
        start_time = time.time()

        # Sanity-check that all threads are still running
        if len(filter(lambda t: t.isAlive(),
                      worker_threads)) != args.concurrency:
            print 'WARNING: one or more worker threads exited early; aborting!'
            break

        put_start = time.time()
        work_queue.put([
            upload_object, run_count, auth_args, args, container_list])
        put_elapsed = time.time() - put_start
        if put_elapsed > desired_period:
            debug(args, 'NOTE -- fell behind (%.2fs to put into queue)',
                  put_elapsed)

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

    debug(args, 'setting keep_running = False')
    keep_running = False
    debug(args, 'joining on work_queue')
    work_queue.join()
    for i, work_thread in enumerate(worker_threads, 1):
        debug(args, 'joining thread #%d', i)
        work_thread.join()

    run_elapsed_time = time.time() - run_start_time
    print 'Ran %.1fs total; uploaded %d objects (avg. %.2f objs/sec)' % (
        run_elapsed_time, UPLOAD_COUNT, UPLOAD_COUNT / run_elapsed_time)


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
    global UPLOAD_COUNT
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
        rc = _run_swift(auth_args, swift_args, subprocess.check_call)
        UPLOAD_COUNT += 1
        return rc
    finally:
        debug(args, 'unlinking %s', object_name)
        os.unlink(object_name)



# We need to be able to simulate an upload of a file of random size (between 2
# byte counts) at some specified average frequency (but make it a little
# random, so it's not completely synthetic?).
#
#These files should be uploaded across a random set of containers (numbering around 1 million).

class RawArgumentDefaultsHelpFormatter(argparse.RawDescriptionHelpFormatter,
                                       argparse.ArgumentDefaultsHelpFormatter):
    pass


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

    The <concurrency> option can increase the number of simultaneous
    connections to the cluster.  The parallel threads are only used to maintain
    the desired upload rate specified by <rate-of-upload>.  For example, if you
    wanted 500 uploads per second, you could run this script on 100 servers
    with <rate-of-upload> set to 5.  Even with a <concurrency> setting greater
    than 1, if a single thread can upload objects in less than 200ms, there may
    not actually be more than one concurrent connection to the Swift cluster.
    In other words, <concurrency> is a maximum concurrency, not guaranteed
    concurrency.
    """,
    formatter_class=RawArgumentDefaultsHelpFormatter)
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
arg_parser.add_argument('-C', '--concurrency', type=int,
                        help='Number of uploads to run simultaneously',
                        default=1)
arg_parser.add_argument('-R', '--create-containers', action='store_true',
                        help='Create all containers in static list or '
                             '<container-name-file>',
                        default=False)
arg_parser.add_argument('-D', '--delete-containers', action='store_true',
                        help='Delete all containers in static list or '
                             '<container-name-file>',
                        default=False)

args = arg_parser.parse_args(sys.argv[1:])
run(args)

