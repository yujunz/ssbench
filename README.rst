ssbench
=======

A benchmarking suite for the OpenStack Swift object storage system.

The ``ssbench`` suite can run benchmark "scenarios" against an OpenStack Swift
cluster, saving statistics about the run to a file.  It can then generate a
report from the saved statstics.  By default, a report will be generated to
STDOUT immediately following a benchmark run in addition to saving the results
to a file.

Coordination between the ``ssbench-master`` and one or more ``ssbench-worker``
processes is managed through a Beanstalkd_ queue.

.. _Beanstalkd: http://kr.github.com/beanstalkd/

Scenarios
---------

A "scenario" (sometimes called a "CRUD scenario") is a JSON file defining a
benchmark run.  Specifically, it defines:

- A name for the scenario (an arbitrary string)
- A set of "object size" classes.  Each class has a name, a minimum object size
  and a maximum object size.  Objects used within an object size class will
  have a size (in bytes) chosen at random uniformly between the minimum and
  maximum sizes.
- A count of initial files per size class.  Each size class can have zero or
  more objects uploaded *prior* to the benchmark run itself.  The proportion of
  initial files also defines the probability distribution of object sizes
  during the benchmark run itself.
- A count of operations to perform during the benchmark run.  An operation is
  either a CREATE, READ, UPDATE, or DELETE of an object.
- A "CRUD profile" which determines the distribution of each kind of operation.
  For instance, ``[3, 4, 2, 2]`` would mean 27% CREATE, 36% READ, 18% UPDATE,
  and 18% DELETE.
- A ``user_count`` which determines the maxiumum client concurrency during the
  benchmark run.  The user is responsible for ensuring there are enough workers
  running to support the scenario's defined ``user_count``.  (Each
  ``ssbench-worker`` process uses eventlet_ to achive very efficeint
  concurrency for the benchmark client requests.)

.. _eventlet: http://eventlet.net/

``ssbench`` comes with a few canned scenarios, but users are encouraged to
experiment and define their own.

Here is an example JSON scenario file::

  {
    "name": "Small test scenario",
    "sizes": [{
      "name": "tiny",
      "size_min": 4096,
      "size_max": 65536
    }, {
      "name": "small",
      "size_min": 100000,
      "size_max": 200000
    }],
    "initial_files": {
      "tiny": 100,
      "small": 10
    },
    "operation_count": 500,
    "crud_profile": [3, 4, 2, 2],
    "user_count": 7
  }

Installation
------------

Install this module (``ssbench``) via pip.  You will also need Beanstalkd_ and
an `OpenStack Swift`_ cluster to benchmark.

.. _`OpenStack Swift`: http://docs.openstack.org/developer/swift/

Usage
-----

The ``ssbench-worker`` script::

  $ ssbench-worker --help
  usage: ssbench-worker [-h] [--qhost QHOST] [--qport QPORT] [-v]
                        [--retries RETRIES]
                        worker_id

  Benchmark your Swift installation

  positional arguments:
    worker_id          An integer ID number; must be unique among all workers

  optional arguments:
    -h, --help         show this help message and exit
    --qhost QHOST      beanstalkd host (default: 127.0.0.1)
    --qport QPORT      beanstalkd port (default: 11300)
    -v, --verbose      Enable more verbose output. (default: False)
    --retries RETRIES  Maximum number of times to retry a job. (default: 10)

Basic usage of ``ssbench-master`` (requires one of ``run-scenario`` to actually
run a benchmark scenario, or ``report-scenario`` to report on an existing
scenario result data file::

  usage: ssbench-master [-h] [--qhost QHOST] [--qport QPORT] [-v]
                        {run-scenario,report-scenario} ...

  Benchmark your Swift installation

  positional arguments:
    {run-scenario,report-scenario}
      run-scenario        Run CRUD scenario, saving statistics. You must supply
                          *either* the -A, -U, and -K options, or the -S and -T
                          options.
      report-scenario     Generate a report from saved scenario statistics

  optional arguments:
    -h, --help            show this help message and exit
    --qhost QHOST         beanstalkd host (default: localhost)
    --qport QPORT         beanstalkd port (default: 11300)
    -v, --verbose         Enable more verbose output. (default: False)

The ``run-scenario`` sub-command of ``ssbench-master`` which actually
runs a benchmark scenario::

  $ ssbench-master run-scenario -h
  usage: ssbench-master run-scenario [-h] [-A AUTH_URL] [-U USER] [-K KEY]
                                     [-S STORAGE_URL] [-T TOKEN]
                                     [-c CONTAINER_COUNT] [-u USER_COUNT] [-q]
                                     -f SCENARIO_FILE [-s STATS_FILE] [-r]

  optional arguments:
    -h, --help            show this help message and exit
    -A AUTH_URL, --auth-url AUTH_URL
                          Auth URL for the Swift cluster under test. (default:
                          http://192.168.22.100/auth/v1.0)
    -U USER, --user USER  The X-Auth-User value to use for authentication.
                          (default: dev:admin)
    -K KEY, --key KEY     The X-Auth-Key value to use for authentication.
                          (default: admin)
    -S STORAGE_URL, --storage-url STORAGE_URL
                          A specific X-Storage-Url to use; mutually exclusive
                          with -A, -U, and -K; requires -T (default: None)
    -T TOKEN, --token TOKEN
                          A specific X-Storage-Token to use; mutually exclusive
                          with -A, -U, and -K; requires -S (default: None)
    -c CONTAINER_COUNT, --container-count CONTAINER_COUNT
                          Override the container count specified in the scenario
                          file. (default: value from scenario)
    -u USER_COUNT, --user-count USER_COUNT
                          Override the user count (concurrency) specified in the
                          scenario file. (default: value from scenario)
    -q, --quiet           Suppress most output (including progress characters
                          during run). (default: False)
    -f SCENARIO_FILE, --scenario-file SCENARIO_FILE
    -s STATS_FILE, --stats-file STATS_FILE
                          File into which benchmarking statistics will be saved
                          (default: /tmp/ssbench-results/<scenario_name>.stat)
    -r, --no-default-report
                          Suppress the default immediate generation of a
                          benchmark report to STDOUT after saving stats-file
                          (default: False)

The ``report-scenario`` sub-command of ``ssbench-master`` which can report on a
previously-run benchmark scenario::

  $ ssbench-master report-scenario -h
  usage: ssbench-master report-scenario [-h] -s STATS_FILE [-f REPORT_FILE]
                                        [-r RPS_HISTOGRAM]

  optional arguments:
    -h, --help            show this help message and exit
    -s STATS_FILE, --stats-file STATS_FILE
                          An existing stats file from a previous --run-scenario
                          invocation (default: None)
    -f REPORT_FILE, --report-file REPORT_FILE
                          The file to which the report should be written (def:
                          STDOUT) (default: <open file '<stdout>', mode 'w' at
                          0x1002511e0>)
    -r RPS_HISTOGRAM, --rps-histogram RPS_HISTOGRAM
                          Also write a CSV file with requests completed per
                          second histogram data (default: None)


Example Run
-----------

First make sure ``beanstalkd`` is running.  Note that you may need to ensure
its maximum file descriptor limit is raised, which may require root privileges
and a more complicated invocation than the simple example below::

  $ beanstalkd -l 127.0.0.1 &

Then, start one or more ``ssbench-worker`` processes (each process is currently
hard-coded to a maximum eventlet-based concurrency of 256)::

  $ ssbench-worker 1 &
  $ ssbench-worker 2 &

Finally, run one ``ssbench-master`` process which will manage and coordinate
the benchmark run::

  $ ssbench-master run-scenario -f scenarios/very_small.scenario -c 200 -u 4 -S http://192.168.22.100/v1/AUTH_dev -T AUTH_tkfc57b0bb67f84afbb054fb8db2d034d7 
  INFO:root:Starting scenario run for "Small test scenario"
  INFO:root:Ensuring 200 containers (ssbench_*) exist; concurrency=10...
  INFO:root:Initializing cluster with stock data (up to 4 concurrent workers)
  INFO:root:Starting benchmark run (up to 4 concurrent workers)
  Benchmark Run:
    .  <  1s first-byte-latency
    o  <  3s first-byte-latency
    O  < 10s first-byte-latency
    * >= 10s first-byte-latency
    X    work job raised an exception
    _    no first-byte-latency available
  ....................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................
  INFO:root:Deleting population objects from cluster
  INFO:root:Calculating statistics for 500 result items...

  Small test scenario
    C   R   U   D     Worker count:   2   Concurrency:   4
  % 27  36  18  18

  TOTAL
         Count:   500  Average requests per second:  45.3
                             min      max     avg     std_dev   median
         First-byte latency:  0.01 -   0.33    0.06  (  0.05)    0.04  (  all obj sizes)
         Last-byte  latency:  0.01 -   0.33    0.06  (  0.05)    0.04  (  all obj sizes)
         First-byte latency:  0.01 -   0.33    0.06  (  0.05)    0.04  (tiny objs)
         Last-byte  latency:  0.01 -   0.33    0.06  (  0.05)    0.04  (tiny objs)
         First-byte latency:  0.01 -   0.23    0.07  (  0.05)    0.05  (small objs)
         Last-byte  latency:  0.01 -   0.23    0.07  (  0.06)    0.05  (small objs)

  CREATE
         Count:   144  Average requests per second:  13.1
                             min      max     avg     std_dev   median
         First-byte latency:  0.02 -   0.33    0.09  (  0.05)    0.07  (  all obj sizes)
         Last-byte  latency:  0.02 -   0.33    0.09  (  0.05)    0.07  (  all obj sizes)
         First-byte latency:  0.02 -   0.33    0.09  (  0.05)    0.07  (tiny objs)
         Last-byte  latency:  0.02 -   0.33    0.09  (  0.05)    0.07  (tiny objs)
         First-byte latency:  0.06 -   0.23    0.11  (  0.05)    0.10  (small objs)
         Last-byte  latency:  0.06 -   0.23    0.11  (  0.05)    0.10  (small objs)

  READ
         Count:   178  Average requests per second:  16.5
                             min      max     avg     std_dev   median
         First-byte latency:  0.01 -   0.07    0.02  (  0.01)    0.02  (  all obj sizes)
         Last-byte  latency:  0.01 -   0.07    0.02  (  0.01)    0.02  (  all obj sizes)
         First-byte latency:  0.01 -   0.06    0.02  (  0.01)    0.02  (tiny objs)
         Last-byte  latency:  0.01 -   0.06    0.02  (  0.01)    0.02  (tiny objs)
         First-byte latency:  0.01 -   0.07    0.03  (  0.02)    0.03  (small objs)
         Last-byte  latency:  0.01 -   0.07    0.03  (  0.02)    0.03  (small objs)

  UPDATE
         Count:    85  Average requests per second:   7.8
                             min      max     avg     std_dev   median
         First-byte latency:  0.02 -   0.20    0.08  (  0.05)    0.07  (  all obj sizes)
         Last-byte  latency:  0.02 -   0.20    0.08  (  0.05)    0.07  (  all obj sizes)
         First-byte latency:  0.02 -   0.20    0.08  (  0.05)    0.07  (tiny objs)
         Last-byte  latency:  0.02 -   0.20    0.08  (  0.05)    0.07  (tiny objs)
         First-byte latency:  0.06 -   0.16    0.11  (  0.04)    0.12  (small objs)
         Last-byte  latency:  0.06 -   0.18    0.12  (  0.04)    0.12  (small objs)

  DELETE
         Count:    93  Average requests per second:   8.5
                             min      max     avg     std_dev   median
         First-byte latency:  0.01 -   0.18    0.05  (  0.04)    0.03  (  all obj sizes)
         Last-byte  latency:  0.01 -   0.18    0.05  (  0.04)    0.03  (  all obj sizes)
         First-byte latency:  0.01 -   0.18    0.05  (  0.04)    0.03  (tiny objs)
         Last-byte  latency:  0.01 -   0.18    0.05  (  0.04)    0.03  (tiny objs)
         First-byte latency:  0.02 -   0.05    0.03  (  0.01)    0.02  (small objs)
         Last-byte  latency:  0.02 -   0.05    0.03  (  0.01)    0.02  (small objs)

