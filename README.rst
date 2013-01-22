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

Here is an example JSON scenario file:

::

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

::

  $ ssbench-worker --help
  usage: ssbench-worker [-h] [--qhost QHOST] [--qport QPORT] [--retries RETRIES]
                        [--concurrency CONCURRENCY]
                        worker_id

  Benchmark your Swift installation

  positional arguments:
    worker_id             An integer ID number; must be unique among all workers

  optional arguments:
    -h, --help            show this help message and exit
    --qhost QHOST         beanstalkd host (def: localhost)
    --qport QPORT         beanstalkd port (def: 11300)
    --retries RETRIES     Maximum number of times to retry a job.
    --concurrency CONCURRENCY
                          Number of concurrent connections for this worker

::

  $ ssbench-master -h
  usage: ssbench-master [-h] [--qhost QHOST] [--qport QPORT]
                        {run-scenario,report-scenario} ...

  Benchmark your Swift installation

  positional arguments:
    {run-scenario,report-scenario}
      run-scenario        Run CRUD scenario, saving statistics
      report-scenario     Generate a report from saved scenario statistics

  optional arguments:
    -h, --help            show this help message and exit
    --qhost QHOST         beanstalkd host (default: localhost)
    --qport QPORT         beanstalkd port (default: 11300)

::

  $ ssbench-master run-scenario -h
  usage: ssbench-master run-scenario [-h] -A AUTH_URL -K KEY -U USER -f
                                     SCENARIO_FILE [-s STATS_FILE] [-r]

  optional arguments:
    -h, --help            show this help message and exit
    -A AUTH_URL, --auth-url AUTH_URL
    -K KEY, --key KEY
    -U USER, --user USER
    -f SCENARIO_FILE, --scenario-file SCENARIO_FILE
    -s STATS_FILE, --stats-file STATS_FILE
                          File into which benchmarking statistics will be saved
                          (default: /tmp/ssbench-results/<scenario_name>.stat)
    -r, --no-default-report
                          Suppress the default immediate generation of a
                          benchmark report to STDOUT after saving stats-file
                          (default: False)

::

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

First make sure ``beanstalkd`` is running.

::

  $ beanstalkd -l 127.0.0.1 &

Then, start one or more ``ssbench-worker`` processes.

::

  $ ssbench-worker 1 &
  $ ssbench-worker 2 &

Finally, run one ``ssbench-master`` process which will manage and coordinate
the benchmark run.

::

  $ ssbench-master run-scenario -A http://192.168.22.100/auth/v1.0 -U dev:admin -K admin -f very_small.scenario
  INFO:root:Starting scenario run for u'Small test scenario'
  INFO:root:Creating containers (ssbench_*) with concurrency 10...
  INFO:root:Initializing cluster with stock data (up to 7 concurrent workers)
  INFO:root:Starting benchmark run (up to 7 concurrent workers)
  INFO:root:Deleting population objects from cluster
  INFO:root:Calculating statistics for 500 result items...

  Small test scenario
    C   R   U   D     Worker count:   2   Concurrency:   7
  % 27  36  18  18

  TOTAL
         Count:   500  Average requests per second:  45.5
                             min      max     avg     std_dev   median
         First-byte latency:  0.01 -   0.44    0.11  (  0.09)    0.07  (  all obj sizes)
         Last-byte  latency:  0.01 -   0.44    0.11  (  0.09)    0.07  (  all obj sizes)
         First-byte latency:  0.01 -   0.44    0.11  (  0.09)    0.07  (tiny objs)
         Last-byte  latency:  0.01 -   0.44    0.11  (  0.09)    0.07  (tiny objs)
         First-byte latency:  0.01 -   0.38    0.13  (  0.10)    0.09  (small objs)
         Last-byte  latency:  0.01 -   0.38    0.13  (  0.10)    0.09  (small objs)

  CREATE
         Count:   133  Average requests per second:  12.3
                             min      max     avg     std_dev   median
         First-byte latency:  0.03 -   0.44    0.16  (  0.10)    0.13  (  all obj sizes)
         Last-byte  latency:  0.03 -   0.44    0.16  (  0.10)    0.14  (  all obj sizes)
         First-byte latency:  0.03 -   0.44    0.16  (  0.10)    0.13  (tiny objs)
         Last-byte  latency:  0.03 -   0.44    0.16  (  0.10)    0.13  (tiny objs)
         First-byte latency:  0.08 -   0.38    0.20  (  0.10)    0.23  (small objs)
         Last-byte  latency:  0.08 -   0.38    0.21  (  0.10)    0.23  (small objs)

  READ
         Count:   176  Average requests per second:  16.2
                             min      max     avg     std_dev   median
         First-byte latency:  0.01 -   0.16    0.04  (  0.03)    0.03  (  all obj sizes)
         Last-byte  latency:  0.01 -   0.16    0.04  (  0.03)    0.03  (  all obj sizes)
         First-byte latency:  0.01 -   0.16    0.04  (  0.03)    0.03  (tiny objs)
         Last-byte  latency:  0.01 -   0.16    0.04  (  0.03)    0.03  (tiny objs)
         First-byte latency:  0.01 -   0.08    0.04  (  0.02)    0.04  (small objs)
         Last-byte  latency:  0.01 -   0.08    0.04  (  0.02)    0.04  (small objs)

  UPDATE
         Count:   100  Average requests per second:   9.2
                             min      max     avg     std_dev   median
         First-byte latency:  0.03 -   0.36    0.15  (  0.08)    0.13  (  all obj sizes)
         Last-byte  latency:  0.03 -   0.36    0.15  (  0.08)    0.13  (  all obj sizes)
         First-byte latency:  0.03 -   0.36    0.14  (  0.08)    0.13  (tiny objs)
         Last-byte  latency:  0.03 -   0.36    0.14  (  0.08)    0.13  (tiny objs)
         First-byte latency:  0.06 -   0.33    0.20  (  0.09)    0.21  (small objs)
         Last-byte  latency:  0.08 -   0.33    0.20  (  0.08)    0.21  (small objs)

  DELETE
         Count:    91  Average requests per second:   8.3
                             min      max     avg     std_dev   median
         First-byte latency:  0.02 -   0.33    0.11  (  0.08)    0.12  (  all obj sizes)
         Last-byte  latency:  0.02 -   0.33    0.11  (  0.08)    0.12  (  all obj sizes)
         First-byte latency:  0.02 -   0.33    0.12  (  0.08)    0.12  (tiny objs)
         Last-byte  latency:  0.02 -   0.33    0.12  (  0.08)    0.12  (tiny objs)
         First-byte latency:  0.03 -   0.14    0.07  (  0.04)    0.04  (small objs)
         Last-byte  latency:  0.03 -   0.14    0.07  (  0.04)    0.04  (small objs)
