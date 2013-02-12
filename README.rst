ssbench
=======

A benchmarking suite for the OpenStack Swift object storage system.

The ``ssbench-master run-scenario`` command will run benchmark "scenarios"
against an
OpenStack Swift cluster, utilizing one or more distributed ``ssbench-worker``
processes, saving statistics about the run to a file.  The ``ssbench-master
report-scenario`` command can then generate a
report from the saved statstics.  By default, ``ssbench-master run-scenario``
will generate a report to STDOUT immediately following a benchmark run in
addition to saving the results to a file.

Coordination between the ``ssbench-master`` and one or more ``ssbench-worker``
processes is managed through a Beanstalkd_ queue.  This additional dependency
allows ``ssbench-master`` to distribute the benchmark run across many, many
client servers while still coordinating the entire run.

.. _Beanstalkd: http://kr.github.com/beanstalkd/

Scenarios
---------

A "scenario" (sometimes called a "CRUD scenario") is a utf8-encoded JSON file
defining a benchmark run.  Specifically, it defines:

- A ``name`` for the scenario (an arbitrary string)
- A ``sizes`` list of "object size" classes.  Each object size class has a
  name, a minimum object size
  and a maximum object size (in bytes).  Objects created or updated within an
  object size
  class will have a size (in bytes) chosen at random uniformly between the
  minimum and maximum sizes.
- An ``initial_files`` list of initial file-counts per size class.  Each size
  class can have zero or
  more objects uploaded *prior* to the benchmark run itself.  The proportion of
  initial files also defines the probability distribution of object sizes
  during the benchmark run itself.  So if a particular object size class has
  a value of 0 in ``initial_files``, then no objects in that size class will
  be used by a benchmark run.
- An ``operation_count`` of operations to perform during the benchmark run.
  An operation is
  either a CREATE, READ, UPDATE, or DELETE of an object.  This value may be
  overridden for any given run with the ``-o COUNT`` flag to ``ssbench-master
  run-scenario``.
- A ``crud_profile`` which determines the distribution of each kind of operation.
  For instance, ``[3, 4, 2, 2]`` would mean 27% CREATE, 36% READ, 18% UPDATE,
  and 18% DELETE.
- A ``user_count`` which determines the maxiumum client concurrency during the
  benchmark run.  The user is responsible for ensuring there are enough workers
  running to support the scenario's defined ``user_count``.  (Each
  ``ssbench-worker`` process uses eventlet_ to achive very efficeint
  concurrency for the benchmark client requests.)  This value may be overridden
  for any given run with the ``-u COUNT`` flag to ``ssbench-master
  run-scenario``.
- A ``container_count`` which determines how many Swift containers are used for
  the benchmark run.  This key is optional in the scenario file and defaults to
  100.  This value may be overridden for any given run with the ``-c
  COUNT`` flag to ``ssbench-master run-scenario``.
- A ``container_concurrency`` value which determines the level of client
  concurrency used by ``ssbench-master`` to create the benchmark containers.
  This value is optional and defaults to 10.

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

**Beware:** hand-editing JSON is error-prone.  Watch out for trailing
commas, in particular.

Installation
------------

You may install this module (``ssbench``) and its dependencies via pip.
You will also need Beanstalkd_ installed and running and an
`OpenStack Swift`_ cluster to benchmark.

.. _`OpenStack Swift`: http://docs.openstack.org/developer/swift/

Usage
-----

The ``ssbench-worker`` script::

  $ ssbench-worker --help
  usage: ssbench-worker [-h] [--qhost QHOST] [--qport QPORT] [-c CONCURRENCY]
                        [--retries RETRIES] [-p COUNT] [-v]
                        worker_id

  Benchmark your Swift installation

  positional arguments:
    worker_id             An integer ID number; must be unique among all workers

  optional arguments:
    -h, --help            show this help message and exit
    --qhost QHOST         beanstalkd host (default: 127.0.0.1)
    --qport QPORT         beanstalkd port (default: 11300)
    -c CONCURRENCY, --concurrency CONCURRENCY
                          Maximum concurrency this worker will provide.
                          (default: 256)
    --retries RETRIES     Maximum number of times to retry a job. (default: 10)
    -p COUNT, --profile-count COUNT
                          Profile COUNT work jobs, starting with the first.
                          (default: 0)
    -v, --verbose         Enable more verbose output. (default: False)

Basic usage of ``ssbench-master`` (requires one sub-command of
``run-scenario`` to actually run a benchmark scenario, or
``report-scenario`` to report on an existing scenario result data file::

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
  usage: ssbench-master run-scenario [-h] -f SCENARIO_FILE [-A AUTH_URL]
                                     [-U USER] [-K KEY] [-S STORAGE_URL]
                                     [-T TOKEN] [-c COUNT] [-u COUNT] [-o COUNT]
                                     [-q] [--profile] [--noop] [-s STATS_FILE]
                                     [-r] [--pctile PERCENTILE]

  optional arguments:
    -h, --help            show this help message and exit
    -f SCENARIO_FILE, --scenario-file SCENARIO_FILE
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
    -c COUNT, --container-count COUNT
                          Override the container count specified in the scenario
                          file. (default: value from scenario)
    -u COUNT, --user-count COUNT
                          Override the user count (concurrency) specified in the
                          scenario file. (default: value from scenario)
    -o COUNT, --op-count COUNT
                          Override the operation count specified in the scenario
                          file. (default: value from scenario)
    -q, --quiet           Suppress most output (including progress characters
                          during run). (default: False)
    --profile             Profile the main benchmark run. (default: False)
    --noop                Exercise benchmark infrastructure without talking to
                          cluster. (default: False)
    -s STATS_FILE, --stats-file STATS_FILE
                          File into which benchmarking statistics will be saved
                          (default: /tmp/ssbench-
                          results/<scenario_name>.<timestamp>.stat)
    -r, --no-default-report
                          Suppress the default immediate generation of a
                          benchmark report to STDOUT after saving stats-file
                          (default: False)
    --pctile PERCENTILE   Report on the N-th percentile, if generating a report.
                          (default: 95)

The ``report-scenario`` sub-command of ``ssbench-master`` which can report on a
previously-run benchmark scenario::

  $ ssbench-master report-scenario -h
  usage: ssbench-master report-scenario [-h] -s STATS_FILE [-f REPORT_FILE]
                                        [--pctile PERCENTILE] [-r RPS_HISTOGRAM]

  optional arguments:
    -h, --help            show this help message and exit
    -s STATS_FILE, --stats-file STATS_FILE
                          An existing stats file from a previous --run-scenario
                          invocation (default: None)
    -f REPORT_FILE, --report-file REPORT_FILE
                          The file to which the report should be written
                          (default: <open file '<stdout>', mode 'w' at
                          0x1002511e0>)
    --pctile PERCENTILE   Report on the N-th percentile. (default: 95)
    -r RPS_HISTOGRAM, --rps-histogram RPS_HISTOGRAM
                          Also write a CSV file with requests completed per
                          second histogram data (default: None)


Example Run
-----------

First make sure ``beanstalkd`` is running.  Note that you may need to ensure
its maximum file descriptor limit is raised, which may require root
privileges::

  $ sudo bash -c 'ulimit -n 8096; beanstalkd -l 127.0.0.1 &'

Then, start one or more ``ssbench-worker`` processes (each ``ssbench-worker``
process defaults to a maximum eventlet-based concurrency of 256, but the
``-c`` option can override that default)::

  $ ssbench-worker 1 &
  $ ssbench-worker 2 &

Finally, run one ``ssbench-master`` process which will manage and coordinate
the benchmark run::

  $ ssbench-master run-scenario -f scenarios/very_small.scenario -u 4 -c 100 --pctile 90
  INFO:root:Starting scenario run for "Small test scenario"
  INFO:root:Ensuring 100 containers (ssbench_*) exist; concurrency=10...
  INFO:root:Initializing cluster with stock data (up to 4 concurrent workers)
  INFO:root:Starting benchmark run (up to 4 concurrent workers)
  Benchmark Run:
    .  <  1s first-byte-latency
    o  <  3s first-byte-latency
    O  < 10s first-byte-latency
    * >= 10s first-byte-latency
    X    work job raised an exception
    _    no first-byte-latency available
  ..............................................................................
  ..............................................................................
  ..............................................................................
  ..............................................................................
  ..............................................................................
  ..............................................................................
  ................................
  INFO:root:Deleting population objects from cluster
  INFO:root:Calculating statistics for 500 result items...

  Small test scenario
    C   R   U   D       Worker count:   1   Concurrency:   4
  % 27  36  18  18      Ran 2013-02-03 23:14:38 UTC to 2013-02-03 23:14:45 UTC (6s)

  TOTAL
         Count:   500  Average requests per second:  84.3
                              min       max      avg      std_dev  90%-ile                   Swift TX ID for worst latency
         First-byte latency:  0.009 -   0.065    0.026  (  0.011)    0.043  (all obj sizes)  txa174575811d04e3bbfffa3daba1e9b86
         Last-byte  latency:  0.009 -   0.117    0.046  (  0.026)    0.084  (all obj sizes)  tx6892be9922014ec2917309f5efa0dbee
         First-byte latency:  0.009 -   0.065    0.025  (  0.011)    0.042  (    tiny objs)  txa174575811d04e3bbfffa3daba1e9b86
         Last-byte  latency:  0.009 -   0.117    0.045  (  0.025)    0.081  (    tiny objs)  txc49bedd478594e24a93c33f087ae243a
         First-byte latency:  0.011 -   0.052    0.029  (  0.011)    0.043  (   small objs)  tx1119d8ca1f5b47fe8f1bf7e0d833ef86
         Last-byte  latency:  0.016 -   0.117    0.057  (  0.029)    0.099  (   small objs)  tx6892be9922014ec2917309f5efa0dbee

  CREATE
         Count:   133  Average requests per second:  22.7
                              min       max      avg      std_dev  90%-ile                   Swift TX ID for worst latency
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (all obj sizes)
         Last-byte  latency:  0.024 -   0.117    0.070  (  0.018)    0.093  (all obj sizes)  tx6892be9922014ec2917309f5efa0dbee
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (    tiny objs)
         Last-byte  latency:  0.024 -   0.117    0.069  (  0.018)    0.091  (    tiny objs)  txc49bedd478594e24a93c33f087ae243a
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (   small objs)
         Last-byte  latency:  0.059 -   0.117    0.087  (  0.019)    0.117  (   small objs)  tx6892be9922014ec2917309f5efa0dbee

  READ
         Count:   187  Average requests per second:  31.7
                              min       max      avg      std_dev  90%-ile                   Swift TX ID for worst latency
         First-byte latency:  0.009 -   0.051    0.021  (  0.008)    0.032  (all obj sizes)  txb73b670e9e12433a87c263f6843afec7
         Last-byte  latency:  0.009 -   0.064    0.024  (  0.009)    0.035  (all obj sizes)  tx09466e0009534f2fae0d7087904f7a69
         First-byte latency:  0.009 -   0.051    0.021  (  0.008)    0.031  (    tiny objs)  txb73b670e9e12433a87c263f6843afec7
         Last-byte  latency:  0.009 -   0.053    0.023  (  0.008)    0.032  (    tiny objs)  txb73b670e9e12433a87c263f6843afec7
         First-byte latency:  0.011 -   0.043    0.025  (  0.009)    0.035  (   small objs)  tx474e44b8f8704c929d1e39fa59893401
         Last-byte  latency:  0.016 -   0.064    0.036  (  0.014)    0.053  (   small objs)  tx09466e0009534f2fae0d7087904f7a69

  UPDATE
         Count:    90  Average requests per second:  15.2
                              min       max      avg      std_dev  90%-ile                   Swift TX ID for worst latency
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (all obj sizes)
         Last-byte  latency:  0.023 -   0.117    0.069  (  0.019)    0.089  (all obj sizes)  txb80150d4055e4406a7c373cf0969d7fd
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (    tiny objs)
         Last-byte  latency:  0.023 -   0.117    0.067  (  0.019)    0.089  (    tiny objs)  txb80150d4055e4406a7c373cf0969d7fd
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (   small objs)
         Last-byte  latency:  0.071 -   0.114    0.086  (  0.014)    0.114  (   small objs)  txb5dfc049939047c3ae973f7e94084e5b

  DELETE
         Count:    90  Average requests per second:  15.2
                              min       max      avg      std_dev  90%-ile                   Swift TX ID for worst latency
         First-byte latency:  0.016 -   0.065    0.036  (  0.010)    0.049  (all obj sizes)  txa174575811d04e3bbfffa3daba1e9b86
         Last-byte  latency:  0.017 -   0.065    0.036  (  0.010)    0.049  (all obj sizes)  txa174575811d04e3bbfffa3daba1e9b86
         First-byte latency:  0.018 -   0.065    0.035  (  0.010)    0.049  (    tiny objs)  txa174575811d04e3bbfffa3daba1e9b86
         Last-byte  latency:  0.018 -   0.065    0.035  (  0.010)    0.049  (    tiny objs)  txa174575811d04e3bbfffa3daba1e9b86
         First-byte latency:  0.016 -   0.052    0.037  (  0.011)    0.052  (   small objs)  tx1119d8ca1f5b47fe8f1bf7e0d833ef86
         Last-byte  latency:  0.017 -   0.052    0.037  (  0.011)    0.052  (   small objs)  tx1119d8ca1f5b47fe8f1bf7e0d833ef86

  INFO:root:Scenario run results saved to /tmp/ssbench-results/Small_test_scenario.2013-02-03.151437.stat
  INFO:root:You may generate a report with:
    ssbench-master report-scenario -s /tmp/ssbench-results/Small_test_scenario.2013-02-03.151437.stat


The No-op Mode
--------------

To test the maximum throughput of the ``ssbench-master`` ==> ``beantalkd``
==> ``ssbench-worker`` infrastructure, you can add ``--noop`` to a
``ssbench-master run-scenario`` command and the scenario will be "run" but
the ``ssbench-worker`` processes will not actually talk to the Swift cluster.

In this manner, you may determine your maximum requests per second if talking
to the Swift cluster were free.

The reported "Average requests per second:" value in the "TOTAL" section of
the report should be higher than you expect to get out of the Swift cluster
itself.  My 2012 15" Retina Macbook Pro can get ~2,700 requests
per second with ``--noop`` using a local beanstalkd, one ``ssbench-worker``,
and a user count (concurrency) of 4.


Contributing to ssbench
-----------------------

First, please use the Github Issues for the project when submitting bug reports
or feature requests.

Code submissions should be submitted as pull requests and all code should be
PEP8 (v. 1.4.2) compliant.  Current unit test line coverage is not 100%, but
code contributions should not *lower* the code coverage (so please include
new tests or update existing ones as part of your change).

If contributing code which implements a feature or fixes
a bug, please ensure a Github Issue exists prior to submitting the pull request
and reference the Issue number in your commit message.

When submitting your first pull request, please also update AUTHORS to include
yourself, maintaining alphabetical ordering by last name.

If any of the file(s) you change do not yet have a copyright line with your
name, please add one at the bottom of the others, above the license text (but
never remove any existing copyright lines).  Your copyright line should look
something like::

  # Copyright (c) 2013 FirstName LastName

