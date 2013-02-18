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

Installation
------------

``ssbench`` has been developed for and tested with Python 2.7 (Python 2.6 might
work if the ``argparse`` module is installed, but I haven't tested that.)

You will first need to make sure Python native extension building works and
install `libevent`_ and `Beanstalkd`_.

On Ubuntu::

  $ sudo apt-get install -y python-dev libevent-dev beanstalkd

On CentOS 6.3, here are some starter instructions.  Because CentOS' system
Python is still 2.6, this won't actually work until ``ssbench`` is made
compatible with Python 2.6 (for starters, ``logging.captureWarnings`` isn't
present in 2.6, apparently).::

  $ sudo rpm -Uvh http://mirror.pnl.gov/epel/6/i386/epel-release-6-8.noarch.rpm
  $ sudo yum install -y gcc python-setuptools python-devel libevent-devel beanstalkd
  $ sudo easy_install pip
  $ sudo pip install argparse
  $ sudo pip install ssbench
  (Note that at this point you'll be using Python 2.6 which may not work.)

On the Mac, Python 2.7, `libevent`_, and `Beanstalkd`_ may all be installed
with Homebrew_.

I have not tested ``ssbench`` against
gevent v1.x, but according to an old `blog post`_, gevent v1.x will
bundle `libev`_ and not require the installation of `libevent`_ or
`libev_`.  If you try ``ssbench`` with gevent 1.x, please let me know how that
goes...

Once the above system dependencies have been satisfied, you may install
this module (``ssbench``) and its Python module dependencies via pip.

You will also need an `OpenStack Swift`_ cluster to benchmark.

.. _`OpenStack Swift`: http://docs.openstack.org/developer/swift/
.. _`libevent`: http://libevent.org/
.. _`blog post`: http://blog.gevent.org/2011/04/28/libev-and-libevent/
.. _`libev`: http://software.schmorp.de/pkg/libev.html
.. _`Homebrew`: http://mxcl.github.com/homebrew/

Scenarios
---------

A "scenario" (sometimes called a "CRUD scenario") is a utf8-encoded JSON file
defining a benchmark run.  Specifically, it defines:

- A ``name`` for the scenario (an arbitrary string)
- A ``sizes`` list of "object size" classes.  Each object size class has a
  ``name``, a ``size_min`` minimum object size, a ``size_max`` maximum object
  size (in bytes), and an
  optional ``crud_profile`` for just this size.  If ``crud_profile`` is not
  given for a size, the top-level ``crud_profile`` will be used.  The
  ``crud_profile`` here is just like the top-level one, an array of 4 numbers
  whose relative sizes determine the percent chance of a Create, Read, Update,
  or Delete operation.  Objects created or updated within an object size
  class will have a size (in bytes) chosen at random uniformly between the
  minimum and maximum sizes.
- An ``initial_files`` dictionary of initial file-counts per size class.  Each
  size class can have zero or
  more objects uploaded *prior* to the benchmark run itself.  The proportion of
  initial files also defines the probability distribution of object sizes
  during the benchmark run itself.  So if a particular object size class is not
  included in ``initial_files`` or has a value of 0 in ``initial_files``, then
  no objects in that size class will be used during the benchmark run.
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
  ``ssbench-worker`` process uses gevent_ to achive very efficeint
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

For each operation of the benchmark run, a size category is first chosen based
on the relative counts for each size category in the ``initial_files``
dictionary.  This probability for each size category appears under the "% Ops"
column in the report.  Then an operation type is chosen based on that size
category's CRUD profile (which can be individually specified or may be
inherited from the "top level" CRUD profile).

If each size category has its own CRUD profile, then the overall CRUD profile
of the benchmark run will be a weighted average between the values in the "%
Ops" column and the CRUD profile of each size category.  This weighted average
CRUD profile is included in the report on the "CRUD weighted average" line.

.. _gevent: http://www.gevent.org/

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

  usage: ssbench-master [-h] [-v] {run-scenario,report-scenario} ...

  Benchmark your Swift installation

  positional arguments:
    {run-scenario,report-scenario}
      run-scenario        Run CRUD scenario, saving statistics. You must supply
                          *either* the -A, -U, and -K options, or the -S and -T
                          options.
      report-scenario     Generate a report from saved scenario statistics

  optional arguments:
    -h, --help            show this help message and exit
    -v, --verbose         Enable more verbose output. (default: False)

The ``run-scenario`` sub-command of ``ssbench-master`` which actually
runs a benchmark scenario::

  $ ssbench-master run-scenario -h
  usage: ssbench-master run-scenario [-h] -f SCENARIO_FILE [--qhost QHOST]
                                     [--qport QPORT] [-A AUTH_URL] [-U USER]
                                     [-K KEY] [-S STORAGE_URL] [-T TOKEN]
                                     [-c COUNT] [-u COUNT] [-o COUNT] [-q]
                                     [--profile] [--noop] [-s STATS_FILE] [-r]
                                     [--pctile PERCENTILE]

  optional arguments:
    -h, --help            show this help message and exit
    -f SCENARIO_FILE, --scenario-file SCENARIO_FILE
    --qhost QHOST         beanstalkd host (default: localhost)
    --qport QPORT         beanstalkd port (default: 11300)
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

First make sure ``beanstalkd`` is running.  Each greenthread in
``ssbench-worker`` used to have its own connection to ``beanstalkd`` which
meant the maximum file descriptor limit for ``beanstalkd`` would probably need
to be raised.  However, now there is just one connection for ``ssbench-master``
and two connections per ``ssbench-worker`` process.::

  $ beanstalkd -l 127.0.0.1 &

Then, start one or more ``ssbench-worker`` processes (each ``ssbench-worker``
process defaults to a maximum gevent-based concurrency of 256, but the
``-c`` option can override that default)::

  $ ssbench-worker 1 &
  $ ssbench-worker 2 &

Finally, run one ``ssbench-master`` process which will manage and coordinate
the benchmark run::
  
  $ ssbench-master run-scenario -f scenarios/very_small.scenario -u 4 -c 100 -o 613 --pctile 90
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
  ...............................................................................
  ...............................................................................
  ...............................................................................
  ...............................................................................
  ...............................................................................
  ...............................................................................
  ...............................................................................
  ............................................................
  INFO:root:Deleting population objects from cluster
  INFO:root:Calculating statistics for 613 result items...
  
  Small test scenario
  Worker count:   1   Concurrency:   4  Ran 2013-02-17 01:20:00 UTC to 2013-02-17 01:20:14 UTC (13s)
  
  % Ops    C   R   U   D       Size Range       Size Name
   91%   % 27  36  18  18        4 kB -  66 kB  tiny
    9%   % 27  36  18  18      100 kB - 200 kB  small
  ---------------------------------------------------------------------
           27  36  18  18      CRUD weighted average
  
  TOTAL
         Count:   613  Average requests per second:  47.3
                              min       max      avg      std_dev  90%-ile                   Worst latency TX ID
         First-byte latency:  0.006 -   0.275    0.040  (  0.048)    0.105  (all obj sizes)  tx21f0a21d5b8743c481e8548210b3617d
         Last-byte  latency:  0.006 -   0.334    0.083  (  0.070)    0.190  (all obj sizes)  txf01ccd23344c4b94b26b24f7afbbb93d
         First-byte latency:  0.006 -   0.275    0.041  (  0.049)    0.107  (    tiny objs)  tx21f0a21d5b8743c481e8548210b3617d
         Last-byte  latency:  0.006 -   0.334    0.084  (  0.071)    0.196  (    tiny objs)  txf01ccd23344c4b94b26b24f7afbbb93d
         First-byte latency:  0.006 -   0.169    0.031  (  0.034)    0.051  (   small objs)  tx48b6768ca9894588b0bdb5e24dec51a2
         Last-byte  latency:  0.015 -   0.239    0.076  (  0.056)    0.169  (   small objs)  tx46463f2296d64fc9a16c541592c7b2ea
  
  CREATE
         Count:   178  Average requests per second:  13.8
                              min       max      avg      std_dev  90%-ile                   Worst latency TX ID
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (all obj sizes)
         Last-byte  latency:  0.025 -   0.334    0.127  (  0.069)    0.227  (all obj sizes)  txf01ccd23344c4b94b26b24f7afbbb93d
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (    tiny objs)
         Last-byte  latency:  0.025 -   0.334    0.128  (  0.070)    0.231  (    tiny objs)  txf01ccd23344c4b94b26b24f7afbbb93d
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (   small objs)
         Last-byte  latency:  0.049 -   0.190    0.108  (  0.044)    0.180  (   small objs)  tx899c24b465a94db79edc08a516675570
  
  READ
         Count:   207  Average requests per second:  16.1
                              min       max      avg      std_dev  90%-ile                   Worst latency TX ID
         First-byte latency:  0.006 -   0.059    0.018  (  0.010)    0.032  (all obj sizes)  tx1aaca8cc64c944088e87ee4a8046bd04
         Last-byte  latency:  0.006 -   0.086    0.025  (  0.014)    0.044  (all obj sizes)  tx9ed06a526c054ef9970828faa62bb60b
         First-byte latency:  0.006 -   0.059    0.018  (  0.010)    0.032  (    tiny objs)  tx1aaca8cc64c944088e87ee4a8046bd04
         Last-byte  latency:  0.006 -   0.066    0.023  (  0.012)    0.041  (    tiny objs)  tx9541abbe77fe4633b367912c5446957d
         First-byte latency:  0.006 -   0.035    0.016  (  0.008)    0.028  (   small objs)  tx2c0a585b9fda4a63be2ffaafe327fe8b
         Last-byte  latency:  0.015 -   0.086    0.040  (  0.017)    0.061  (   small objs)  tx9ed06a526c054ef9970828faa62bb60b
  
  UPDATE
         Count:   123  Average requests per second:   9.5
                              min       max      avg      std_dev  90%-ile                   Worst latency TX ID
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (all obj sizes)
         Last-byte  latency:  0.039 -   0.259    0.119  (  0.062)    0.217  (all obj sizes)  txd0a4ed87775a4e7e980c0ca819da90ca
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (    tiny objs)
         Last-byte  latency:  0.039 -   0.259    0.117  (  0.062)    0.213  (    tiny objs)  txd0a4ed87775a4e7e980c0ca819da90ca
         First-byte latency:  N/A   -   N/A      N/A    (  N/A  )    N/A    (   small objs)
         Last-byte  latency:  0.072 -   0.239    0.134  (  0.063)    0.234  (   small objs)  tx46463f2296d64fc9a16c541592c7b2ea
  
  DELETE
         Count:   105  Average requests per second:   8.1
                              min       max      avg      std_dev  90%-ile                   Worst latency TX ID
         First-byte latency:  0.020 -   0.275    0.083  (  0.062)    0.176  (all obj sizes)  tx21f0a21d5b8743c481e8548210b3617d
         Last-byte  latency:  0.020 -   0.276    0.083  (  0.062)    0.176  (all obj sizes)  tx21f0a21d5b8743c481e8548210b3617d
         First-byte latency:  0.020 -   0.275    0.085  (  0.063)    0.181  (    tiny objs)  tx21f0a21d5b8743c481e8548210b3617d
         Last-byte  latency:  0.020 -   0.276    0.085  (  0.063)    0.181  (    tiny objs)  tx21f0a21d5b8743c481e8548210b3617d
         First-byte latency:  0.030 -   0.169    0.065  (  0.044)    0.149  (   small objs)  tx48b6768ca9894588b0bdb5e24dec51a2
         Last-byte  latency:  0.030 -   0.169    0.065  (  0.044)    0.149  (   small objs)  tx48b6768ca9894588b0bdb5e24dec51a2
  
  INFO:root:Scenario run results saved to /tmp/ssbench-results/Small_test_scenario.2013-02-16.171956.stat
  INFO:root:You may generate a report with:
    ssbench-master report-scenario -s /tmp/ssbench-results/Small_test_scenario.2013-02-16.171956.stat


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

