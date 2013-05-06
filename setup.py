#!/usr/bin/python
# Copyright (c) 2013 SwiftStack, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import sys
from glob import glob
from setuptools import setup, find_packages

thispath = os.path.dirname(__file__)
sys.path.insert(0, thispath)

import ssbench

with open(os.path.join(thispath, '.requirements.txt'), 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

with open(os.path.join(thispath, '.requirements-test.txt'), 'r') as f:
    test_requires = [x.strip() for x in f if x.strip()]

with open(os.path.join(thispath, 'README.rst'), 'r') as f:
    readme = f.read()

setup(
    name='ssbench',
    version=ssbench.version,
    description='SwiftStack Swift Benchmarking Suite',
    long_description=readme,
    license='Apache License (2.0)',
    author='SwiftStack, Inc.',
    author_email='darrell@swiftstack.com',
    url='http://github.com/SwiftStack/ssbench',
    packages=find_packages(exclude=['ssbench.tests']),
    test_suite='nose.collector',
    tests_require=test_requires,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Telecommunications Industry',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Testing :: Traffic Generation',
        'Topic :: System :: Benchmark',
        'Topic :: Utilities',
    ],
    keywords='openstack swift object storage benchmark',
    install_requires=requires,
    scripts=[
        'bin/ssbench-master',
        'bin/ssbench-worker',
    ],
    data_files=[('share/ssbench/scenarios', glob('scenarios/*')),
                ('share/ssbench', ['CHANGELOG', 'AUTHORS', 'LICENSE'])],
)
