#!/usr/bin/env python

import argparse
import ssbench
import sys

arg_parser = argparse.ArgumentParser(description='Benchmark your Swift installation')
arg_parser.add_argument('--qhost', default="localhost")
arg_parser.add_argument('--qport', default=11300, type=int)

args = arg_parser.parse_args(sys.argv[1:])
print "%r\n" % args
print args.qhost
