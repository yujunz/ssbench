# Copyright (c) 2012-2015 SwiftStack, Inc.

import math
import os
import resource
import socket


def add_dicts(*args, **kwargs):
    """
    Utility to "add" together zero or more dicts passed in as positional
    arguments with kwargs.  The positional argument dicts, if present, are not
    mutated.
    """
    result = {}
    for d in args:
        result.update(d)
    result.update(kwargs)
    return result


def raise_file_descriptor_limit():
    _, hard_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
    nofile_target = hard_nofile
    if os.geteuid() == 0:
        nofile_target = 1024 * 64
    # Now bump up max filedescriptor limit as high as possible
    while True:
        try:
            hard_nofile = nofile_target
            resource.setrlimit(resource.RLIMIT_NOFILE,
                               (nofile_target, hard_nofile))
        except ValueError:
            nofile_target /= 1024
        break


def is_ipv6(addr):
    """
    For hostnames, we will use IPv4, if both IPv4 and IPv6 are present as
    results of getaddrinfo().
    """
    sockaddrs = socket.getaddrinfo(addr, None)
    if any([addr_tuple[0] == socket.AF_INET for addr_tuple in sockaddrs]):
        return False
    return True


def mean(iterable):
    if not iterable:
        return None
    return sum(iterable) / len(iterable)


def uncorrected_stdev(iterable):
    """
    This implementation of standard deviation uses an uncorrected variance
    computation. This is not the population sample variance (does not include
    Bessel's correction), but rather the formula for the finite population. It
    is exactly what statlib.stats.lsamplestdev would compute.
    """
    if not iterable:
        return None
    if len(iterable) == 1:
        return 0
    iter_mean = mean(iterable)
    deltas = [(x - iter_mean) ** 2 for x in iterable]
    variance = float(sum(deltas)) / len(iterable)
    return math.sqrt(variance)


def median(iterable):
    if not iterable:
        return None
    sorted_list = sorted(iterable)
    if len(sorted_list) % 2 == 1:
        return sorted_list[len(sorted_list) / 2]
    else:
        right_median = sorted_list[len(sorted_list) / 2]
        left_median = sorted_list[len(sorted_list) / 2 - 1]
        return (right_median + left_median) / 2.0
