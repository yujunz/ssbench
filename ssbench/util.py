# Copyright (c) 2012-2015 SwiftStack, Inc.

import os
import resource


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
