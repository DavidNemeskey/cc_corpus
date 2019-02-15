#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Utility functions."""

import bz2
import gzip
import os
import os.path as op

def openall(
    filename, mode='rt', encoding=None, errors=None, newline=None,
    buffering=-1, closefd=True, opener=None,  # for open()
    compresslevel=5,  # faster default compression
):
    """
    Opens all file types known to the Python SL. There are some differences
    from the stock functions:
    - the default mode is 'rt'
    - the default compresslevel is 5, because e.g. gzip does not benefit a lot
      from higher values, only becomes slower.
    """
    if filename.endswith('.gz'):
        return gzip.open(filename, mode, compresslevel,
                         encoding, errors, newline)
    elif filename.endswith('.bz2'):
        return bz2.open(filename, mode, compresslevel,
                        encoding, errors, newline)
    else:
        return open(filename, mode, buffering, encoding, errors, newline,
                    closefd, opener)


def collect_inputs(inputs):
    """
    Collects all files from the files and directories specified.
    """
    # TODO: glob?
    files = []
    for input in inputs:
        if op.isfile(input):
            files.append(input)
        elif op.isdir(input):
            # TODO this doesn't work with a recursive directory structure
            files.extend([op.join(input, f) for f in os.listdir(input)])
        else:
            raise ValueError('{} is neither a file nor a directory'.format(input))
    return files
