#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Utility functions."""

import bz2
import gzip
import os
import os.path as op
import pickle

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


def unpickle_stream(inf):
    """
    Wraps the while loop of loading stuff with pickle from a stream so that
    the user can use a for loop instead.
    """
    try:
        while True:
            yield pickle.load(inf)
    except EOFError:
        return


class NoEmptyWriteWrapper:
    """
    A file object wrapper deletes the file on close() if if was opened for
    writing but nothing was written. Useful for file filtering tasks.

    Truth be told, instead of this, a lazy file object (that only creates the
    actual file on the first write()) would be much better, but it is much
    more difficult to implement, due to the complexity of the io classes.
    """
    def __init__(self, f):
        self._f = f
        self._written = 0

    def write(self, data):
        written = self._f.write(data)
        self._written += written
        return written

    def close(self):
        self._f.close()
        if self._written == 0:
            if 'w' in self._f.mode:
                os.remove(self._f.name)

    def __enter__(self):
        """
        Enter needs to be defined, because for some reason, providing it through
        __getattribute__ is not enough. Maybe because that returns the wrapped
        object's __enter__; unfortunately, all we get is an AttributeError, so
        it's hard to tell.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """See __enter__."""
        self.close()

    def __getattribute__(self, name):
        """
        Returns the methods and data members defined in the object. Forwards
        everything else to the wrapped file object.
        """
        if name in ['_f', '_written', '__enter__', '__exit__', 'write', 'close']:
            return super().__getattribute__(name)
        else:
            return getattr(self._f, name)


def notempty(f):
    """Wraps f, a file object, in a NoEmptyWriteWrapper."""
    return NoEmptyWriteWrapper(f)


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
