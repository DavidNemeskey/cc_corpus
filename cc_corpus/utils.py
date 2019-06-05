#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Utility functions."""

from argparse import ArgumentTypeError
import bz2
import copy
import gzip
import inspect
import io
from itertools import zip_longest
import os
import os.path as op
import pickle

try:
    import idzip
except ImportError:
    idzip = None

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
    if filename.endswith('.dz') and idzip:
        # Unfortunately idzip's API is not very good
        f = idzip.open(filename, mode.replace('t', '').replace('b', '') + 'b')
        if 't' in mode:
            return io.TextIOWrapper(f, encoding, errors,
                                    newline, write_through=True)
        else:
            return f
    elif filename.endswith('.gz') or filename.endswith('.dz'):
        # .dz is .gz, so if we don't have idzip installed, we can still read it
        return gzip.open(filename, mode, compresslevel,
                         encoding, errors, newline)
    elif filename.endswith('.bz2'):
        return bz2.open(filename, mode, compresslevel,
                        encoding, errors, newline)
    else:
        return open(filename, mode, buffering, encoding, errors, newline,
                    closefd, opener)


def file_mode(f):
    """
    Returns the mode in which the file has been opened (with e.g. openall).

    Unfortunately, this is only reliable for streams opened by io.open();
    gzip and bz2 objects only differentiate between read and write modes.
    """
    mode = getattr(f, 'mode', None)
    if mode and isinstance(mode, str):
        return mode
    else:
        if isinstance(f, io.TextIOWrapper):
            mode = 't'
            f = f.buffer
        else:
            mode = 'b'
        if isinstance(f, gzip.GzipFile):
            return ('w' if f.mode == gzip.WRITE else 'r') + mode
        elif isinstance(f, bz2.BZ2File):
            return ('w' if f._mode == bz2._MODE_WRITE else 'r') + mode
        elif idzip and isinstance(f, idzip.IdzipFile):
            return ('w' if 'w' in f.mode else 'r') + mode
        else:
            raise ValueError('Unknown file object type {}'.format(type(f)))


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

    Note that ATM it is only possible to differentiate between 'w' and 'a'
    modes for regular (not gzip or bz2) files.
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
            if 'w' in file_mode(self._f):
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


def host_weight(value):
    """Implements an argument type for argparse that is a string:float tuple."""
    host, _, weight = value.partition(':')
    if weight:
        try:
            weight = float(weight)
        except:
            raise ArgumentTypeError(
                'Must be in the form of host:weight, where weight is a number. '
                'It is optional, though.')
    else:
        weight = 1
    return host, weight


def host_to_path(path, host):
    """
    Adds the host name to the path. If path has a file extension (such as .gz),
    the host name is appended before that; otherwise, it is appended at the
    end of the file / directory name.
    """
    root, ext = op.splitext(path.rstrip(os.sep))
    hosty_path = root + '_{}'.format(host)
    if ext:
        hosty_path += ext
    return hosty_path


class Stats:
    """
    Class that can be used to count various things. The class cannot be used
    as-is; users should create subclasses with the create() class method.
    """
    __slots__ = ()  # So that we don't create a  __dict__

    def __init__(self, *values, **kwvalues):
        """Initializes all fields to 0."""
        if len(values) > len(self.__slots__):
            raise ValueError('Too many arguments to {}(): at most {} '
                             'supported, received {}'.format(
                                 self.__class__.__name__, len(values),
                                 len(self.__slots__)))

        for slot, value in zip_longest(self.__slots__, values, fillvalue=0):
            setattr(self, slot, value)
        for slot, value in kwvalues.items():
            if slot in self.__slots__:
                setattr(self, slot, value)
            else:
                raise ValueError('{} does not have a slot {}'.format(
                    self.__class__.__name__, slot))

    def __iadd__(self, other):
        """+= for all fields."""
        for slot in self.__slots__:
            setattr(self, slot, getattr(self, slot) + getattr(other, slot))
        return self

    def __add__(self, other):
        """+ for all fields."""
        ret = copy.copy(self)
        ret += other
        return ret

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except:
            raise KeyError('No slot called `{}`'.format(key))

    def __setitem__(self, key, value):
        if key in self.__slots__:
            setattr(self, key, value)
        else:
            raise KeyError('No slot called `{}`'.format(key))

    def __repr__(self):
        """Generic string representation."""
        return '{}({})'.format(
            self.__class__.__name__, ', '.join('{}: {}'.format(
                slot, getattr(self, slot)) for slot in self.__slots__))

    def __str__(self):
        """Comma-separated string representation."""
        return ', '.join('{}: {}'.format(slot, getattr(self, slot))
                         for slot in self.__slots__)

    @classmethod
    def create(cls, *fields):
        """
        Creates a subclass of Stats with the specified fields. The name of the
        new class is added to the current module, so that it is pickle-able.
        """
        subclass = type('Stats_' + '_'.join(map(str, fields)), (cls,),
                        {'__slots__': fields})
        setattr(inspect.getmodule(cls), subclass.__name__, subclass)
        return subclass


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks. An itertools recipe."""
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def grouper2(iterable, n, fillvalue=None):
    """
    Same as :funct:`grouper`, but it also filters all instances of ``fillvalue``
    from the returned groups. If it does not occur anywhere else in
    ``iterable``, this effectively means that the last chunk might
    contain fewer elements than the rest.
    """
    # grouper2('ABCDEFG', 3) --> ABC DEF G"
    for group in grouper(iterable, n, fillvalue):
        yield tuple(elem for elem in group if elem != fillvalue)


class IllegalStateError(RuntimeError):
    """Thrown when a method is invoked on an object in inappropriate state."""
