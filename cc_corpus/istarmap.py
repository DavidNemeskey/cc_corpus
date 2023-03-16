#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Defines the :func:`istarmap` function, which is unaccountably missing from
:mod:`multiprocessing`. The :mod:`multiprocessing` module is patched,
so this module should be imported _first_.
Taken from
https://stackoverflow.com/questions/57354700/.
"""

import multiprocessing.pool as mpp


def istarmap(self, func, iterable, chunksize=1):
    """
    Iterable version of :func:`multiprocessing.starmap`.
    """
    self._check_running()
    if chunksize < 1:
        raise ValueError(
            "Chunksize must be 1+, not {0:n}".format(
                chunksize))

    task_batches = mpp.Pool._get_tasks(func, iterable, chunksize)
    result = mpp.IMapIterator(self)
    self._taskqueue.put(
        (
            self._guarded_task_generation(result._job,
                                          mpp.starmapstar,
                                          task_batches),
            result._set_length
        ))
    return (item for chunk in result for item in chunk)


mpp.Pool.istarmap = istarmap
