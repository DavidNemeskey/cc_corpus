#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Replacement for the typing library that only became available in 3.5. If not,
we replace all types we need with a special class that supports []
so that type declarations work.
"""

__all__ = ['Any', 'BinaryIO', 'Dict', 'Iterator', 'List', 'Set', 'Tuple']

# TODO: there should be a better way to do this. Maybe a corpus-level
# __getattr__ or something?
try:
    from typing import Any, BinaryIO, Dict, Iterator, List, Set, Tuple
except ImportError:
    class GetItemType:
        def __getitem__(self, key):
            pass

    Any, BinaryIO, Dict, Iterator, List, Set, Tuple = [GetItemType()] * len(__all__)
