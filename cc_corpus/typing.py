#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Replacement for the typing library that only became available in 3.5.
"""

# TODO: there should be a better way to do this. Maybe a corpus-level
# __getattr__ or something?
try:
    from typing import Any, BinaryIO, Dict, Iterator, List, Set, Tuple
except ImportError:
    Any, BinaryIO, Dict, Iterator, List, Set, Tuple = [None] * 6

__all__ = ['Any', 'BinaryIO', 'Dict', 'Iterator', 'List', 'Set', 'Tuple']
