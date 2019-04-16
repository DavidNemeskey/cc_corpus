#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Replacement for the typing library that only became available in 3.5.
"""

try:
    from typing import Any, Dict, Iterator, List, Set, Tuple
except ImportError:
    Any, Dict, Iterator, List, Set, Tuple = [None] * 6

__all__ = ['Any', 'Dict', 'Iterator', 'List', 'Set', 'Tuple']
