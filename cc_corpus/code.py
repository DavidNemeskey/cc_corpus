#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Infrastructure to compiles Python expressions to code."""

from typing import Any


class Filter:
    """Compiles filters that are valid Python expressions and applies them."""

    _allowed_builtins = {
        'abs': abs,
        'all': all,
        'any': any,
        'chr': chr,
        'divmod': divmod,
        'len': len,
        'max': max,
        'min': min,
        'pow': pow,
        'round': round,
        'sorted': sorted,
        'sum': sum,
        'bool': bool,
        'float': float,
        'int': int,
        'list': list,
        'map': map,
        'range': range,
        'str': str,
        'tuple': tuple,
        'type': type,
        'zip': zip,
    }
    _globals = {'__builtins__': _allowed_builtins}

    def __init__(self, *filters: str):
        """
        Creates a :class:`Filter` that only accepts objects that pass all
        ``filter``s specified.

        :param filters: any number of valid Python expressions that return a
                        ``bool`` value.
        :type filters: str

        An example:

        ::

            f = Filter('a > 3', 'b <= 5')
            a, b = 4, 3
            f.filter(a=a, b=b)  # Returns True
            f.filter(a=3, b=b)  # Returns False
        """
        if not filters:
            filters = 'True',
        self.code = compile('(' + ') and ('.join(filters) + ')',
                            '<string>', 'eval', optimize=2)

    def __call__(self, **kwargs: Any) -> bool:
        """Runs the compiled filter on the variables in ``kwargs``."""
        return eval(self.code, Filter._globals, kwargs)
