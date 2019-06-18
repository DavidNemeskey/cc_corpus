#!/usr/bin/env python

"""
Get the "distributed" name of files or directories; i.e. append the host name
to their name. The reason we need a filter to do that is that we have to take
into account file extensions, ending path separators, etc.
"""

import os
import re


class FilterModule:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.slashp = re.compile('([{}]+)$'.format(os.sep))

    def filters(self):
        return {'distribute': self.distribute}

    def distribute(self, path, host_name):
        """
        Adds ``host_name`` to ``path``. The insertion point depends on whether
        ``path`` denotes a directory or a file: for a directory, it is put
        just before the ending path separator (``/`` or ``\``, depending on the
        OS); for files, before the extension.

        There are two caveats:

        1. At this point we don't know if ``path`` is *actually* a directory.
           If it ends in a path separator, it is a directory; otherwise, it
           isn't.
        2. In some cases, we might want to insert the host name somewhere else.
           In this case, the insertion point can be marked with a pair of
           braces (``{}``), e.g. ``path/to_{}/file.ext``.
        """
        parts = path.split('{}')
        if len(parts) > 1:
            return host_name.join(parts)
        else:
            # Check if path is a directory; i.e. there is a slash at the end
            # while also ensuring there is at most one
            m = self.slashp.search(path)
            if m:
                # Directory
                return '{}_{}{}'.format(path[:m.start()], host_name, os.sep)
            else:
                # File
                root, ext = os.path.splitext(path)
                return '{}_{}{}'.format(root, host_name, ext)
