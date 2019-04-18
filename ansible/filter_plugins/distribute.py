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
        # Ensure that there is at most one slash at the end
        m = self.slashp.search(path)
        if m:
            return '{}_{}{}'.format(path[:m.start()], host_name, os.sep)
        else:
            root, ext = os.path.splitext(path)
            return '{}_{}{}'.format(root, host_name, ext)
