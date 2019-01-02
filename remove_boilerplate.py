#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple
import fnmatch
import gzip
import io
import os
import os.path as op

import warc


IndexTuple = namedtuple('IndexTuple', ['index', 'domain', 'url', 'warc',
                                       'offset', 'length', 'status', 'mime'])


class IndexWarcReader:
    """
    Reads index files and the files with the downloaded WARC segments in
    parallel.

    Note: in the class description, "WARC file" means "a file that contains
    downloaded WARC segments from Common Crawl". One difference to a CC WARC
    file is that these files only contain the responses.
    """
    def __init__(self, index_dir, warc_dir):
        """
        Creates a new IndexWarcReader with the specified index and warc
        directories. These must be compatible, i.e. the WARC directory should
        contain the downloaded segments corresponding to the files in the
        index directory.

        index_dir: the directory with the index files.
        warc_dir: the directory with the WARC files.
        """
        self.index_dir = index_dir
        self.warc_dir = warc_dir

    def read(self, index_file):
        """
        Enumerates the index and WARC records in the specified index file and
        the matching WARC files.
        """
        index_iter = self.index_lines(index_file)
        warc_iter = self.warc_records(index_file)
        for i, index_tuple in enumerate(index_iter):
            print(index_tuple)
            if i == 4:
                break
        for i, record in enumerate(warc_iter):
            print(record['WARC-Target-URI'])
            if i == 4:
                break

    def index_lines(self, index_file):
        """Enumerates the lines of the index file into IndexTuples."""
        module = gzip if index_file.endswith('.gz') else io
        with module.open(op.join(self.index_dir, index_file), 'rt') as inf:
            for line in inf:
                yield IndexTuple(op.splitext(index_file)[0],
                                 *line.strip().split())

    def warc_records(self, index_file):
        """
        Enumerates WARC records from the WARC files that correspond to
        index_file.
        """
        for warc_file in self.warc_files_for_index(index_file):
            for record in warc.open(warc_file):
                yield record

    def warc_files_for_index(self, index_file):
        """Returns all WARC files that correspond to an index file."""
        pattern = op.splitext(index_file)[0] + '_*.warc*'
        return [op.join(self.warc_dir, f)
                for f in os.listdir(warc_dir) if fnmatch(f, pattern)]


def main():
    pass


if __name__ == '__main__':
    main()
