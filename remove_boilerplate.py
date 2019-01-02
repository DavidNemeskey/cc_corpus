#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import namedtuple
from fnmatch import fnmatch
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
    def __init__(self, index_dir, warc_dir, output_dir):
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
        self.output_dir = output_dir
        # This is the output stream
        self.outf = None

    def read(self, index_file, func):
        """
        Enumerates the index and WARC records in the specified index file and
        the matching WARC files. Calls the specified function with the two
        records.
        """
        index_iter = self.index_lines(index_file)
        warc_iter = self.warc_records(index_file)
        for i, warc_record in enumerate(warc_iter):
            url = warc_record['WARC-Target-URI']
            for index in index_iter:
                if index.url == url:
                    self.process_record(index, warc_record)
                    break
            else:
                raise ValueError('URL {} was not found in index'.format(url))

    def process_record(self, index, warc):
        """Writes the output file."""
        print('<doc domain="{0}" index="{1}" url="{2}" warc-file="{3}" ' \
              'offset="{4}" length="{5}" response="{6}" mime-type="{7}">\n' \
              '<meta>\n<request>\n{8}\n</request>\n<response>\n{9}\n'
              '</response>\n</meta>\n{10}\n</doc>\n\n\n'.format(
                  index.domain, index.index, index.url, index.warc_file,
                  index.offset, index.length, index.status, index.mime,
                  # warc1, warc2, text_removed).encode('UTF-8'),
                  'a', 'b', 'c').encode('UTF-8'),
              file=self.outf)

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
        try:
            for warc_file in self.warc_files_for_index(index_file):
                output_file = op.basename(warc_file).replace('.warc.', '.txt.')
                with gzip.open(op.join(self.output_dir, output_file), 'wt') as outf:
                    self.outf = outf
                    for record in warc.open(warc_file):
                        yield record
        finally:
            self.outf = None

    def warc_files_for_index(self, index_file):
        """Returns all WARC files that correspond to an index file."""
        pattern = op.splitext(index_file)[0] + '_*.warc*'
        return sorted([op.join(self.warc_dir, f)
                       for f in os.listdir(self.warc_dir) if fnmatch(f, pattern)])


def main():
    reader = IndexWarcReader('cc_index_dedup_52', 'cc_downloaded_52')
    reader.read('domain-hu-CC-MAIN-2018-05-0000.gz')


if __name__ == '__main__':
    main()
