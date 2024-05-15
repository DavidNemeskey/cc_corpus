#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generic I/O related functionality."""

import logging
from pathlib import Path
from typing import Any

from cc_corpus.utils import openall


class BatchWriterBase:
    """
    Writes generic "documents" into a batch of files with consecutive numbering.

    .. attribute:: total_written

        The total number of documents written. The documents written to the
        current file are not included in the count.

    .. attribute:: current_file

        The name of the currently open file.
    """
    def __init__(self, batch_size: int, out_dir: Path, digits: int = 4,
                 name_prefix: str = '', suffix: str = '.gz',
                 first_batch: int = 1):
        """
        Parameters:
        :param batch_size: the number of documents after which a new batch file
                           is opened (with consecutive numbering)
        :param out_dir: the output directory
        :param digits: the number of zeroes in the batch files' name (e.g. if 2,
                       the first batches will be called 01, 02, etc.)
        :param name_prefix: prepend this string to all file names
        :param suffix: the suffix to add to each file (the part after the
                       digits). This also determines whether the file will be
                       compressed (e.g. the default valud ``.gz`` implies
                       ``gzip`` compression).
        :param first_batch: start batch numbering here instead of the default 1
        """
        self.batch_size = batch_size
        self.out_dir = Path(out_dir)
        self.digits = digits
        self.name_prefix = name_prefix
        self.suffix = suffix
        self.batch = first_batch - 1
        self.outf = None
        self.docs_written = self.batch_size + 1  # so that we invoke new_file

        self.total_written = 0
        self.current_file = None

    def write(self, document: Any):
        """
        Writes a single index document to the currently open file. Opens a new
        file when the current one is full.

        :param document: the document to write. It should convert to a
                         meaningful string representation explicitly.
        """
        if self.docs_written >= self.batch_size:
            self.new_file()
        print(document, file=self.outf)
        self.docs_written += 1

    def new_file(self):
        """Closes the old file and opens a new one."""
        self.close()

        self.batch += 1
        new_file_name = f'{self.name_prefix}{{:0{self.digits}}}'.format(
            self.batch
        )
        self.current_file = (self.out_dir / new_file_name).with_suffix(self.suffix)
        logging.debug('Opening file {}...'.format(self.current_file))
        self.outf = openall(self.current_file, 'wt')

    def close(self):
        """
        Closes the currently written file handle. Called automatically when
        the batch counter increases, but should also be called when processing
        ends to close the files of the last batch.
        """
        if self.outf is not None:
            self.outf.close()
            self.outf = None
            self.current_file = None

            self.total_written += self.docs_written
        self.docs_written = 0

    def __del__(self):
        """Just calls close()."""
        self.close()
