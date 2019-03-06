#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Stuff common to all deduplication scripts (minhash.py, lsh.py, etc.)
"""

import logging
import os
import pickle


class BatchWriter:
    """Writes batches of minhash data."""
    def __init__(self, batch_size, out_dir, zeroes=4):
        self.batch_size = batch_size
        self.out_dir = out_dir
        self.zeroes = zeroes
        self.batch = 0
        self.minhashf = self.doc_idf = self.filef = None
        self.mh_offset = self.di_offset = 0
        self.p_written = self.batch_size + 1  # so that we invoke new_file
        self.total_written = 0

    def write_results(self, input_file, results):
        """Prints the results of minhashing a data file."""
        if self.p_written >= self.batch_size:
            self.new_file()

        print('{}\t{}\t{}\t{}'.format(input_file, len(results['minhash']),
                                      self.mh_offset, self.di_offset),
              file=self.filef)
        for mh in results['minhash']:
            self.mh_offset += self.minhashf.write(pickle.dumps(mh))
        for id_fields in results['id']:
            self.di_offset += self.doc_idf.write(
                '{}\n'.format('\t'.join(str(f) for f in id_fields)).encode('utf-8'))
        self.p_written += len(results['minhash'])

    def new_file(self):
        """Closes the old file and opens a new one."""
        self.close()

        self.batch += 1
        logging.info('Opening file {}...'.format(self.batch))
        prefix = os.path.join(self.out_dir,
                              '{{:0{}}}'.format(self.zeroes).format(self.batch))
        self.minhashf = open(prefix + '.minhashes', 'wb')
        self.doc_idf = open(prefix + '.doc_ids', 'wb')
        self.filef = open(prefix + '.files', 'wt')

    def close(self):
        """
        Closes the currently written file handles. Called automatically when
        the batch counter increases, but should also be called when processing
        ends to close the files of the last batch.
        """
        if self.filef is not None:
            self.minhashf.close()
            self.doc_idf.close()
            self.filef.close()
            self.minhashf = self.doc_idf = self.filef = None

            self.mh_offset = 0
            self.di_offset = 0

            self.total_written += self.p_written
        self.p_written = 0

    def __del__(self):
        """Just calls close()."""
        self.close()
