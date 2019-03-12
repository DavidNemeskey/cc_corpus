#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Stuff common to all deduplication scripts (minhash.py, lsh.py, etc.)
"""

from itertools import islice
import logging
import os
import pickle


class BatchWriter:
    """Writes batches of minhash data."""
    def __init__(self, batch_size, out_dir, zeroes=4, first_batch=1):
        """
        Parameters:
        - batch_size: the number of documents after which a new batch file is
                      opened (with consecutive numbering)
        - out_dir: the output directory
        - zeroes: the number of zeroes in the batch files' name (e.g. if 2,
                  the first batches will be called 01, 02, etc.)
        - first_batch: what should be the number (name) of the first batch
        """
        self.batch_size = batch_size
        self.out_dir = out_dir
        self.zeroes = zeroes
        self.batch = first_batch - 1
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
        prefix = os.path.join(
            self.out_dir, '{{:0{}}}'.format(self.zeroes).format(self.batch))
        self.minhashf = open(prefix + '.minhashes', 'wb')
        self.doc_idf = open(prefix + '.doc_ids', 'wb')
        self.filef = open(prefix + '.files', 'wt', encoding='utf-8')

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


def read_batch(batch_file_prefix):
    """
    Reads a single batch written previously with BatchWriter. Yields a (document
    name, results) tuple for each input file in the batch.

    Kind of sluggish because of the unpickling, but that's how usually we need
    the result.
    """
    with open(batch_file_prefix + '.minhashes', 'rb') as minhashf, \
         open(batch_file_prefix + '.doc_ids', 'rt', encoding='utf-8') as doc_idf, \
         open(batch_file_prefix + '.files', 'rt', encoding='utf-8') as filef:
        for doc_file, num_lines, _, _ in (l.strip().split() for l in filef):
            doc_ids = [doc_id.strip().split('\t') for doc_id in
                       islice(doc_idf, int(num_lines))]
            minhashes = [pickle.load(minhashf) for _ in range(int(num_lines))]
            yield doc_file, {'minhash': minhashes, 'id': doc_ids}


def find_all_batches(input_dir, greater_than=None):
    """
    Returns all minhash batches file prefixes in the specified directory. If
    greater_than is specified, only those batches are returned that are
    numerically greater than the specified number.
    """
    batches = [f[:-6] for f in os.listdir(input_dir)
               if re.match('[0-9]+.files', f)]
    if greater_than is not None:
        batches = [b for b in batches if int(b) > greater_than]
    return [op.join(input_dir, b) for b in sorted(batches, key=int)]
