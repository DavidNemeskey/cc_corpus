#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Stuff common to all deduplication scripts (minhash.py, lsh.py, etc.)
"""

from itertools import islice, cycle
import logging
import os
from pathlib import Path
import pickle
import re
import shutil
from typing import Optional

from datasketch import LeanMinHash, MinHash, MinHashLSH


class BatchWriter:
    """Writes batches of minhash data."""
    def __init__(self, batch_size, out_dir, digits=1, first_batch=1):
        """
        Parameters:
        - batch_size: the number of documents after which a new batch file is
                      opened (with consecutive numbering)
        - out_dir: the output directory
        - digits: the number of zeroes in the batch files' name (e.g. if 2,
                  the first batches will be called 01, 02, etc.)
        - first_batch: what should be the number (name) of the first batch
        """
        self.batch_size = batch_size
        self.out_dir = out_dir
        self.digits = digits
        self.batch = first_batch - 1
        self.minhashf = self.doc_idf = self.filef = None
        self.mh_offset = self.di_offset = 0
        self.p_written = self.batch_size + 1  # so that we invoke new_file
        self.total_written = 0

    def write_results(self, input_file, results):
        """Prints the results of minhashing a data file."""
        # If there are no records in the file: just skip it!
        if results['minhash']:
            if self.p_written >= self.batch_size:
                self.new_file()

            print('{}\t{}\t{}\t{}'.format(input_file, len(results['minhash']),
                                          self.mh_offset, self.di_offset),
                  file=self.filef)
            for mh in results['minhash']:
                self.mh_offset += self.minhashf.write(pickle.dumps(mh))
            for id_fields in results['id']:
                self.di_offset += self.doc_idf.write(
                    '{}\n'.format('\t'.join(str(f) for f in id_fields))
                    .encode('utf-8')
                )
            self.p_written += len(results['minhash'])

    def copy_file(self, input_prefix):
        """
        Opens (a set of) new files and copies the data from ``input_prefix``
        into them.
        """
        self.new_file()
        self.close()

        prefix = os.path.join(
            self.out_dir, '{{:0{}}}'.format(self.digits).format(self.batch))
        for ext in ['.minhashes', '.doc_ids', '.files']:
            shutil.copy(input_prefix + ext, prefix + ext)

    def new_file(self):
        """Closes the old file and opens a new one."""
        self.close()

        self.batch += 1
        logging.info('Opening file {}...'.format(self.batch))
        prefix = os.path.join(
            self.out_dir, '{{:0{}}}'.format(self.digits).format(self.batch))
        self.minhashf = open(prefix + '.minhashes', 'wb')
        self.doc_idf = open(prefix + '.doc_ids', 'wb')
        self.filef = open(prefix + '.files', 'wt', encoding='utf-8')

    def close(self):
        """
        Closes the currently written file handles. Called automatically when
        the batch counter increases, but should also be called when processing
        ends to close the files of the last batch.
        """
        logging.debug('Closing...')
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


def _read_batch(batch_file_prefix: Path):
    """
    A generator function to read the batch data.

    Kind of sluggish because of the unpickling, but that's how usually we need
    the result.
    """
    with open(batch_file_prefix.with_suffix('.minhashes'), 'rb') as minhashf, \
            open(batch_file_prefix.with_suffix('.doc_ids'), 'rt',
                 encoding='utf-8') as doc_idf, \
            open(batch_file_prefix.with_suffix('.files'), 'rt',
                 encoding='utf-8') as filef:
        for doc_file, num_lines, _, _ in (l.strip().split() for l in filef):
            doc_ids = [doc_id.strip().split('\t') for doc_id in
                       islice(doc_idf, int(num_lines))]
            minhashes = [pickle.load(minhashf) for _ in range(int(num_lines))]
            yield (doc_ids, minhashes, doc_file)


def read_batch_to_memory(batch_file_prefix: Path):
    """
    Reads a single batch written previously with BatchWriter into memory.
    Returns a list of tuples. Each tuple is one document.  The elements of a
    tuple are:
    1. document title (it's url), represented as a list of strings
    note:: the url is split along tabs, for reasons unknown to me.
    2. minhash,
    3. source file (the .gz it comes from)
    """
    collected_data = []
    for doc_ids, minhashes, doc_file in _read_batch(batch_file_prefix):
        # Combine the data into a tuple, add the current doc_file as well:
        data_in_file = zip(doc_ids, minhashes, cycle([doc_file]))
        collected_data += data_in_file
    return collected_data


def read_batch(batch_file_prefix: Path):
    """
    Reads a single batch written previously with BatchWriter. Yields a
    (document name, results) tuple for each input file in the batch.
    """
    for doc_ids, minhashes, doc_file in _read_batch(batch_file_prefix):
        yield doc_file, {'minhash': minhashes, 'id': doc_ids}


def find_all_batches(input_dir: Path, greater_than=None) -> list[Path]:
    """
    Returns all minhash batches file prefixes in the specified directory. If
    greater_than is specified, only those batches are returned that are
    numerically greater than the specified number.
    """
    batch_stems = [f.stem for f in input_dir.iterdir()
                   if re.match('[0-9]+.files', f.name)]
    batch_stems = sorted(batch_stems, key=int)
    if greater_than is not None:
        batch_stems = [b for b in batch_stems if int(b) > greater_than]
    return [input_dir / b for b in batch_stems]


class MinHasher:
    """Minhashes text."""
    def __init__(self, permutations, n):
        self.permutations = permutations
        self.n = n

    def shinglize(self, text):
        """Creates character n-grams from the text."""
        for i in range(len(text) - self.n + 1):
            yield text[i:i + self.n]

    def minhash(self, text):
        mh = MinHash(num_perm=self.permutations)
        for shingle in self.shinglize(text):
            mh.update(shingle.encode('utf-8'))
        return LeanMinHash(mh)


def read_batch_to_lsh(
    batch: Path, lsh: Optional[MinHashLSH] = None,
    threshold: Optional[float] = None, permutations: Optional[int] = None
) -> MinHashLSH:
    """
    Reads a batch into a :class:`MinHashLSH` object. Works in two ways:

    #. If an already existing object is passed in the _lsh_ argument, it will
       be updated with the contents of the batch. It is assumed that all
       documents in the batch are unique w.r.t. other documents in the batch
       **and the keys in the _lsh_ object as well**. In this cae, the rest of
       the arguments are ignored.
    #. If _lsh_ is ``None``, a new one is created and returned with the
       specified threshold and number of permutations.
    """
    if lsh is None:
        lsh = MinHashLSH(threshold=threshold, num_perm=permutations)
    for input_file, results in read_batch(batch):
        for doc_id, minhash in zip(results['id'], results['minhash']):
            lsh.insert('\t'.join(doc_id), minhash)
    return lsh
