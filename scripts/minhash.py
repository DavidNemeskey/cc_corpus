#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Computes the minhash for a directory of files. Outputs the results into another
directory, with consecutively numbered files:
    - xxx.minhashes: the minhashes of the paragraphs in a batch
    - xxx.doc_ids: the document and paragraph ids of a batch
    - xxx.files: contains the names of all data files in the batch, the number
                 of paragraphs from each, as well as offsets for the data in
                 both the minhashes and doc_ids files.
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Pool
import os
import pickle

from datasketch import MinHash, LeanMinHash
from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.utils import openall, collect_inputs


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories to compute the minhash for.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--batch-size', '-b', type=int, default=1000000,
                        help='the number of paragraphs in a single batch. '
                             'This is not an exact number, as documents in '
                             'the same data files are always put into the same '
                             'batch.')
    parser.add_argument('--permutations', '-p', type=int, default=256,
                        help='the number of permutations per paragraph (256).')
    parser.add_argument('--n', '-n', type=int, default=5,
                        help='the number of permutations per paragraph (5).')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    parser.add_argument('--zeroes', '-Z', type=int, default=4,
                        help='the number of zeroes in the batch files\' names.')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def shinglize(text, n):
    """Creates character n-grams from the text."""
    for i in range(len(text) - n + 1):
        yield text[i:i+n]


def process_file(input_file, permutations, n):
    logging.info('Processing {}...'.format(input_file))
    results = {'id': [], 'minhash': []}
    num_docs, num_ps = 0, 0
    try:
        for doc in parse_file(input_file, meta=False):
            try:
                num_docs += 1
                num_ps += len(doc.paragraphs)
                logging.debug('Hashing URL {}...'.format(doc.attrs['url']))
                for p, text in enumerate(doc.paragraphs, start=1):
                    results['id'].append((doc.attrs['url'], p))
                    mh = MinHash(num_perm=permutations)
                    for shingle in shinglize(text, n):
                        mh.update(shingle.encode('utf-8'))
                    results['minhash'].append(LeanMinHash(mh))
            except:
                logging.exception(
                    'Exception while processing file {}, in doc {}'.format(
                        input_file, doc))
    except:
        logging.exception('Error processing file {}'.format(input_file))
    logging.info('Finished processing {}, which contained {} paragraphs in {} '
                 'documents.'.format(input_file, num_ps, num_docs))
    return results


class BatchWriter:
    """Writes batches of minhash data."""
    def __init__(self, batch_size, zeroes=4):
        self.batch_size = batch_size
        self.zeroes = zeroes
        self.batch = 1
        self.minhashf = self.doc_idf == self.filef = None
        self.mh_offset = self.di_offset = 0

    def write_results(self, input_file, results):
        """Prints the results of minhashing a data file."""
        print('{}\t{}\t{}\t{}'.format(input_file, len(results),
                                      self.mh_offset, self.di_offset),
              file=self.filef)
        for mh in results['minhash']:
            self.mh_offset += self.minhashf.write(pickle.dumps(mh))
        for doc, p in results['id']:
            self.di_offset += self.doc_idf.write(
                '{}\t{}\n'.format(doc, p).encode('utf-8'))


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    files = sorted(collect_inputs(args.inputs))
    logging.info('Found a total of {} input files.'.format(len(files)))
    with Pool(args.processes) as pool:
        f = partial(process_file, permutations=args.permutations, n=args.n)
        with openall('{}.minhashes'.format(args.output), 'wb') as mout, \
             openall('{}.doc_ids'.format(args.output), 'wt') as dout:
            num_ps = 0
            for results in pool.map(f, files):
                for mh in results['minhash']:
                    mout.write(pickle.dumps(mh))
                for doc, p in results['id']:
                    print(doc, p, sep='\t', file=dout)
                num_ps += len(results['minhash'])
            logging.info('Hashed in total {} paragraphs.'.format(num_ps))

        pool.close()
        pool.join()
    logging.info('Done.')


if __name__ == '__main__':
    main()
