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
from contextlib import closing
from functools import partial
import logging
from multiprocessing import Pool
import os

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.deduplication import BatchWriter, MinHasher
from cc_corpus.utils import collect_inputs, otqdm


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories to compute the minhash for.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--batch-size', '-b', type=int, default=1000000,
                        help='the number of units in a single batch. '
                             'This is not an exact number, as documents in '
                             'the same data files are always put into the same '
                             'batch.')
    parser.add_argument('--unit', '-u', choices=['doc', 'p'], default='p',
                        help='the deduplication unit: document or paragraph (p).')
    parser.add_argument('--permutations', '-p', type=int, default=256,
                        help='the number of permutations per paragraph (256).')
    parser.add_argument('--n', '-n', type=int, default=5,
                        help='the size of the n-grams (5).')
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


def minhash_ps(input_file, permutations, n):
    """Minhashes paragraphs."""
    logging.info('Processing {}...'.format(input_file))
    minhasher = MinHasher(permutations, n)
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
                    results['minhash'].append(minhasher.minhash(text))
            except:
                logging.exception(
                    'Exception while processing file {}, in doc {}'.format(
                        input_file, doc))
    except:
        logging.exception('Error processing file {}'.format(input_file))
    logging.info('Finished processing {}, which contained {} paragraphs in {} '
                 'documents.'.format(input_file, num_ps, num_docs))
    return input_file, results


def minhash_docs(input_file, permutations, n):
    """Minhashes documents."""
    logging.info('Processing {}...'.format(input_file))
    minhasher = MinHasher(permutations, n)
    results = {'id': [], 'minhash': []}
    num_docs = 0
    try:
        for doc in parse_file(input_file, meta=False):
            try:
                num_docs += 1
                logging.debug('Hashing URL {}...'.format(doc.attrs['url']))
                results['id'].append((doc.attrs['url'],))
                results['minhash'].append(minhasher.minhash(doc.content()))
            except:
                logging.exception(
                    'Exception while processing file {}, in doc {}'.format(
                        input_file, doc))
    except:
        logging.exception('Error processing file {}'.format(input_file))
    logging.info('Finished processing {}, which contained {} documents.'.format(
        input_file, num_docs))
    return input_file, results


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
    print(f'Number of processes: {args.processes}')
    logging.info('Found a total of {} input files.'.format(len(files)))

    with closing(
        BatchWriter(args.batch_size, args.output_dir, args.zeroes)
    ) as writer:
        with Pool(args.processes) as pool:
            minhash_fun = minhash_ps if args.unit == 'p' else minhash_docs
            f = partial(minhash_fun, permutations=args.permutations, n=args.n)
            for input_file, results in otqdm(
                pool.imap_unordered(f, files), 'Minhashing...', total=len(files)
            ):
                logging.debug('Got results for {}: {}'.format(
                    input_file, len(results['minhash'])))
                writer.write_results(input_file, results)

            pool.close()
            pool.join()
        logging.info('Done.')

    logging.info('Hashed in total {} paragraphs.'.format(writer.total_written))


if __name__ == '__main__':
    main()
