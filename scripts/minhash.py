#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Computes the minhash for a directory of files.
"""

from argparse import ArgumentParser
from functools import partial
import logging
import os
import pickle

from datasketch import MinHash, LeanMinHash
from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.utils import openall, collect_inputs


def parse_arguments():
    parser = ArgumentParser('Computes the minhash for a directory of files.')
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories to compute the minhash for.')
    parser.add_argument('--output', '-o', required=True,
                        help='the output file(s).')
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
    results = {'id': [], 'minhash': []}
    for doc in parse_file(input_file, meta=False):
        for p, text in enumerate(doc.paragraphs, start=1):
            results['id'].append((doc.attrs['url'], p))
            mh = MinHash(num_perm=permutations)
            for shingle in shinglize(text, n):
                mh.update(shingle.encode('utf-8'))
            results['minhash'].append(LeanMinHash(mh))
    return results


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)

    files = collect_inputs(args.inputs)
    logging.info('Found a total of {} input files.'.format(len(files)))
    with Pool(args.processes) as pool:
        f = partial(process_file, permutations=args.permutations, n=args.n)
        with openall('{}.minhashes'.format(args.output), 'wb') as mout, \
             openall('{}.doc_ids'.format(args.output), 'wt') as dout:
            for results in pool.map(f, files):
                for mh in results['minhash']:
                    mout.write(mh)
                for doc, p in results['id']:
                    print(doc, p, sep='\t', file=dout)

        pool.close()
        pool.join()
    logging.info('Done.')

if __name__ == '__main__':
    main()
