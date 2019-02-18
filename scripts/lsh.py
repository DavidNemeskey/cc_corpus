#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Deduplicates the documents with Locality Sensitive Hashing, based on the files
written by minhash.py. 
"""

from argparse import ArgumentParser
import logging
import os
import pickle

from datasketch import MinHashLSH, LeanMinHash

from cc_corpus.utils import unpickle_stream


def parse_arguments():
    parser = ArgumentParser('Deduplicates the documents with Locality '
                            'Sensitive Hashing, based on the files written '
                            'by minhash.py.')
    parser.add_argument('--input', '-i', required=True,
                        help='the input file prefix.')
    parser.add_argument('--threshold', '-t', type=float, default=0.9,
                        help='the number of permutations per paragraph (256).')
    parser.add_argument('--permutations', '-p', type=int, default=256,
                        help='the number of permutations per paragraph (256).')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    return args


def load_minhashes(minhash_file):
    """Loads the minhash objects into a list."""
    with open(minhash_file, 'rb') as inf:
        obj = pickle.load(inf)
        step = len(pickle.dumps(obj))
        inf.seek(0)
        return list(unpickle_stream(inf))


def find_duplicates(minhashes, threshold, permutations):
    """Find the duplicates amongst the minhashes."""
    lsh = MinHashLSH(threshold=threshold, num_perm=permutations)
    for i, mh in enumerate(minhashes, start=1):
        lsh.insert(str(i), mh, check_duplication=False)
    for i, mh in enumerate(minhashes, start=1):
        similar = lsh.query(mh)
        similar.remove(str(i))
        if similar:
            print(i, similar)


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    minhashes = load_minhashes(args.input + '.minhashes')
    find_duplicates(minhashes, args.threshold, args.permutations)

    os.nice(20)


if __name__ == '__main__':
    main()
