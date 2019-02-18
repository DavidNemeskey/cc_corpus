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

from datasketch import MinHash, LeanMinHash


def parse_arguments():
    parser = ArgumentParser('Deduplicates the documents with Locality '
                            'Sensitive Hashing, based on the files written '
                            'by minhash.py.')
    parser.add_argument('--input', '-i', required=True,
                        help='the input file prefix.')
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
        minhashes = []
        while True:
            minhashes.append(pickle.load(inf))
        return minhashes


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    minhashes = load_minhashes(args.input + '.minhashes')

    os.nice(20)


if __name__ == '__main__':
    main()
