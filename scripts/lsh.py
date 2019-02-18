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

from cc_corpus.utils import openall, unpickle_stream


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
    # TODO maybe to a threshold number > the regular threshold?
    parser.add_argument('--skip-same-doc', '-s', action='store_true',
                        help='if true, does not deduplicate paragraphs from '
                             'the same document.')
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


def read_names(names_file):
    """
    Reads the names of the documents and returns a list of _their hashes_. The
    reason we return a hash instead of the actual URL is to minimize the memory
    consumption; for checking equality, a hash should suffice.
    """
    with openall(names_file) as inf:
        return [hash(line.split('\t', 1)[0]) for line in inf]


def find_duplicates(minhashes, threshold, permutations, name_hashes):
    """
    Find the duplicates amongst the minhashes.

    Arguments:
    - minhashes: a list of minhashes
    - threshold: the Jaccard threshold for similarity / identity
    - permutations: the number of permutations. Must be the same as for the
                    minhash objects
    - name_hashes: list of document hashes (or any ID type, really). If not
                   empty, similarities between documents with the same ID are
                   taken for granted and are not reported.
    """
    lsh = MinHashLSH(threshold=threshold, num_perm=permutations)
    for i, mh in enumerate(minhashes, start=1):
        lsh.insert(str(i), mh, check_duplication=False)
    for i, mh in enumerate(minhashes, start=1):
        similar = lsh.query(mh)
        similar.remove(str(i))
        if name_hashes:
            similar = [s for s in similar if name_hashes[i] != name_hashes[int(s)]]
        if similar:
            print(i, similar)


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    minhashes = load_minhashes(args.input + '.minhashes')
    if args.skip_same_doc:
        name_hashes = read_names(args.input + '.doc_ids')
    else:
        name_hashes = []
    find_duplicates(minhashes, args.threshold, args.permutations, name_hashes)

    os.nice(20)


if __name__ == '__main__':
    main()
