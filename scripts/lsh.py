#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Deduplicates the documents with Locality Sensitive Hashing, based on the files
written by minhash.py.
"""

from argparse import ArgumentParser
from contextlib import closing
import logging
import os
import os.path as op
import sys

from datasketch import MinHashLSH

from cc_corpus.utils import openall, unpickle_stream
from cc_corpus.deduplication import BatchWriter, read_batch


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
    subparsers = parser.add_subparsers(
        help='The "modus operandi" (lit.) of the script.')
    parser_print = subparsers.add_parser(
        'print',
        help='Just print the matching line groups.'
    )
    parser_print.set_defaults(command='print')
    parser_dedup = subparsers.add_parser(
        'deduplicate', aliases=['dedup'],
        help='Deduplicate the documents and writes a new set of minhash files.'
    )
    parser_dedup.set_defaults(command='deduplicate')
    parser_dedup.add_argument('--output-dir', '-o', required=True,
                              help='the directory to which the updated minhash '
                                   'files are written.')
    args = parser.parse_args()
    return args


def load_minhashes(minhash_file):
    """Loads the minhash objects into a list."""
    with open(minhash_file, 'rb') as inf:
        # obj = pickle.load(inf)
        # step = len(pickle.dumps(obj))
        # inf.seek(0)
        return list(unpickle_stream(inf))


def read_names(names_file):
    """
    Reads the names of the documents and returns a list of _their hashes_. The
    reason we return a hash instead of the actual URL is to minimize the memory
    consumption; for checking equality, a hash should suffice.
    """
    with openall(names_file) as inf:
        return [hash(line.strip().split('\t', 1)[0]) for line in inf]


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
            # Remove matches that occur in the same document
            similar = [s for s in similar if name_hashes[i - 1] != name_hashes[int(s) - 1]]
        if similar:
            print('{}\t{}'.format(i, ' '.join(similar)))


def deduplicate_file(file_prefix, output_dir, threshold, permutations):
    """
    Deduplicates a set of minhashed documents (3 files with the same minhash
    prefix) and writes them to output_dir.

    Warning: only works for full documents at this point!
    """
    lsh = MinHashLSH(threshold=threshold, num_perm=permutations)
    file_base = op.basename(file_prefix)
    logging.info('Processing batch {}...'.format(file_base))
    total_read = 0
    with closing(BatchWriter(sys.maxsize, output_dir,
                             len(file_base), int(file_base))) as bw:
        for input_file, results in read_batch(file_prefix):
            minhashes, new_minhashes = results['minhash'], []
            doc_ids, new_doc_ids = results['id'], []
            total_read += len(doc_ids)
            for i, minhash in enumerate(minhashes):
                if not lsh.query(minhash):
                    lsh.insert(doc_ids[i], minhash)
                    new_minhashes.append(minhash)
                    new_doc_ids.append(doc_ids[i])
            bw.write_results(input_file,
                             {'id': new_doc_ids, 'minhash': new_minhashes})
            logging.debug('Kept {} documents out of {}'.format(
                len(new_doc_ids), len(doc_ids)))
        logging.info('Processed batch {}; kept {} documents out of {}.'.format(
            file_base, bw.total_written, total_read))


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
