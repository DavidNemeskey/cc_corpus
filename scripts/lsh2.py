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
import re
import sys

from datasketch import MinHashLSH

from cc_corpus.deduplication import BatchWriter, read_batch


def parse_arguments():
    parser = ArgumentParser('Deduplicates the documents with Locality '
                            'Sensitive Hashing, based on the files written '
                            'by minhash.py.')
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the input directory that contains the minhash '
                             'batches to deduplicate.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the directory to which the updated minhash '
                             'files are written.')
    parser.add_argument('--threshold', '-t', type=float, default=0.9,
                        help='the Jaccard similarity threshold (0.9).')
    parser.add_argument('--permutations', '-p', type=int, default=256,
                        help='the number of permutations per paragraph (256).')
    # TODO maybe to a threshold number > the regular threshold?
    parser.add_argument('--skip-same-doc', '-s', action='store_true',
                        help='if true, does not deduplicate paragraphs from '
                             'the same document.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1). Note that in order '
                             'to deduplicate documents, much memory might be '
                             'needed, so it is a good idea to be conservative '
                             'with the number of processes.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


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


def deduplicate_self(file_prefix, output_dir, threshold, permutations):
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
                    lsh.insert('_'.join(doc_ids[i]), minhash)
                    new_minhashes.append(minhash)
                    new_doc_ids.append(doc_ids[i])
            bw.write_results(input_file,
                             {'id': new_doc_ids, 'minhash': new_minhashes})
            logging.debug('Kept {} documents out of {}'.format(
                len(new_doc_ids), len(doc_ids)))
    logging.info('Processed batch {}; kept {} documents out of {}.'.format(
        file_base, bw.total_written, total_read))
    return bw.total_written, total_read


def deduplicate_other(file_prefix, working_dir, threshold, permutations):
    """
    Removes all documents from a set of minhashed documents (3 files with the
    same minhash prefix) that occur in other batches in working_dir. Only
    batches whose number is higher than the batch in question are considered
    (i.e. upper triangular matrix).

    We do not overwrite the original batch files, because that might cause
    concurrency-related problems. So we just save batches named 1_, 2_, etc.

    Warning: only works for full documents at this point!
    """
    lsh = MinHashLSH(threshold=threshold, num_perm=permutations)
    file_base = op.basename(file_prefix)
    logging.info('Processing batch {}...'.format(file_base))

    # First, load the (already deduplicated) batch...
    for input_file, results in read_batch(file_prefix):
        for doc_id, minhash in zip(results['id'], results['minhash']):
            lsh.insert('\t'.join(doc_id), minhash)

    initial_len = len(lsh.keys)
    to_match_with = find_all_batches(working_dir,
                                     int(working_dir.rpartition(os.sep)[-1]))

    # Now, remove all documents in it that are contained in other batches
    # to the "right" of it (with greater batch numbers)
    for batch in to_match_with:
        initial_batch_len = len(lsh.keys)
        for _, results in read_batch(batch):
            for i, minhash in enumerate(results['minhash']):
                for duplicate in lsh.query(minhash):
                    lsh.remove(duplicate)
        logging.info('Cross-deduplicated with batch {}: {} -> {} documents.'.format(
            os.basename(batch), initial_batch_len, len(lsh.keys)))

    # Finally, we print the documents left. Unfortunately, in order to
    # keep the format, we have to read the original batch again.
    with closing(BatchWriter(sys.maxsize, output_dir, len(file_base),
                             int(file_base), batch_format='{}_')) as bw:
        # OK, we need to re-read the batch unfortunately
        for input_file, results in read_batch(file_prefix):
            doc_ids, minhashes = [], []
            for doc_id, minhash in zip(results['id'], results['minhash']):
                if doc_id in lsh:
                    doc_ids.append(doc_id)
                    minhashes.append(minhash)
            bw.write_results(input_file, {'id': doc_ids, 'minhash': minhashes})
    logging.info('Processed batch {}; kept {} out of {} documents.'.format(
        file_base, len(lsh.keys), initial_len))
    return len(lsh.keys), initial_len


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    batch_prefixes = find_all_batches(args.input_dir)
    logging.info('Found a total of {} batches.'.format(len(batch_prefixes)))

    # First, deduplicate documents _within_ the same batch
    original_doc_num, diagonal_doc_num, final_doc_num = 0, 0, 0
    with Pool(args.processes) as pool:
        f = partial(deduplicate_self, output_dir=args.output_dir,
                    threshold=args.threshold, permutations=args.permutations)
        new_num, old_num = pool.map(f, batch_prefixes)
        original_doc_num += old_num
        diagonal_doc_num += new_num
    pool.close()
    pool.join()

    logging.info('Self deduplication done; in all, kept '
                 '{} documents out of {}.'.format(diagonal_doc_num,
                                                  original_doc_num))

    # Now, we need to do the deduplication between batches. The idea here is
    # to load one batch into memory, and delete all documents from it that are
    # also present in any of the other batches (more precisely, we only need to
    # do the upper triangle matrix).
    # At this point, we do all work in output_dir.
    with Pool(args.processes) as pool:
        f = partial(deduplicate_other, output_dir=args.output_dir,
                    threshold=args.threshold, permutations=args.permutations)
        new_num, old_num = pool.map(f, batch_prefixes)
        final_doc_num += new_num
    pool.close()
    pool.join()

    logging.info('Full deduplication done; in all, kept '
                 '{} documents out of {}.'.format(final_doc_num,
                                                  original_doc_num))

    # The last step created batches names 1_, 2_, etc. Let's get rid of the
    # underscore (and the partial results).
    for f in os.listdir(args.output_dir):
        if re.match('(\d+)_[.](doc_ids|files|minhashes)$', f):
            os.replace(op.join(args.output_dir, f),
                       op.join(args.output_dir, f.replace('_', '')))


if __name__ == '__main__':
    main()
