#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Deduplicates the documents with Locality Sensitive Hashing, based on the files
written by minhash.py.
"""

from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from functools import partial
import logging
from multiprocessing import Pool
import os
import os.path as op
import shutil
import sys

from datasketch import MinHashLSH
from multiprocessing_logging import install_mp_handler

from cc_corpus.deduplication import BatchWriter, find_all_batches, read_batch


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
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
    subparsers = parser.add_subparsers(
        help='Choose between two deduplication tasks.')
    parser_self = subparsers.add_parser(
        'self', aliases=['auto'], help='Fully deduplicate a corpus.')
    parser_self.set_defaults(command='self')
    parser_other = subparsers.add_parser(
        'other', aliases=['cross'],
        help='Remove all documents from a corpus that are found in another.'
    )
    parser_other.set_defaults(command='other')
    parser_other.add_argument('--cross-dir', '-c', required=True,
                              help='the directory that contains the minhash '
                                   'values for the corpus to cross-deduplicate '
                                   'with.')

    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    if args.command == 'other' and not op.isdir(args.cross_dir):
        parser.error('The minhash directory for the other corpus (-c) must exist.')
    return args


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
    duplicate_urls = 0
    with closing(BatchWriter(sys.maxsize, output_dir,
                             len(file_base), int(file_base))) as bw:
        for input_file, results in read_batch(file_prefix):
            minhashes, new_minhashes = results['minhash'], []
            doc_ids, new_doc_ids = results['id'], []
            total_read += len(doc_ids)
            input_duplicate_urls = 0
            for doc_id, minhash in zip(doc_ids, minhashes):
                key = '_'.join(doc_id)
                if key in lsh:
                    input_duplicate_urls += 1
                    continue
                if not lsh.query(minhash):
                    lsh.insert(key, minhash)
                    new_minhashes.append(minhash)
                    new_doc_ids.append(doc_id)
            bw.write_results(input_file,
                             {'id': new_doc_ids, 'minhash': new_minhashes})
            duplicate_urls += input_duplicate_urls
            logging.debug('Kept {} documents out of {} in file {}; '
                          '{} duplicate urls.'.format(
                              len(new_doc_ids), len(doc_ids),
                              input_file, input_duplicate_urls))
    logging.info('Deduplicated batch {}; kept {} documents out of {}; '
                 '{} duplicate urls.'.format(
                     file_base, bw.total_written, total_read, duplicate_urls))
    return bw.total_written, total_read


def deduplicate_other_old(file_prefix, input_dir, output_dir,
                          threshold, permutations):
    """
    Removes all documents from a set of minhashed documents (3 files with the
    same minhash prefix) that occur in other batches in input_dir. Only
    batches whose number is higher than the batch in question are considered
    (i.e. upper triangular matrix).

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
    to_match_with = find_all_batches(input_dir,
                                     int(file_prefix.rpartition(os.sep)[-1]))

    # Now, remove all documents in it that are contained in other batches
    # to the "right" of it (with greater batch numbers)
    for batch in to_match_with:
        initial_batch_len = len(lsh.keys)
        for _, results in read_batch(batch):
            for i, minhash in enumerate(results['minhash']):
                for duplicate in lsh.query(minhash):
                    lsh.remove(duplicate)
        logging.info(
            'Cross-deduplicated batch {} with batch {}: {} -> {} documents.'.format(
                file_base, op.basename(batch), initial_batch_len, len(lsh.keys))
        )

    # Finally, we print the documents left. Unfortunately, in order to
    # keep the format, we have to read the original batch again.
    with closing(BatchWriter(sys.maxsize, output_dir,
                             len(file_base), int(file_base))) as bw:
        # OK, we need to re-read the batch unfortunately
        for input_file, results in read_batch(file_prefix):
            doc_ids, minhashes = [], []
            for doc_id, minhash in zip(results['id'], results['minhash']):
                if '\t'.join(doc_id) in lsh:
                    doc_ids.append(doc_id)
                    minhashes.append(minhash)
            bw.write_results(input_file, {'id': doc_ids, 'minhash': minhashes})
    logging.info('Processed batch {}; kept {} out of {} documents.'.format(
        file_base, len(lsh.keys), initial_len))
    return len(lsh.keys), initial_len


def deduplicate_other(main_batch, batches_to_subtract, output_dir,
                      threshold, permutations):
    """
    Removes all documents from a set of minhashed documents (3 files with the
    same minhash prefix) that occur in other batches. Both main_batch and
    batches_to_subtract should be batch prefixes.

    Warning: only works for full documents at this point!
    """
    lsh = MinHashLSH(threshold=threshold, num_perm=permutations)
    main_base = op.basename(main_batch)
    logging.info('Processing input batch {}...'.format(main_base))

    # First, load the (already deduplicated) batch...
    for input_file, results in read_batch(main_batch):
        for doc_id, minhash in zip(results['id'], results['minhash']):
            lsh.insert('\t'.join(doc_id), minhash)
    initial_len = len(lsh.keys)

    # Now, remove all documents in it that are contained in th batches
    # to subtract
    content_duplicates, url_duplicates = 0, 0
    for batch in batches_to_subtract:
        batch_content_duplicates, batch_url_duplicates = 0, 0
        initial_batch_len = len(lsh.keys)
        for _, results in read_batch(batch):
            for doc_id, minhash in zip(results['id'], results['minhash']):
                key = '_'.join(doc_id)
                if key in lsh:
                    batch_url_duplicates += 1
                    lsh.remove(key)
                else:
                    for duplicate in lsh.query(minhash):
                        lsh.remove(duplicate)
                        batch_content_duplicates += 1
        logging.info(
            'Cross-deduplicated input batch {} with cross batch {}: {} -> {} '
            'documents (removed {} by url, {} by content).'.format(
                main_base, op.basename(batch), initial_batch_len, len(lsh.keys),
                batch_url_duplicates, batch_content_duplicates)
        )
        content_duplicates += batch_content_duplicates
        url_duplicates += batch_url_duplicates

    # Finally, we print the documents left. Unfortunately, in order to
    # keep the format, we have to read the original batch again.
    with closing(BatchWriter(sys.maxsize, output_dir,
                             len(main_base), int(main_base))) as bw:
        # OK, we need to re-read the batch unfortunately
        for input_file, results in read_batch(main_batch):
            doc_ids, minhashes = [], []
            for doc_id, minhash in zip(results['id'], results['minhash']):
                if '\t'.join(doc_id) in lsh:
                    doc_ids.append(doc_id)
                    minhashes.append(minhash)
            bw.write_results(input_file, {'id': doc_ids, 'minhash': minhashes})
    logging.info('Processed input batch {}; kept {} out of {} documents '
                 '(removed {} by url, {} by content).'.format(
                     main_base, len(lsh.keys), initial_len,
                     url_duplicates, content_duplicates))
    return len(lsh.keys), initial_len


def self_main(args):
    """The "real" main function of the "self" mode."""
    working_dir = op.join(args.output_dir, 'self')
    if not os.path.isdir(working_dir):
        os.makedirs(working_dir)

    batch_prefixes = find_all_batches(args.input_dir)
    logging.info('Found a total of {} batches in {}.'.format(
        len(batch_prefixes), args.input_dir))

    # First, deduplicate documents _within_ the same batch
    original_doc_num, self_doc_num, final_doc_num = 0, 0, 0
    with Pool(args.processes) as pool:
        f = partial(deduplicate_self, output_dir=working_dir,
                    threshold=args.threshold, permutations=args.permutations)
        for new_num, old_num in pool.map(f, batch_prefixes):
            original_doc_num += old_num
            self_doc_num += new_num
    pool.close()
    pool.join()

    logging.info('Self deduplication done; in all, kept '
                 '{} documents out of {}.'.format(self_doc_num,
                                                  original_doc_num))

    # Now, we need to do the deduplication between batches. The idea here is
    # to load one batch into memory, and delete all documents from it that are
    # also present in any of the other batches (more precisely, we only need to
    # do the upper triangle matrix: batch b_i is deduplicated with batches b_j,
    # where j > i).
    # At this point, we do all work in output_dir.
    # Yes, there is no need to send the last batch through this round, except
    # for counting final_doc_num.
    batch_prefixes = find_all_batches(working_dir)
    batches_to_subtract = [
        find_all_batches(working_dir, int(op.basename(file_prefix)))
        for file_prefix in batch_prefixes
    ]

    with ProcessPoolExecutor(max_workers=args.processes) as executor:
        f = partial(deduplicate_other, output_dir=args.output_dir,
                    threshold=args.threshold, permutations=args.permutations)
        final_doc_num = sum(num for num, _ in
                            executor.map(f, batch_prefixes, batches_to_subtract))

    logging.info('Full deduplication done; in all, kept '
                 '{} documents out of {}.'.format(final_doc_num,
                                                  original_doc_num))

    # Let's delete the intermediate directory.
    shutil.rmtree(working_dir)


def other_main(args):
    """The "real" main function of the "other" mode."""
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    batch_prefixes = find_all_batches(args.input_dir)
    logging.info('Found a total of {} batches in {}.'.format(
        len(batch_prefixes), args.input_dir))

    batches_to_subtract = find_all_batches(args.cross_dir)
    logging.info(f'Found a total of {len(batches_to_subtract)} batches in '
                 f'{args.cross_dir} to deduplicate against.')

    with ProcessPoolExecutor(max_workers=args.processes) as executor:
        f = partial(deduplicate_other, batches_to_subtract=batches_to_subtract,
                    output_dir=args.output_dir,
                    threshold=args.threshold, permutations=args.permutations)
        original_doc_num, final_doc_num = 0, 0
        for new_num, old_num in executor.map(f, batch_prefixes):
            original_doc_num += old_num
            final_doc_num += new_num

    logging.info('Cross deduplication done; in all, kept '
                 '{} documents out of {}.'.format(final_doc_num,
                                                  original_doc_num))


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()
    os.nice(20)

    self_main(args) if args.command == 'self' else other_main(args)


if __name__ == '__main__':
    main()
