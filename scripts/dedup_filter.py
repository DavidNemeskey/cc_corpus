#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Filters the corpus files based on the output of lsh.py. It only works on a
document (not paragraph) basis.

Empty files are not written to the output directory.

Having a separate script instead of using just filter_corpus.py seems
redundant, but this specialized script avoids potential memory issues that
might arise otherwise.
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Pool
import os
import os.path as op

from cc_corpus.corpus import parse_file
from cc_corpus.deduplication import find_all_batches, read_batch
from cc_corpus.utils import notempty, openall


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--minhash-dir', '-m', required=True,
                        help='the input directory that contains the minhash '
                             'batches to deduplicate. The .files file contains '
                             'the names of the (corpus) input files.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the directory to which the updated minhash '
                             'files are written.')
    parser.add_argument('--input-dir', '-i',
                        help='the directory that contains the corpus files to '
                             'filter. Since the minhash files store the names '
                             'of the input files, this argument is only '
                             'necessary if that information is outdated, e.g. '
                             'the files have been moved.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1).')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def deduplicate_batch_documents(batch_prefix, output_dir, input_dir=None):
    """
    Filters documents not present in the batch and writes the filtered corpus
    files to output_dir. As above, input_dir can be specified if the location
    information in the batch files is outdated.

    Empty files will not be written.
    """
    batch_base = op.basename(batch_prefix)
    logging.info('Filtering batch {}...'.format(batch_base))

    kept, total = 0, 0
    for input_file, results in read_batch(batch_prefix):
        file_base = op.basename(input_file)
        url_set = set('_'.join(doc_id) for doc_id in results['id'])
        input_file = op.join(input_dir, file_base) if input_dir else input_file
        with notempty(openall(op.join(output_dir, file_base), 'wt')) as outf:
            for doc_no, doc in enumerate(parse_file(input_file), start=1):
                if doc.attrs['url'] in url_set:
                    print(doc, file=outf)
                    kept += 1
            total += doc_no

    logging.info('Filtered batch {}; kept {} documents out of {}.'.format(
        batch_base, kept, total))
    return kept, total


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    batch_prefixes = find_all_batches(args.minhash_dir)
    logging.info('Found a total of {} batches.'.format(len(batch_prefixes)))

    with Pool(args.processes) as pool:
        f = partial(deduplicate_batch_documents,
                    output_dir=args.output_dir, input_dir=args.input_dir)
        kept, total = 0, 0
        for batch_kept, batch_total in pool.imap(f, batch_prefixes):
            kept += batch_kept
            total += batch_total
        pool.close()
        pool.join()
    logging.info('Done.')

    logging.info('Kept {} documents out of {} in total'.format(kept, total))


if __name__ == '__main__':
    main()
