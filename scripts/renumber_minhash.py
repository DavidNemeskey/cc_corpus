#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Renumbers minhash files in one or several input directories. This script can be
used to even out the number of documents in the files.
"""

from argparse import ArgumentParser
from contextlib import closing
import logging
import os
import os.path as op

from cc_corpus.deduplication import BatchWriter, find_all_batches, read_batch


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        action='append', default=[], dest='input_dirs',
                        help='an input directory that contains the minhash '
                             'batches to deduplicate. Can be specified more '
                             'than once.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the directory to which the updated minhash '
                             'files are written.')
    parser.add_argument('--batch-size', '-b', type=int, default=1000000,
                        help='the number of units in a single batch. '
                             'This is not an exact number, as documents in '
                             'the same data files are always put into the same '
                             'batch.')
    parser.add_argument('--zeroes', '-Z', type=int, default=4,
                        help='the number of zeroes in the batch files\' names.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    return parser.parse_args()


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    os.nice(20)

    if not op.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    input_batches = [batch_prefix for input_dir in args.input_dirs
                     for batch_prefix in find_all_batches(input_dir)]

    logging.info('Found a total of {} input batches.'.format(len(input_batches)))

    with closing(BatchWriter(args.batch_size, args.output_dir,
                             args.zeroes)) as bw:
        for input_batch in input_batches:
            logging.info('Reading batch {}...'.format(input_batch))
            for input_file, results in read_batch(input_batch):
                bw.write_results(input_file, results)

    logging.info('Done; renumbered {} documents.'.format(bw.total_written))


if __name__ == '__main__':
    main()