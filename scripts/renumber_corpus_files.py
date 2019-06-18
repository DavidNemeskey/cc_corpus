#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Renumbers corpus files in one or several input directories. This script can be
used to even out the number of documents in the files.
"""

from argparse import ArgumentParser
from contextlib import closing
import logging
import os
import sys

from cc_corpus.corpus import BatchWriter, parse_file
from cc_corpus.utils import collect_inputs


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('input_dirs', nargs='+',
                        help='the input directories.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    size_group = parser.add_mutually_exclusive_group()
    size_group.add_argument('--documents', '-d', required=True,
                            help='the number of documents a file should contain.')
    size_group.add_argument('--keep-sizes', '-k', action='store_true',
                            help='do not merge or split files; i.e. only '
                                 'copies files to the output directory.')
    parser.add_argument('--zeroes', '-Z', type=int, default=4,
                        help='the number of zeroes in the output files\' names.')
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


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    input_files = collect_inputs(args.input_dirs)
    logging.info('Scheduled {} files for renumbering.'.format(len(input_files)))

    batch_size = args.documents if not args.keep_sizes else sys.maxsize
    num_docs = 0
    with closing(BatchWriter(batch_size, args.output_dir, args.zeroes)) as bw:
        for input_file in input_files:
            if not args.keep_sizes:
                logging.debug('Reading file {}...'.format(input_file))
                for document in parse_file(input_file):
                    bw.write(document)
                    num_docs += 1
            else:
                logging.debug('Copying file {}...'.format(input_file))
                bw.copy_file(input_file)

    if not args.keep_sizes:
        logging.info(
            'Done. Renumbered {} files to {}, {} documents each; '
            '{} documents in total.'.format(
                len(input_files), args.output_dir, args.documents, num_docs)
        )
    else:
        logging.info('Done. Renumbered {} files to {}.'.format(
                     len(input_files), args.output_dir))


if __name__ == '__main__':
    main()
