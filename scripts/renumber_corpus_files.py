#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Renumbers corpus files in one or several input directories. This script can be
used to even out the number of documents in the files.
"""

from argparse import ArgumentParser
import logging
from multiprocessing import Pool
import os
import os.path as op

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the corpus directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--documents', '-d', required=True,
                        help='the number of documents a file should contain.')
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


def read_files(input_file):
    return [doc for doc in parse_file(input_file)]


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    files = os.listdir(args.input_dir)
    logging.info('Scheduled {} files for renumbering.'.format(len(files)))

    with Pool(args.processes) as pool:
        pool.close()
        pool.join()

    logging.info('Done. Renumbered {} files to {}, {} documents each; '
                 '{} documents in total.'.format(
                     len(files), XXX, args.documents, YYY))


if __name__ == '__main__':
    main()
