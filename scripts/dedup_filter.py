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
import re
import shutil
import sys

from cc_corpus.deduplication import BatchWriter, read_batch


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


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)


if __name__ == '__main__':
    main()
