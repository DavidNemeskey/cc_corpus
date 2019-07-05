#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Rewrites files into another format."""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Pool
import os
import os.path as op

from multiprocessing_logging import install_mp_handler

from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('input_dir', help='the input directory.')
    parser.add_argument('output_dir', help='the input directory.')
    parser.add_argument('--format', '-f', required=True,
                        help='the file extension of the output format.')
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


def rewrite_file(input_file, input_dir, output_dir, extension):
    ipath = op.join(input_dir, input_file)
    opath = op.join(output_dir, '{}.{}'.format(
        op.splitext(input_file)[0], extension))
    with openall(ipath, 'rt') as inf, openall(opath, 'wt') as outf:
        for line in inf:
            outf.write(line)


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

    input_files = os.listdir(args.input_dir)
    logging.info('Found a total of {} input files.'.format(len(input_files)))

    with Pool(args.processes) as pool:
        f = partial(rewrite_file, input_dir=args.input_dir,
                    output_dir=args.output_dir, extension=args.format)
        pool.map(f, input_files)
    pool.close()
    pool.join()

    logging.info('Done.')


if __name__ == '__main__':
    main()
