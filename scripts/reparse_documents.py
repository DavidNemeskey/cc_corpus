#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reparses documents, which enables the omission of certain parts: i.e.
attributes, meta or content, or even individual meta fields.

This script is basically a front-end for the parse() function in the corpus
module.
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Pool
import os
import os.path as op
import sys

from multiprocessing_logging import install_mp_handler
from tqdm import tqdm

from cc_corpus.corpus import parse_file
from cc_corpus.utils import collect_inputs, consume


# tqdm to print the progress bar to stdout. This helps keeping the log clean.
otqdm = partial(tqdm, file=sys.stdout)


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories to reparse.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--attrs', '-a', action='store_true',
                        help='include the attributes in the output.')
    parser.add_argument('--meta', '-m', action='store_true',
                        help='include the meta data in the output.')
    parser.add_argument('--content', '-c', action='store_true',
                        help='include the textual content in the output.')
    parser.add_argument('--meta-field', '-f', action='append', default=[],
                        dest='meta_fields',
                        help='include the specified meta field in the output. '
                             'Takes precedence over --meta/-m.')
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
    if not (args.attrs or args.meta or args.content or args.meta_fields):
        parser.error('At least one of -a, -m, -c or -f must be specified.')
    return args


def reparse(input_file: str, output_dir: str,
            attrs: bool, meta: bool, content: bool, **meta_fields: bool):
    logging.debug(f'Reparsing file {input_file}...')
    with open(op.join(output_dir, op.basename(input_file)), 'wt') as outf:
        for doc in parse_file(input_file, attrs, meta, content, **meta_fields):
            print(doc, file=outf)
    logging.debug(f'Reparsed file {input_file}.')


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

    files = sorted(collect_inputs(args.inputs))
    logging.info('Scheduled {} files for reparsing.'.format(len(files)))

    with Pool(args.processes) as pool:
        f = partial(reparse, output_dir=args.output_dir,
                    attrs=args.attrs, meta=args.meta, content=args.content,
                    meta_fields={field: True for field in args.meta_fields})
        consume(otqdm(pool.imap_unordered(f, files),
                desc=f'Reparsing corpus files...', total=len(files)))

        pool.close()
        pool.join()
    logging.info('Done.')


if __name__ == '__main__':
    main()
