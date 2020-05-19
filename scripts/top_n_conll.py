#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Collects the top n most frequent values in a CoNLL column."""

from argparse import ArgumentParser
from collections import Counter
from functools import partial
import logging
from multiprocessing import Pool
import os
from typing import Union

from multiprocessing_logging import install_mp_handler

from cc_corpus.tsv import parse_file
from cc_corpus.utils import collect_inputs


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('inputs', nargs='*',
                        help='the files/directories to count.')
    parser.add_argument('--column', '-c', default=0,
                        help='Which column\'s content to count. Both numbers '
                             'and column names are accepted. Default is 0 '
                             '(form).')
    parser.add_argument('--n', '-n', type=int, default=50000,
                        help='The number of words to return (50000).')
    parser.add_argument('--lower', '-l', action='store_true',
                        help='Lower case the content of the column.')
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
    # Convert column to integer, if it is
    try:
        args.column = int(args.column)
    except ValueError:
        pass
    return args


def process_file(file_name: str, column: Union[int, str], n: int, lower: bool):
    logging.debug(f'Processing {file_name}...')
    c = Counter()
    it = parse_file(file_name)
    header = next(it)
    fn = str.lower if lower else lambda s: s
    if isinstance(column, str):
        column = header.index(column)
    for doci, doc in enumerate(it, 1):
        for ti, token in enumerate(doc.tokens(), 1):
            try:
                c[fn(token[column])] += 1
            except Exception as e:
                logging.exception(f'Error in token {ti} in document {doci} '
                                  f'({doc.comment}) in file {file_name}.')
                # We tolerate IndexErrors, which step from crappy tokenization
                if not isinstance(e, IndexError):
                    raise
    logging.debug(f'Processed {file_name}; found {len(c)} types.')
    return c


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)

    files = collect_inputs(args.inputs)
    logging.info(f'Scheduled {len(files)} files for finding top {args.n} '
                 f'{"lower cased " if args.lower else ""} values in column '
                 f'{args.column}...')

    with Pool(args.processes) as p:
        c_all = Counter()
        f = partial(process_file, column=args.column,
                    n=args.n, lower=args.lower)
        for c in p.imap_unordered(f, files):
            c_all.update(c)
        for key, freq in sorted(c_all.most_common(args.n),
                                key=lambda kv: (-kv[1], kv[0])):
            print(f'{key}\t{freq}')


if __name__ == '__main__':
    main()
