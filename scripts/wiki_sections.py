#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extracts all section titles from a Wikipedia extract created by
`WikiExtractor.py <https://github.com/attardi/wikiextractor>`_.
"""

from argparse import ArgumentParser
from collections import Counter
from io import StringIO
import json
import logging
from multiprocessing import Pool
import os
import re

from multiprocessing_logging import install_mp_handler

from cc_corpus.utils import collect_inputs, openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories of Wikipedia extracts.')
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


def process_file(filename):
    logging.info('Processing file {}...'.format(filename))
    counter = Counter()
    p = re.compile('Section::::(.+)')
    with openall(filename, 'rt') as inf:
        for page in inf:
            j = json.loads(page)
            for line in StringIO(j['text']):
                m = p.search(line)
                if m:
                    counter[m.group(1)] += 1
                    if m.start() != 0:
                        logging.warning(f'Section not on first character: '
                                        f'{line.strip()} in {filename}')
    logging.info('Finished processing file {}...'.format(filename))
    return counter


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    input_files = sorted(collect_inputs(args.inputs))
    logging.info('Scheduled {} files for filtering.'.format(len(input_files)))

    with Pool(args.processes) as pool:
        counter = Counter()
        for c in pool.imap_unordered(process_file, input_files):
            counter.update(c)

    for section, freq in counter.most_common():
        print(f'{section}\t{freq}')


if __name__ == '__main__':
    main()
