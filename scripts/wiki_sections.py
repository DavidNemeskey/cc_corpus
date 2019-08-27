#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extracts all section titles from a Wikipedia extract created by
`WikiExtractor.py <https://github.com/attardi/wikiextractor>`_.
"""

from argparse import ArgumentParser
from collections import Counter
from functools import partial
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
    parser.add_argument('--filter', '-f', action='store_true',
                        help='filter bullets and keep only non-empty sections.')
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


def process_file(filename, filter=False):
    logging.info('Processing file {}, {}...'.format(filename, filter))
    counter = Counter()
    section_p = re.compile('Section::::(.+)')
    bullet_p = re.compile('BULLET::::')
    with openall(filename, 'rt') as inf:
        for page in inf:
            j = json.loads(page)
            section_title = None
            section_text = []
            for line in map(str.strip, StringIO(j['text'])):
                sm = section_p.search(line)
                if sm:
                    if section_title and (section_text or not filter):
                        counter[section_title] += 1
                    section_title = sm.group(1)
                    section_text = []
                    if sm.start() != 0:
                        logging.warning(f'Section not on first character: '
                                        f'{line.strip()} in {filename}')
                else:
                    bm = bullet_p.search(line)
                    if line and not bm:
                        section_text.append(line)
            if section_title and section_text:
                counter[section_title] += 1
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
        f = partial(process_file2, filter=args.filter)
        counter = Counter()
        for c in pool.imap_unordered(f, input_files):
            counter.update(c)

    for section, freq in sorted(counter.items(), key=lambda sf: (-sf[1], sf[0])):
        print(f'{section}\t{freq}')


if __name__ == '__main__':
    main()
