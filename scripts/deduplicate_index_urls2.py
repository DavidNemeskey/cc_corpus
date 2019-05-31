#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Deduplicates the urls in the index."""

from argparse import ArgumentParser
from collections import defaultdict
from functools import partial
import gzip
import logging
from multiprocessing import Pool
import os
import os.path as op
import re
from typing import Set

from multiprocessing_logging import install_mp_handler
from url_normalize import url_normalize

from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the index directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--skip-urls', '-s', metavar='URL_FILE', default=None,
                        help='a file with the list of URLs to skip (i.e. '
                             'drop). Typically, these are URLs already '
                             'downloaded in a previous batch.')
    parser.add_argument('--keep', '-k', choices=['latest', 'biggest'],
                        default='biggest',
                        help='which occurrence to keep. Default: biggest.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def read_urls(urls_file: str) -> Set[int]:
    """
    Reads URLS from the file ``urls_file``, one per line. The URLs are
    normalized and their hashes are returned in a set.

    We use hashes instead of the full url to conserve memory. In our
    experiments, we have not encountered collisions yet.

    Note: normalization keeps the separate http / https versions. Hopefully,
    document deduplication will take care of this.
    """
    with openall(urls_file) as inf:
        hashes = set()
        for no_urls, url in enumerate(map(str.strip, inf), start=1):
            hashes.add(hash(url_normalize(url)))
        logging.info('Loaded {} urls from {}; {} unique hashes.'.format(
            no_urls, urls_file, len(hashes)))
        return hashes


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    skip_hashes = read_urls(args.skip_urls) if args.skip_urls else set()


if __name__ == '__main__':
    main()
