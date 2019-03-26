#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Writes the positions of all documents in each file."""

from argparse import ArgumentParser
from functools import partial
from itertools import accumulate
import logging
from multiprocessing import Pool
import os
import os.path as op

from cc_corpus.corpus import parse_file


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('input_dir', required=True,
                        help='the corpus directory.')
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


def process_file(input_file, input_dir):
    input_path = op.join(input_dir, input_file)
    urls, lens = [], []
    for doc in parse_file(input_path):
        urls.append(doc.attrs['url'])
        lens.append(doc.stream_size())
    return input_file, list(zip(urls, accumulate([0] + lens[:-1]), lens))


def main():
    args = parse_arguments()

    input_files = os.listdir(args.input_dir)
    # result = []
    with Pool(args.processes) as pool:
        f = partial(process_file, input_dir=args.input_dir)
        for input_file, urls_poss_lens in pool.imap(f, input_files):
            for doc_url, doc_pos, doc_len in urls_poss_lens:
                # result.append((doc_url, input_file, doc_pos, doc_len))
                print(doc_url, input_file, doc_pos, doc_len)
    pool.close()
    pool.join()


if __name__ == '__main__':
    main()
