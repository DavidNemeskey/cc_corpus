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
from urllib.parse import urlsplit

from cc_corpus.corpus import parse_file
from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('input_dir', help='the corpus directory.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1). Note that in order '
                             'to deduplicate documents, much memory might be '
                             'needed, so it is a good idea to be conservative '
                             'with the number of processes.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    subparsers = parser.add_subparsers(
        help='The steps of frequent paragraph detection.')
    parser_index = subparsers.add_parser(
        'index_docs', aliases=['index'],
        help='Indexes the documents in the corpus and sorts the index by '
             'domain and corpus location.'
    )
    parser_index.set_defaults(command='index_docs')
    parser_index.add_argument('--index-file', '-i', required=True,
                              help='the output index file.')

    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def index_file(input_file, input_dir):
    """
    Indexes an input file. Returns two items:
    - the input file: since this function is called (kind of) asynchronously,
      we need to keep track of it
    - a list of tuples for each document: its url, position and length in the
      file.
    """
    input_path = op.join(input_dir, input_file)
    urls, lens = [], []
    for doc in parse_file(input_path):
        urls.append(doc.attrs['url'])
        lens.append(doc.stream_size())
    return input_file, list(zip(urls, accumulate([0] + lens[:-1]), lens))


def index_key(url_file_pos_len):
    """The key function for index list sorting."""
    url, input_file, input_pos, _ = url_file_pos_len
    return (urlsplit(url).netloc.split('.')[::-1], input_file, input_pos)


def main_index_documents(args):
    """The main function for indexing documents."""
    input_files = os.listdir(args.input_dir)
    index = []
    with Pool(args.processes) as pool:
        f = partial(index_file, input_dir=args.input_dir)
        for input_file, urls_poss_lens in pool.imap(f, input_files):
            for doc_url, doc_pos, doc_len in urls_poss_lens:
                index.append((doc_url, input_file, doc_pos, doc_len))
                # print(doc_url, input_file, doc_pos, doc_len, sep='\t')
    pool.close()
    pool.join()

    index.sort(key=index_key)
    with openall(args.index_file, 'wt') as outf:
        for doc_tuple in index:
            print('\t'.join(doc_tuple), file=outf)


def main():
    args = parse_arguments()
    if args.command == 'index_docs':
        main_index_documents(args)


if __name__ == '__main__':
    main()
