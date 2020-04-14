#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Filters documents in a corpus in the tsv format. Currently only the length
filter is supported.
"""

from argparse import ArgumentParser
from collections import Counter
from functools import partial
import logging
from multiprocessing import Pool
import os
import re

from multiprocessing_logging import install_mp_handler

from cc_corpus.tsv import parse_file, Sentence
from cc_corpus.utils import openall, notempty


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the input directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--min-len', '-m', type=str, required=True,
                        help='the minimum number of characters / words in a '
                             'document. Activates length filtering. Values '
                             'are accepted in the format of e.g. 500c and 100w.')
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
    if args.min_len and not re.match(r'^\d+[w|c]$', args.min_len):
        parser.error('Invalid value for the minimum length parameter.')
    return args


def each_doc(doc_iter, stats):
    """
    This function is just there so that we can count the number of documents
    initially.
    """
    doc_no = 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        yield doc
    stats['initial'] = doc_no


def num_chars(tsv_unit):
    """Returns the number of documents in a tsv unit."""
    if isinstance(tsv_unit, Sentence):
        return sum(len(fields[0]) for fields in tsv_unit.content)
    else:
        return sum(num_chars(c) for c in tsv_unit.content)


def filter_length(doc_iter, min_len_str, stats):
    min_len = int(min_len_str[:-1])
    len_fn = len if min_len_str[-1].lower() == 'w' else num_chars

    doc_no, kept = 0, 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        if len_fn(doc) >= min_len:
            kept += 1
            yield doc
    if doc_no:
        logging.info('Filtered {} documents based on length, kept {}.'.format(
            doc_no, kept))
    stats['length'] = kept


def process_file(filename, input_dir, output_dir, min_len_str):
    input_file = os.path.join(input_dir, filename)
    output_file = os.path.join(output_dir, filename)
    logging.info('Processing file {}...'.format(filename))

    stats = Counter()
    it = parse_file(input_file, True)
    it = each_doc(it, stats)
    if min_len_str:
        it = filter_length(it, min_len_str, stats)
    try:
        with notempty(openall(output_file, 'wt')) as outf:
            for doc in it:
                print(doc, file=outf)
    except:
        logging.exception(f'Got an error in file {input_file}.')
    logging.info('Finished processing file {}...'.format(filename))
    return stats


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
    logging.info('Scheduled {} files for filtering.'.format(len(files)))

    with Pool(args.processes) as p:
        f = partial(process_file, input_dir=args.input_dir,
                    output_dir=args.output_dir, min_len_str=args.min_len)
        # Note: + / sum() do not keep keys with 0 values here, hence update()
        stats = Counter()
        for sub_stats in p.map(f, files):
            stats.update(sub_stats)
        logging.info('Statistics: {}'.format(stats))
        p.close()
        p.join()
        logging.info('Done.')


if __name__ == '__main__':
    main()
