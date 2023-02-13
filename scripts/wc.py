#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Counts the number of documents, paragraphs, words and / or characters in
(a set of) documents in WARC or the corpus format.
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Pool
import os

from multiprocessing_logging import install_mp_handler
import warc

from cc_corpus.corpus import parse_file
from cc_corpus.utils import collect_inputs, otqdm


def parse_arguments():
    parser = ArgumentParser(
        description='{}\nSimilarly to Unix wc, any number of these switches '
                    'can be specified, and then only those numbers are '
                    'printed; if no flags are specified, all numbers are '
                    'printed.\n\nFor WARC, number of paragraphs and words '
                    'will always be 0.'.format(__doc__))
    parser.add_argument('inputs', nargs='*',
                        help='the files/directories to count.')
    parser.add_argument('--documents', '-d', action='store_true',
                        help='Count the number of documents.')
    parser.add_argument('--paragraphs', '-p', action='store_true',
                        help='Count the number of paragraphs.')
    parser.add_argument('--words', '-w', action='store_true',
                        help='Count the number of words.')
    parser.add_argument('--characters', '-c', action='store_true',
                        help='Count the number of characters.')
    parser.add_argument('--warc', '-W', action='store_true',
                        help='Assume ALL input files are in the WARC format.')
    parser.add_argument('--latex', '-l', action='store_true',
                        help='LaTeX table row-style output.')
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
    # Without fields specified => all fields
    if not any([args.documents, args.paragraphs, args.words, args.characters]):
        args.documents = args.paragraphs = args.words = args.characters = True
    if args.warc and not (args.documents or args.characters):
        parser.error('Can only count documents and characters in WARC format.')
    return args


def count_file(filename, docs, ps, words, chars):
    """
    Counts the file denoted by filename. docs, ps, words and chars are bools
    telling the code whether to count the respective units.
    """
    # We need the content if we are counting anything aside from docs
    logging.debug('Counting {}...'.format(filename))
    need_content = ps or words or chars
    num_docs = num_ps = num_words = num_chars = 0
    try:
        for doc in parse_file(filename, False, False, need_content):
            num_docs += 1
            if ps:
                num_ps += doc.wc(p=True)
            if words:
                num_words += doc.wc(w=True)
            if chars:
                num_chars += doc.wc(c=True)
    except:
        logging.exception('Error in file {}; read {} documents thus far.'.format(
            filename, num_docs))
    logging.debug('Counted {}.'.format(filename))
    return num_docs, num_ps, num_words, num_chars


def count_warc_file(filename, docs, ps, words, chars):
    """Same as count_file, but for WARC files."""
    # We need the content if we are counting anything aside from docs
    logging.debug('Counting {}...'.format(filename))
    num_docs = num_chars = 0
    try:
        for doc in warc.open(filename):
            num_docs += 1
            if chars:
                num_chars += doc.header.content_length
    except:
        logging.exception('Error in file {}; read {} documents thus far.'.format(
            filename, num_docs))
    logging.debug('Counted {}.'.format(filename))
    return num_docs, 0, 0, num_chars


def tqdm_info(inputs):
    return ', '.join(inputs[:3]) + ('...' if len(inputs) >= 3 else '')


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)

    files = collect_inputs(args.inputs)
    count_fn = count_file if not args.warc else count_warc_file
    logging.info('Scheduled {} files for counting...'.format(len(files)))
    with Pool(args.processes) as p:
        f = partial(count_fn, docs=args.documents, ps=args.paragraphs,
                    words=args.words, chars=args.characters)
        stats = [0, 0, 0, 0]
        tqdm_msg = f'Counting {tqdm_info(args.inputs)}'
        for sub_stats in p.imap_unordered(f, otqdm(files, tqdm_msg)):
            for i in range(len(stats)):
                stats[i] += sub_stats[i]

        fields = [args.documents, args.paragraphs, args.words, args.characters]
        if args.latex:
            print(' & ' + ' & '.join('{:,d}'.format(stat) if field else ''
                                     for stat, field in zip(stats, fields)) +
                  r' \\')
        else:
            print(' '.join(str(stat) for stat, field in zip(stats, fields)
                           if field))
        p.close()
        p.join()
    logging.info('Done.')


if __name__ == '__main__':
    main()
