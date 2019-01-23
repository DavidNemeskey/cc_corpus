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
import os.path as op

from multiprocessing_logging import install_mp_handler
import warc

from cc_corpus.corpus import parse_file


def parse_arguments():
    parser = ArgumentParser('Counts the number of documents, paragraphs, '
                            'words and / or characters in (a set of) '
                            'files in WARC or the corpus format.\n\nSimilarly '
                            'to Unix wc, any number of these switches can be '
                            'specified, and then only those numbers are '
                            'printed; if no flags are specified, all numbers '
                            'are printed.\n\nFor WARC, number of paragraphs '
                            'and words will always be 0.')
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


def collect_inputs(inputs):
    """
    Collects all files to be counted from the files and directories specified.
    """
    files = []
    for input in inputs:
        if op.isfile(input):
            files.append(input)
        elif op.isdir(input):
            files.extend([op.join(input, f) for f in os.listdir(input)])
        else:
            raise ValueError('{} is neither a file nor a directory'.format(input))
    return files


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
        for sub_stats in p.map(f, files):
            for i in range(len(stats)):
                stats[i] += sub_stats[i]
        print(' '.join(str(stat) for stat, attr in
                       zip(stats, [args.documents, args.paragraphs,
                                   args.words, args.characters])
                       if attr))
        p.close()
        p.join()
    logging.info('Done.')


if __name__ == '__main__':
    main()
