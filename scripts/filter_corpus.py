#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Filters documents in a corpus. Currently two filters are supported:
    - a language filter that discards documents not in one of the accepted
      languages
    - length filter that discards too short documents
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Pool
import os
import re

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser('Filters documents in a corpus. Currently two '
                            'filters are supported:'
                            '- a language filter that discards documents not '
                            'in one of the accepted languages'
                            '- length filter that discards too short documents')
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the corpus directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--language', '-l', action='append', dest='languages',
                        help='activates language filtering and marks a '
                             'language to keep. Should be specified once '
                             'per language.')
    parser.add_argument('--min-len', '-m', type=str,
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
    if not re.match(r'^\d+[w|c]$', args.min_len):
        parser.error('Invalid value for the minimum length parameter.')
    if not args.languages and not args.min_len:
        parser.error('At least one filter must be specified.')
    if args.languages:
        try:
            import cld2  # noqa
        except:
            parser.error('cld2 library not available.')
    return args


def filter_languages(doc_iter, languages):
    import cld2

    doc_no, kept = 0, 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        _, _, lang = cld2.detect(doc.content())
        if lang[0].language_code in languages:
            yield doc
            kept += 1
    if doc_no:
        logging.info('Filtered {} documents based on language, kept {}.'.format(
            doc_no, kept))


def filter_length(doc_iter, min_len_str):
    min_len = int(min_len_str[:-1])
    arg = {min_len_str[-1]: True}

    doc_no, kept = 0, 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        if doc.wc(**arg) >= min_len:
            kept += 1
            yield doc
    if doc_no:
        logging.info('Filtered {} documents based on length, kept {}.'.format(
            doc_no, kept))


def process_file(filename, input_dir, output_dir, languages, min_len_str):
    input_file = os.path.join(input_dir, filename)
    output_file = os.path.join(output_dir, filename)
    logging.info('Processing file {}...'.format(filename))

    it = parse_file(input_file, True, True, True)
    if languages:
        it = filter_languages(it, languages)
    if min_len_str:
        it = filter_length(it, min_len_str)
    try:
        with openall(output_file, 'wt') as outf:
            for doc in it:
                print(doc, file=outf)
    except:
        logging.exception('Got an error.')
    logging.info('Finished processing file {}...'.format(filename))


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
    p = Pool(args.processes)
    f = partial(process_file, input_dir=args.input_dir,
                output_dir=args.output_dir, languages=set(args.languages),
                min_len_str=args.min_len)
    p.map(f, files)
    p.close()
    p.join()
    logging.info('Done.')


if __name__ == '__main__':
    main()
