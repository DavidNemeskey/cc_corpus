#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fixes various issues with in the final emtsv corpus. These include:
    - tokens that start with a hash mark (#) mess up the output and the
      analysis fields are missing from their lines
    - the wsafter field is missing
Note that most of the corrections here should actually be included
in emtsv.py and will most likely be in the future.
"""

from argparse import ArgumentParser
from collections import Counter
from functools import partial
import logging
from multiprocessing import Pool
import os
import re

from multiprocessing_logging import install_mp_handler

from cc_corpus.tsv import parse_file
from cc_corpus.utils import headtail, openall, notempty, otqdm


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the corpus directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
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


tokenp = re.compile(r'^#\S.*')
textp = re.compile('# text = (.*)')


def fix_invalid_lines(document, num_fields):
    """Fixes lines which only have a single field (assumed to be the form)."""
    num_fixed = 0
    for paragraph in document:
        for sentence in paragraph:
            for i, line in enumerate(sentence):
                token = line.split('\t')
                if len(token) != num_fields:
                    if len(token) != 1:
                        raise ValueError(f'Another error: only {len(token)} '
                                         f'in line {line}')
                    sentence[i] = '{0}\t[]\t{0}\t[/N][Nom]'.format(token[0])
                    num_fixed += 1
    return num_fixed


def add_wsafter(document):
    """Adds the wsafter field."""
    for paragraph in document:
        sentences = len(paragraph.content)
        for s_id, sentence in enumerate(paragraph, 1):
            raw, raw_idx = textp.fullmatch(sentence.comment).group(1), 0
            lines = len(sentence)
            new_content = []
            for l_id, line in enumerate(sentence, 1):
                token = line.split('\t')
                raw_idx += len(token[0])
                if l_id == lines:
                    wsafter = r'\n\n' if s_id == sentences else r'\n'
                elif raw[raw_idx] == ' ':
                    raw_idx += 1
                    wsafter = ' '
                else:
                    wsafter = ''
                token.insert(1, f'"{wsafter}"')
                new_content.append('\t'.join(token))
            sentence.content = new_content


def process_file(filename, input_dir, output_dir):
    input_file = os.path.join(input_dir, filename)
    output_file = os.path.join(output_dir, filename)
    logging.info('Processing file {}...'.format(filename))

    stats = Counter()

    with notempty(openall(output_file, 'wt')) as outf:
        header, it = headtail(parse_file(input_file, True))
        num_fields = len(header)
        do_wsafter = 'wsafter' not in header
        if do_wsafter:
            header.insert(1, 'wsafter')
            logging.debug('Adding the wsafter field...')
        print('\t'.join(header), file=outf)
        for document in it:
            stats['documents'] += 1
            try:
                stats['token_errors'] += fix_invalid_lines(document, num_fields)
                if do_wsafter:
                    add_wsafter(document)
            except ValueError:
                logging.exception(f'Error in file {input_file}')
                raise
            print(document, file=outf)
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
    logging.info('Scheduled {} files for correction.'.format(len(files)))

    with Pool(args.processes) as pool:
        fn = partial(process_file, input_dir=args.input_dir,
                     output_dir=args.output_dir)
        stats = Counter()
        for sub_stats in otqdm(pool.imap_unordered(fn, files), total=len(files)):
            stats.update(sub_stats)
        logging.info('Statistics: {}'.format(stats))
        pool.close()
        pool.join()

    logging.info('Done.')


if __name__ == '__main__':
    main()
