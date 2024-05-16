#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Renumbers corpus files in one or several input directories. This script can be
used to even out the number of documents in the files.
"""

from argparse import ArgumentParser
from contextlib import closing
import logging
import os
from pathlib import Path
import sys

from cc_corpus.corpus import BatchWriter, is_it_jsonl, parse_file
from cc_corpus.utils import collect_inputs, otqdm


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    # This argument should be called input-dirs, but we kept it in singular
    # to keep the pattern used by every other script and we don't have to
    # handle this as a separate case in the manager webapp:
    parser.add_argument('--input-dir', '-i', type=Path, action='append',
                        help='the corpus directory')
    # parser.add_argument('input_dirs', nargs='+',
    #                     help='the input directories.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the output directory.')
    size_group = parser.add_mutually_exclusive_group(required=True)
    size_group.add_argument('--documents', '-d', type=int,
                            help='the number of documents a file should contain.')
    size_group.add_argument('--keep-sizes', '-k', action='store_true',
                            help='do not merge or split files; i.e. only '
                                 'copies files to the output directory.')
    parser.add_argument('--prefix', '-p',
                        help='the file name prefix that comes before the '
                             'digits; optional.')
    parser.add_argument('--digits', '-Z', type=int, default=4,
                        help='the number of digits in the output files\' names.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    if not args.input_dir:
        parser.error('At least one input must be supplied.')
    return args


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    os.nice(20)

    input_files = collect_inputs(args.input_dir)
    logging.info('Scheduled {} files for renumbering.'.format(len(input_files)))

    batch_size = args.documents if not args.keep_sizes else sys.maxsize
    num_docs = 0
    name_prefix = f'{args.prefix}_' if args.prefix else ''
    with closing(
        BatchWriter(batch_size, args.output_dir, args.digits, name_prefix)
    ) as bw:
        for input_file in otqdm(input_files, 'Renumbering files...'):
            if not args.keep_sizes:
                logging.debug('Reading file {}...'.format(input_file))
                for document in parse_file(input_file):
                    bw.write(document, jsonl=is_it_jsonl(input_file))
                    num_docs += 1
            else:
                logging.debug('Copying file {}...'.format(input_file))
                bw.copy_file(input_file)

    if not args.keep_sizes:
        logging.info(
            'Done. Renumbered {} files to {}, {} documents each; '
            '{} documents in total.'.format(
                len(input_files), args.output_dir, args.documents, num_docs)
        )
    else:
        logging.info('Done. Renumbered {} files to {}.'.format(
                     len(input_files), args.output_dir))


if __name__ == '__main__':
    main()
