#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extracts document attributes. The extracted fields will be written into
a single file in the tsv format.
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Pool
import os
import sys

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.utils import collect_inputs, openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the corpus directory.')
    parser.add_argument('--output-file', '-o', default=None,
                        help='the output file; defaults to stdout.')
    parser.add_argument('--attr', '-a', action='append', default=[],
                        dest='attributes',
                        help='an attribute to extract. Can be specified more '
                             'than once. The default is the URL.')
    parser.add_argument('--write-headers', '-w', action='store_true',
                        help='write the tsv headers. Off by default.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()

    if not args.attributes:
        args.attributes.append('url')
    return args


def extract_attrs_fields(input_file, attrs):
    logging.info('Processing file {}...'.format(input_file))
    ret = []
    for doc in parse_file(input_file, meta=False, content=False):
        ret.append([doc.attrs.get(attr) for attr in attrs])
    logging.info('Finished processing file {}...'.format(input_file))
    return ret


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    files = sorted(collect_inputs([args.input_dir]))
    logging.info('Found a total of {} input files.'.format(len(files)))

    out = openall(args.output_file, 'wt') if args.output_file else sys.stdout

    attributes = list(map(str.lower, args.attributes))
    if args.write_headers:
        print('\t'.join(attributes), file=out)

    with Pool(args.processes) as pool:
        f = partial(extract_attrs_fields, attrs=attributes)
        for lines in pool.map(f, files):
            for attrs in lines:
                # print('\t'.join(attrs), file=out)
                print('\t'.join(map(str, attrs)), file=out)
        pool.close()
        pool.join()

    if out != sys.stdout:
        out.close()

    logging.info('Done.')


if __name__ == '__main__':
    main()
