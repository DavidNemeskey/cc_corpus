#!/usr/bin/env python3
# -*- coding: utf-8, vim: expandtab:ts=4 -*-

"""
Sorts corpus files based on the THE FIRST URL in them. In other words, this
script can be used to re-sort the corpus by domain after the exact order has
been disrupted by concurrent processing by the last step of
frequent_paragraphs.py.

This script renames the files in the input directory.
"""

from argparse import ArgumentParser
import logging
from operator import itemgetter
import os
import re
import shutil

from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the input directory.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    return parser.parse_args()


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    os.nice(20)

    os.chdir(args.input_dir)
    input_files = sorted(os.listdir('.'))
    logging.info('Found a total of {} input files.'.format(len(input_files)))

    url_p = re.compile(r'^<doc .*? url="([^"]+)" .*>$')
    first_urls = []
    for f in input_files:
        with openall(f) as inf:
            header = inf.readline()
            url = url_p.match(header).group(1)
            first_urls.append((url, f))
    first_urls.sort()

    for old_doc, new_doc in zip(map(itemgetter(1), first_urls),
                                sorted(input_files)):
        shutil.move(old_doc, 'new_' + new_doc)

    for f in os.listdir('.'):
        shutil.move(f, f[4:])

    logging.info('Done.')


if __name__ == '__main__':
    main()
