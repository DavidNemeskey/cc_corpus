#!/usr/bin/env python3
# -*- coding: utf-8, vim: expandtab:ts=4 -*-

"""
Sorts corpus or emtsv-analyzed tsv files based on the THE FIRST URL in them.
In other words, this script can be used to re-sort the corpus by domain after
the exact order has been disrupted by concurrent processing by the last step of
frequent_paragraphs.py.

This script renames the files in the input directory.
"""

from argparse import ArgumentParser
import logging
from operator import itemgetter
import os
import re
import shutil
from urllib.parse import urlsplit

from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the input directory.')
    parser.add_argument('--type', '-t', default='corpus',
                        choices=['corpus', 'tsv'],
                        help='the file type to sort.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    return parser.parse_args()


def urlkey(url_f):
    """
    The key used to sort the url--file list. The sorting only depends on the
    domain and the part after the ``/``, in this order.
    """
    url, f = url_f
    t = urlsplit(url)
    return t.netloc, t.path


class UrlFinder:
    """Finds the first URL in a file, based on a regex search."""
    def __init__(self, pattern):
        self.p = re.compile(pattern)

    def get_url(self, f):
        with openall(f) as inf:
            for line in inf:
                m = self.p.match(line)
                if m:
                    return m.group(1)


class CorpusUrlFinder(UrlFinder):
    """Finds the first URL in corpus files."""
    def __init__(self):
        super().__init__(r'^<doc .*? url="([^"]+)" .*>$')


class TsvUrlFinder(UrlFinder):
    """Finds the first URL in tsv files."""
    def __init__(self):
        super().__init__(r'# newdoc id = (.+)$')


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

    url_finder = CorpusUrlFinder() if args.type == 'corpus' else TsvUrlFinder()

    first_urls = []
    for f in input_files:
        first_urls.append((url_finder.get_url(f), f))
    first_urls.sort(key=urlkey)

    for old_doc, new_doc in zip(map(itemgetter(1), first_urls),
                                sorted(input_files)):
        shutil.move(old_doc, 'new_' + new_doc)

    for f in os.listdir('.'):
        shutil.move(f, f[4:])

    logging.info('Done.')


if __name__ == '__main__':
    main()
