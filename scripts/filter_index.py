#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Python version of the old filter_index.sh script. Filters problematic entries
from the downloaded index. In particular,

1. Filter URL-s with robots.txt at the end
2. Keep entries with status 200 only
3. Normalize mime-type field
4. Prefix domain (sed)
5. Keep only the allowed mime-types (fgrep)
6. Filter entries permanently resulting in decompression errors (bad index?) (fgrep)
7. Sort by the domain
"""

from argparse import ArgumentParser
import os
import re

from cc_corpus.utils import openall


robotsp = re.compile(r'/robots\.txt')
mime1p = re.compile(r'')
mime2p = re.compile(r'[,;].*')


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('input_dir', help='the input directory.')
    parser.add_argument('output_dir', help='the output directory.')
    args = parser.parse_args()
    return args.input_dir, args.output_dir


def filter_file(input_file, output_file):
    with openall(input_file, 'rt') as inf, openall(output_file, 'wt') as outf:
        for url, warc, offset, length, status, mime_type, *mime_cont in (
                line.strip().split(' ') for line in inf
        ):
            if not robotsp.search(url) and int(status) == 200:



def main():
    input_dir, output_dir = parse_arguments()

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    for input_file in os.listdir(input_dir):
        filter_file(os.path.join(input_dir, input_file),
                    os.path.join(input_dir, input_file))


if __name__ == '__main__':
    main()
