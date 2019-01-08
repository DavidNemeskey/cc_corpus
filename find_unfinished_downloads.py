#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Finds all finished files in a download log. This is useful when the
download process stops for some reason.
"""

from argparse import ArgumentParser
import os
import os.open as op
import re


def parse_arguments():
    parser = ArgumentParser('Finds all finished files in a download log. '
                            'This is useful when the download process stops '
                            'for some reason.')
    parser.add_argument('log_file', help='the log file.')
    args = parser.parse_args()
    if not op.isfile(args.log_file):
        parser.error('The log file must exist.')
    return args.log_file, args.finished


def find_files(log_file, finished):
    start_p = re.compile(r'Thread-(\d+)\s*\).*Starting batch (.+)$')
    end_p = re.compile(r'Thread-(\d+)\s*\).*Downloaded a total of (\d+) URLs')
    threads = {}
    with (gzip if log_file.endswith('.gz') else io).open(log_file, 'rt') as inf:
        for line in inf:
            start_m = start_p.search(line)
            if start_m: 
                threads[start_m.group(1)] = start_m.group(2)
            else:
                end_m = end_p.search(line)
                if end_m:
                    yield threads.pop(end_m.group(1))


def main():
    log_file, finished = parse_args()
    for fn in find_files(log_file, finished):
        print(fn)


if __name__ == '__main__':
    main()
