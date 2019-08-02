#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parses log files created by one of the scripts outputs the log messages therein
grouped by the files for which they were emitted. It is possible to filter
messages by logging level (so that e.g. only ERRORs and WARNINGs are kept).
"""

from argparse import ArgumentParser
from collections import defaultdict
import logging
import re
import sys
from typing import Generator, Tuple

from cc_corpus.utils import collect_inputs, openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories of log files to parse.')
    parser.add_argument('--start', '-s', default='Processing',
                        help='the string that comes before the file name that '
                             'signals the start of file processing.')
    parser.add_argument('--end', '-e', default='Finished',
                        help='the string that comes before the file name that '
                             'signals the end of file processing.')
    parser.add_argument('--log-level', '-L', type=str, default='warning',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the lowest logging level to include in the output.')
    return parser.parse_args()


startp, endp, logp = None, None, None


def create_patterns(start: str, end: str):
    """Creates the regular expression patterns used in parsing the log files."""
    global startp, endp, logp
    startp = re.compile(r'^(?P<datetime>[^ ]+ [^ ]+) - (?P<pid>\d+) - '
                        rf'(?P<level>[^ ]+) - {start} (?P<filename>.+?)...$')
    endp = re.compile(r'^(?P<datetime>[^ ]+ [^ ]+) - (?P<pid>\d+) - '
                      rf'(?P<level>[^ ]+) - {end} (?P<filename>.+?).$')
    logp = re.compile(r'^(?P<datetime>[^ ]+ [^ ]+) - (?P<pid>\d+) - '
                      rf'(?P<level>[^ ]+) - (?P<message>.+?)$')


def parse_file(log_file: str, min_level: int) -> Generator[Tuple[str, str], None, None]:
    """Filters log messages in *log_file*."""
    files = {}
    pids = defaultdict(list)
    with openall(log_file, 'rb') as inf:
        for raw_line in inf:
            try:
                line = raw_line.decode('utf-8')
            except UnicodeDecodeError as ude:
                # Most likely quntoken errors
                continue
            m = startp.match(line)
            if m:
                files[m.group('filename')] = m.group('pid')
                continue
            m = endp.match(line)
            if m:
                if m.group('pid') in pids:
                    yield m.group('filename'), pids[m.group('pid')]
                    del pids[m.group('pid')]
                del files[m.group('filename')]
                continue
            m = logp.match(line)
            if m and getattr(logging, m.group('level')) >= min_level:
                pids[m.group('pid')].append(line)
                continue
        # Files whose processing could not finish
        for file_name, pid in files.items():
            yield file_name, pids[pid]


def main():
    args = parse_arguments()
    min_level = getattr(logging, args.log_level.upper())
    create_patterns(args.start, args.end)

    input_files = sorted(collect_inputs(args.inputs))
    for input_file in input_files:
        try:
            for file_name, log_lines in parse_file(input_file, min_level):
                print(f'{file_name}:')
                for line in log_lines:
                    print(line)
                print()
        except:
            print(f'Error in file {input_file}!', file=sys.stderr)
            raise


if __name__ == '__main__':
    main()
