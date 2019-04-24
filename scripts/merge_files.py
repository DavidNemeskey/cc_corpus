#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script that can be used to merge the various file types created by the pipeline.
"""

from argparse import ArgumentParser
from contextlib import closing
import logging
import os


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('inputs', nargs='+', help='the input files (prefixes).')
    parser.add_argument('--type', '-t', required=True, choices=['pdata'],
                        help='type of the input and output files. See above.')
    parser.add_argument('--output', '-o', required=True,
                        help='the output file (prefix).')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')


def merge_pdata(output_prefix, *file_prefixes):
    """
    Merges "paragraph data" files output by frequent_paragraphs.py's collect
    mode. This includes two file types: the index file .pdi and the file with
    the actual paragraph data (.pdata).
    """
    # Merge the data files
    with closing(open('{}.pdata'.format(output_prefix), 'wb')) as dataf:
        for input_prefix in file_prefixes:
            with closing(open('{}.pdata'.format(input_prefix), 'rb')) as inf:
                while True:
                    bytes_read = inf.read(1024)
                    if not bytes_read:
                        break
                    bytes_written = 0
                    while bytes_written < len(bytes_read):
                        bytes_written += dataf.write(bytes_read[bytes_written:])

    # Merge the indices
    index = []
    initial_offset = 0
    for input_prefix in file_prefixes:
        max_offset = 0
        with closing(open('{}.pdi'.format(input_prefix), 'rt')) as inf:
            for line in inf:
                domain, offset, length, num = line.strip().split('\t')
                int_offset = int(offset)
                int_length = int(length)
                curr_offset = int_offset + int_length
                if curr_offset > max_offset:
                    max_offset = curr_offset
                index.append(
                    (domain, initial_offset + int_offset, int_length, num))
        initial_offset += max_offset

    index.sort()
    with closing(open('{}.pdi'.format(output_prefix), 'wt')) as indexf:
        for domain, offset, length, num in index:
            print('{}\t{}\t{}\t{}'.format(
                domain, offset, length, num), file=indexf)


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    os.nice(20)

    if args.type == 'pdata':
        fun = merge_pdata

    fun(args.output, *args.inputs)


if __name__ == '__main__':
    main()
