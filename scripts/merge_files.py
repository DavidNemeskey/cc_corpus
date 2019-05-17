#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script that can be used to merge the various file types created by the pipeline.
"""

from argparse import ArgumentParser
from contextlib import closing
import logging
import os
import sys

from cc_corpus.frequent import open as pdata_open


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('inputs', nargs='+', help='the input files (prefixes).')
    parser.add_argument('--type', '-t', required=True, choices=['pdata'],
                        help='type of the input and output files. See above.')
    parser.add_argument('--output', '-o', required=True,
                        help='the output file (prefix).')
    parser.add_argument('--merge-type', '-m', choices=['simple', 'iterator'],
                        default='simple',
                        help='the merge algorithm. Simple merge (the default) '
                             'simply concatenates the two files (and updates '
                             'index offsets, etc.). Iterator mode copies data '
                             'from the old files to the new on a per-record '
                             'basis. As such, it is much slower than simple '
                             'mode; however, it is useful when the index '
                             'does not describe the whole data, as it will '
                             'avoid copying excess records. When filtering '
                             '(--filter), the mode is automatically set to '
                             'iterator.')
    parser.add_argument('--filter', '-f', action='append', default=[],
                        dest='filters',
                        help='add a filter condition to the paragraphs read '
                             'from the input files. The filter is a Python '
                             'expression; however, it can only refer to the '
                             'following variables: domain, docs (in domain), '
                             'pdata (the paragraph object). Can be specified '
                             'more than once.')
    parser.add_argument('--head', type=int, default=0, metavar='N',
                        help='stop after the first N domains. For debugging, '
                             'mostly.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()

    if args.filters:
        args.merge_type = 'iterator'
    return args

class Filter:
    """Compiles the filters and applies them."""

    _allowed_builtins = {
        'abs': abs,
        'all': all,
        'any': any,
        'chr': chr,
        'divmod': divmod,
        'len': len,
        'max': max,
        'min': min,
        'pow': pow,
        'round': round,
        'sorted': sorted,
        'sum': sum,
        'bool': bool,
        'float': float,
        'int': int,
        'list': list,
        'map': map,
        'range': range,
        'str': str,
        'tuple': tuple,
        'type': type,
        'zip': zip,
    }
    _globals = {'__builtins__': _allowed_builtins}

    def __init__(self, *filters):
        if not filters:
            filters = ['True']
        self.code = compile('(' + ') and ('.join(filters) + ')',
                            '<string>', 'eval', optimize=2)

    def filter(self, **kwargs):
        return eval(self.code, Filter._globals, kwargs)


def merge_pdata_it(output_prefix, file_prefixes, **kwargs):
    """
    Merges "paragraph data" files output by frequent_paragraphs.py's collect
    mode. This includes two file types: the index file .pdi and the file with
    the actual paragraph data (.pdata).
    """
    cond = Filter(kwargs['filters'])
    head = kwargs['head'] or sys.maxsize
    num_written = 0
    with pdata_open(output_prefix, 'w') as outf:
        for input_prefix in file_prefixes:
            with pdata_open(input_prefix, 'r') as inf:
                for domain, docs, pdatas in inf:
                    pdatas = [cond.filter(domain=domain, docs=docs, pdata=pdata)
                              for pdata in pdatas]
                    if pdatas:
                        outf.write(domain, docs, *pdatas)
                        num_written += 1
                        if num_written == head:
                            return


def merge_pdata(output_prefix, file_prefixes, **kwargs):
    """
    Merges "paragraph data" files output by frequent_paragraphs.py's collect
    mode. This includes two file types: the index file .pdi and the file with
    the actual paragraph data (.pdata).

    :param kwargs: not used.
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
        for domain, offset, length, num, docs in index:
            print('{}\t{}\t{}\t{}\t{}'.format(
                domain, offset, length, num, docs), file=indexf)


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    os.nice(20)

    if args.type == 'pdata':
        fun = merge_pdata_it if args.merge_type == 'iterator' else merge_pdata

    fun(args.output, args.inputs, filters=args.filters, head=args.head})


if __name__ == '__main__':
    main()
