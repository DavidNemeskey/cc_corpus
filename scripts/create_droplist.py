#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Takes a list of urls and transforms them using known transformations.
"""

from argparse import ArgumentParser
import logging
from pathlib import Path
from urllib import parse


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', type=Path, required=True,
                        help='the input file.')
    parser.add_argument('--output', '-o', type=Path, required=True,
                        help='the output file.')
    parser.add_argument('--transformation', '-t', type=str, required=True,
                        help='the transformation pattern to apply')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    if not args.input.is_file():
        parser.error('The input file must exist.')
    return args


def nepszava_transformation(input_url: str):
    parsed = parse.urlparse(input_url)
    article_title = parsed.query.split('=', 1)[1]
    new_parsed = ('http', parsed.netloc, article_title, '', '', '',)
    output_url = parse.urlunparse(new_parsed)
    return output_url


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    logging.info(f'Transforming input file {args.input} with pattern '
                 f'{args.transformation}')

    if args.transformation == 'nepszava':
        transf = nepszava_transformation
    else:
        raise ValueError(f'Unknown transformation pattern: '
                         f'{args.transformation}')

    with open(args.output, 'wt') as out_f, open(args.input, 'rt') as in_f:
        for line in in_f:
            print(transf(line), file=out_f)


if __name__ == '__main__':
    main()
