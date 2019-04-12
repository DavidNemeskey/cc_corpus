#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script to test the python role.
"""

from argparse import ArgumentParser


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--content', '-c', required=True,
                        help='the content of the output file.')
    parser.add_argument('--file', '-f', required=True,
                        help='the output file.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='the number of parallel processes.')
    return parser.parse_args()


def main():
    args = parse_arguments()
    with open(args.file, 'wt') as outf:
        outf.write(args.content)


if __name__ == '__main__':
    main()
