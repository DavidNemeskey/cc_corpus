#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import concurrent.futures as cf
import gzip
from itertools import chain
import os
import os.path as op


def parse_arguments():
    parser = ArgumentParser('Sorts segments & offsets to download.')
    parser.add_argument('input_dir', help='the index directory')
    parser.add_argument('--processes', '-p', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def sort_one_file(file_name):
    with gzip.open(file_name, 'rt') as inf:
        # After filtering, the line is prepended with the "domain"
        # I skip that and extract it myself; hence the [:7][-6:] part
        to_sort = [(segment, int(offset), int(length))
                   for segment, offset, length in
                   (line.split()[:7][-6:][1:4] for line in map(str.strip, inf))]
        to_sort.sort()
        return to_sort


def main():
    args = parse_arguments()
    to_process = [op.join(args.input_dir, f) for f in os.listdir(args.input_dir)]
    with cf.ProcessPoolExecutor(max_workers=args.processes) as executor:
        for segment, offset, length in sorted(chain(*executor.map(sort_one_file, to_process))):
            print('{}\t{}\t{}'.format(segment, offset, length))


if __name__ == '__main__':
    main()
