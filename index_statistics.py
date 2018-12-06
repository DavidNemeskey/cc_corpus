#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from collections import Counter
import concurrent.futures as cf
import os
import os.path as op
import tldextract
from urllib.parse import urljoin


def parse_arguments():
    parser = ArgumentParser('Collects statistics of the index.')
    parser.add_argument('--index-dir', '-i', required=True,
                        help='the index directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--processes', '-p', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


class Stats:
    def __init__(self):
        self.urls = Counter()
        self.domains = Counter()
        self.lengths = 0
        self.statuses = Counter()
        self.mimes = Counter()

    def __iadd__(self, other):
        self.urls.update(other.urls)
        self.domains.update(other.domains)
        self.length += other.lengths
        self.statuses.update(other.statuses)
        self.mimes.update(other.mimes)


def one_file_stats(file_name):
    stats = Stats()
    with open(file_name) as inf:
        for line in map(str.strip, inf):
            url, _, _, length, status, mime = line.split()
            er = tldextract.extract(url)

            stats.urls[url] += 1
            stats.domains[er.domain + '.' + er.suffix] += 1
            stats.lengths += length
            stats.statuses[status] += 1
            stats.mimes[mime] += 1
    return stats


def main():
    args = parse_arguments()
    to_process = [op.join(args.input_dir, f) for f in os.listdir(args.input_dir)]
    with cf.ProcessPoolExecutor(max_workers=args.processes) as executor:
        aggr_stats = Stats()
        for stats in executor.map(one_file_stats, to_process):
            aggr_stats += stats
    # TODO: print


if __name__ == '__main__':
    main()
