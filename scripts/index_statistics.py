#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from collections import Counter
import concurrent.futures as cf
import gzip
import os
import os.path as op
import tldextract
from urllib.parse import urljoin


def parse_arguments():
    parser = ArgumentParser('Collects statistics of the index.')
    parser.add_argument('--input-dir', '-i', required=True,
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
        self.segments = set()
        self.lengths = 0
        self.statuses = Counter()
        self.mimes = Counter()

    def __iadd__(self, other):
        self.urls.update(other.urls)
        self.domains.update(other.domains)
        self.segments |= other.segments
        self.lengths += other.lengths
        self.statuses.update(other.statuses)
        self.mimes.update(other.mimes)
        return self


def one_file_stats(file_name):
    stats = Stats()
    with gzip.open(file_name, 'rt') as inf:
        for line in map(str.strip, inf):
            # After filtering, the line is prepended with the "domain"
            # I skip that and extract it myself
            url, segment, _, length, status, mime = line.split()[:7][-6:]
            er = tldextract.extract(url)

            stats.urls[url] += 1
            stats.domains[er.domain + '.' + er.suffix] += 1
            stats.segments.add(segment)
            stats.lengths += int(length)
            stats.statuses[status] += 1
            stats.mimes[mime] += 1
        return stats


def dict_to_file(d, out_file, percent=False):
    if percent:
        all_values = sum(d.values()) / 100

    with open(out_file, 'wt') as outf:
        for key, value in sorted(d.items(), key=lambda kv: (-kv[1], kv[0])):
            print('{}\t{}'.format(key, value), end='', file=outf)
            if percent:
                print('\t{:.3f}'.format(value / all_values), end='', file=outf)
            print(file=outf)


def write_statistics(stats, output_dir, file_prefix=None):
    """Writes the collected statistics to file."""
    dict_to_file(stats.urls, op.join(output_dir, 'urls.tsv'))
    dict_to_file(stats.domains, op.join(output_dir, 'domains.tsv'), True)
    dict_to_file(stats.statuses, op.join(output_dir, 'statuses.tsv'), True)
    dict_to_file(stats.mimes, op.join(output_dir, 'mimes.tsv'), True)
    with open(op.join(output_dir, 'stats.tsv'), 'wt') as outf:
        print('{}\t{}'.format('num_docs', sum(stats.domains.values())), file=outf)
        print('{}\t{}'.format('sum_length', stats.lengths), file=outf)
        print('{}\t{}'.format(
            'avg_length', stats.lengths / sum(stats.domains.values())), file=outf)
        print('{}\t{}'.format('num_segments', len(stats.segments)), file=outf)
        print('{}\t{}'.format('avg_docs_per_segment',
                              sum(stats.domains.values()) / len(stats.segments)),
              file=outf)


def main():
    args = parse_arguments()
    os.nice(20)  # Play nice

    to_process = [op.join(args.input_dir, f) for f in os.listdir(args.input_dir)]
    with cf.ProcessPoolExecutor(max_workers=args.processes) as executor:
        aggr_stats = Stats()
        for stats in executor.map(one_file_stats, to_process):
            aggr_stats += stats

    if not os.path.isdir(args.output_dir):
        os.mkdir(args.output_dir)
    write_statistics(aggr_stats, args.output_dir)


if __name__ == '__main__':
    main()
