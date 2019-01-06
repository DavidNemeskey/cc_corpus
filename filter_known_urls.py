#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Filters known (already downloaded in a previous batch) URLs from the index."""

from argparse import ArgumentParser
import concurrent.futures as cf
import gzip
import io
import logging
import os


def parse_arguments():
    parser = ArgumentParser('Filters known (already downloaded in a previous '
                            'batch) URLs from the index.')
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the index directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--urls', '-u', required=True,
                        help='the file that lists known URLs.')
    parser.add_argument('--parallel', '-p', type=int, default=1,
                        help='number of worker threads to use (max is the '
                             'num of cores, default: 1)')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.parallel < 1 or args.parallel > num_procs:
        parser.error('Number of threads must be between 1 and {}'.format(
            num_procs))
    return args


def read_urls(urls_file):
    module = gzip if index_file.endswith('.gz') else io
    with module.open(urls_file, 'rt') as inf:
        return set(line.strip() for line in inf)


def filter_file(input_file, output_file, known_urls):
    logging.info('Filtering file {}...'.format(input_file))
    with gzip.open(input_file, 'rt') as inf, gzip.open(output_file, 'wt') as outf:
        lines_printed = 0
        for line_no, line in enumerate(map(str.strip, inf), start=1):
            try:
                url, warc, offset, length = line.split()[:7][-6:-2]
                if url not in known_urls:
                    print(line, file=outf)
            except:
                logging.exception(
                    'Exception in file {}:{}'.format(input_file, line_no))
        logging.info('Kept {} URLs out of {} in {}.'.format(
            lines_printed, line_no, input_file))


def main():
    args = parse_arguments()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    known_urls = read_urls(args.urls)
    logging.info('Read {} known URLs.'.format(len(known_urls)))

    files = os.listdir(args.input_dir)
    to_process = [op.join(args.input_dir, f) for f in files]
    with cf.ThreadPoolExecutor(max_workers=args.parallel) as executor:
        cf.wait([executor.submit(filter_file, op.join(args.input_dir, f),
                                 op.join(args.output_dir, f), known_urls)
                 for f in files])


if __name__ == '__main__':
    main()
