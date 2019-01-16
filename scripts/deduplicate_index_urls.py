#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Deduplicates the urls in the index."""

from argparse import ArgumentParser
from collections import defaultdict
import concurrent.futures as cf
from functools import partial
import gzip
import logging
from multiprocessing import Pool
from multiprocessing_logging import install_mp_handler
import os
import os.path as op
import re

file_name_p = re.compile('(\d{4}-\d{2}-\d+).gz$')


class Record():
    def __init__(self, warc, offset, length, index):
        self.warc = warc
        self.offset = int(offset)
        self.length = int(length)
        self.index = index

    def __repr__(self):
        return '({}, {}, {} in {})'.format(
            self.warc, self.offset, self.length, self.index)


def parse_arguments():
    parser = ArgumentParser('Deduplicates the urls in the index.')
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the index directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--keep', '-k', choices=['latest', 'biggest'],
                        default='biggest',
                        help='which occurrence to keep. Default: biggest.')
    parser.add_argument('--processes', '-p', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def uniq_record(url, record, uniqs, keep):
    """
    Uniq's a record. Returns whether the record is uniq (not in uniqs), or is the
    representative of its URL (i.e. it is the latest / biggest).
    """
    if url in uniqs:
        other_record = uniqs[url]
        if keep == 'latest':
            if record.warc < other_record.warc:
                return False
        else:
            if record.length <= other_record.length:
                return False

    uniqs[url] = record
    return True


def file_to_dict(file_name, keep):
    logging.info('Collecting URLs from {}...'.format(file_name))
    try:
        with gzip.open(file_name, 'rt') as inf:
            uniqs = {}
            file_id = file_name_p.search(file_name).group(1)
            for line_no, line in enumerate(map(str.strip, inf), start=1):
                try:
                    # After filtering, the line is prepended with the "domain"
                    # I skip that and extract it myself
                    url, warc, offset, length = line.split()[:7][-6:-2]
                    record = Record(warc, offset, length, file_id)
                    uniq_record(url, record, uniqs, keep)
                except:
                    logging.exception(
                        'Exception in file {}:{}'.format(file_name, line_no))
                    break
            logging.info('Deduplicated {} URLs in {} to {}.'.format(
                line_no, file_name, len(uniqs)))
            return uniqs
    except:
        logging.exception(
            'Exception in file {}'.format(file_name))
        return {}


def filter_file(input_file, output_file, uniqs):
    if not uniqs:
        return
    logging.info('Filtering file {}...'.format(input_file))
    with gzip.open(input_file, 'rt') as inf, gzip.open(output_file, 'wt') as outf:
        lines_printed = 0
        for line_no, line in enumerate(map(str.strip, inf), start=1):
            try:
                url, warc, offset, length = line.split()[:7][-6:-2]
                record = uniqs.get(url)
                if (
                    record and record.warc == warc and
                    record.offset == int(offset) and record.length == int(length)
                ):
                    print(line, file=outf)
                    lines_printed += 1
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
    install_mp_handler()

    # Collect the representative records for all URLs
    files = os.listdir(args.input_dir)
    to_process = [op.join(args.input_dir, f) for f in files]
    with Pool(args.processes) as pool:
    # with cf.ProcessPoolExecutor(max_workers=args.processes) as executor:
        aggr_uniqs = {}
        fn = partial(file_to_dict, keep=args.keep)
        for uniqs in pool.imap(fn, to_process):
            for url, record in uniqs.items():
                uniq_record(url, record, aggr_uniqs, args.keep)
            logging.info('Aggregated result: {} URLs.'.format(len(aggr_uniqs)))
        logging.info('Final tally: {} URLs.'.format(len(aggr_uniqs)))

    # Sort them by file (so that the whole dict need not be sent to every process)
    uniqs_by_file = defaultdict(dict)
    for url, record in aggr_uniqs.items():
        uniqs_by_file[record.index][url] = record
    del aggr_uniqs

    # And filter the files with these per-file dictionaries
    if not os.path.isdir(args.output_dir):
        os.mkdir(args.output_dir)
    tasks = zip([op.join(args.input_dir, f) for f in files],
                [op.join(args.output_dir, f) for f in files],
                [uniqs_by_file.pop(file_name_p.search(f).groups(1), {}) for f in files])
    with cf.ProcessPoolExecutor(max_workers=args.processes) as executor:
        cf.wait([executor.submit(filter_file, *task) for task in tasks])


if __name__ == '__main__':
    main()
