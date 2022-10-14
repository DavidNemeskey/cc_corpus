#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extracts the raw HTLMs from WARC files to one file per HTML. Also generates a
JSON file that contains the mapping between HTML pages and URLs.

Warning: do not use this on an unfiltered download directory, because the number
of HTML files generated might make the file system unusable; also, the HTML-URL
mapping will grow big and cause memory issues.
"""

from argparse import ArgumentParser
from collections import Counter
from functools import partial
import json
import logging
from multiprocessing import Pool
import os
import os.path as op
import sys

from multiprocessing_logging import install_mp_handler
import warc

from cc_corpus.mime import normalize_content
from cc_corpus.utils import openall, otqdm


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the download (WARC) directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--max-pages', '-m', type=int, default=sys.maxsize,
                        help='the maximum number of pages to extract from a '
                             'single WARC file.')
    parser.add_argument('--zeros', '-Z', type=int, default=3,
                        help='the minimum number of zeros (digits) in the '
                             'output files\' names.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()

    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def extract_pages(warc_file: str, input_dir: str,
                  output_dir: str, max_pages: int = sys.maxsize,
                  zeros: int = 3):
    """
    :return: the file name -> URL mapping.
    """
    input_file = op.join(input_dir, warc_file)
    logging.info('Processing file {}...'.format(warc_file))
    padding = f'{{0:0{zeros}}}'
    mapping = {}

    warc_id = 0
    with warc.open(input_file) as inf:
        written = 0
        for warc_id, warc_record in enumerate(inf, 1):
            _, content = warc_record.payload.read().split(b'\r\n\r\n', maxsplit=1)
            content = normalize_content(content)
            if content is not None:
                written += 1
                content = content.encode('utf-8')
                output_file = op.join(
                    output_dir,
                    f'{op.splitext(warc_file)[0]}_{padding.format(warc_id)}.html'
                )
                with openall(output_file, 'wb') as outf:
                    outf.write(content)
                    mapping[op.basename(output_file)] = warc_record['WARC-Target-URI']
                if written == max_pages:
                    break
    stats = {'documents': warc_id, 'written': written}
    logging.info(f'Processed {warc_file}...')
    return mapping, stats


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not op.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    warc_files = os.listdir(args.input_dir)
    logging.info('Scheduled {} files for filtering.'.format(len(warc_files)))

    fn = partial(extract_pages, input_dir=args.input_dir,
                 output_dir=args.output_dir, max_pages=args.max_pages,
                 zeros=args.zeros)
    with Pool(args.processes) as pool:
        all_mapping = {}
        all_stats = Counter()
        for mapping, stats in otqdm(pool.imap_unordered(fn, warc_files),
                             'Extracting HTLMs...', total=len(warc_files)):
            all_mapping.update(mapping)
            all_stats.update(stats)
        pool.close()
        pool.join()
    with openall(op.join(args.output_dir, 'mapping.json.gz'), 'wt') as outf:
        json.dump(all_mapping, outf)
    logging.info(f'Stats: {dict(all_stats)}')

    logging.info('Done.')


if __name__ == '__main__':
    main()
