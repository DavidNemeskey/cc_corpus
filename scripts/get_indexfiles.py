#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Python version of the old get_indexfiles.sh script. Downloads selected
collection(s) from Common Crawl.
"""

from argparse import ArgumentParser
from collections import defaultdict
import os
import re
import sys
import subprocess
import time
from typing import Dict, List


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--query', '-q', required=True,
                        help='the query string; e.g. "*.hu".')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--log-file', '-l', required=True,
                        help='the (base) name of the (rotating) log file.')
    parser.add_argument('--max-retry', '-m', type=int, default=5,
                        help='maximum number of attempts to redownload a '
                             'specific page.')
    parser.add_argument('--collection', '-c', default='all',
                        help='the collection to download. The default is "all".')
    return parser.parse_args()


def download_index(query: str, output_dir: str, params: str, log_file: str,
                   mode: str = 'w'):
    """Calls the script :program:`cdx-index-client.py` to do the actual work."""
    with open(log_file, '{}t'.format(mode)) as logf:
        res = subprocess.run(
            'cdx-index-client.py --fl url,filename,offset,length,status,mime '
            '-z {} -d {} {}'.format(query, output_dir, params),
            stdout=logf, stderr=subprocess.STDOUT, shell=True, check=True
        )
        return res.returncode


def get_uncompleted(log_file: str) -> Dict[str, List[str]]:
    """Returns the list of """
    mrp = re.compile(r'^Max retries .* page (?P<page>[0-9any]+) '
                     r'for crawl (?P<coll>[A-Z0-9]+)-index$')
    with open(log_file, 'rt') as inf:
        colls = defaultdict(list)
        for line in map(str.strip, inf):
            m = mrp.match(line)
            if m:
                colls[m.group('coll')].append(m.group('page'))
    return colls


def main():
    args = parse_arguments()
    i = 0
    log_file = args.log_file + '.{}'.format(i)
    rcode = download_index(args.query, args.output_dir,
                           '-c {}'.format(args.collection), log_file)
    while rcode == 0 and i <= args.max_retry:
        uncompleted = get_uncompleted(log_file)
        if not uncompleted:
            break
        i += 1
        log_file = args.log_file + '.{}'.format(i)
        print('Doing {}th round of full retry'.format(i), file=sys.stderr)
        if os.path.exists(log_file):
            os.remove(log_file)
        for coll, pages in uncompleted:
            time.sleep(30)  # Sleep to prevent DDoS
            download_index(args.query, args.output_dir,
                           '-c {} --pages {}'.format(coll, ' '.join(pages)),
                           log_file, 'a')

    if i <= args.max_retry:
        print('Successfully finished after {} iterations!'.format(i + 1),
              file=sys.stderr)
    else:
        print('Finished after {} of {} iterations please check if everything '
              'is all right!'.format(i + 1, args.max_retry + 1), file=sys.stderr)


if __name__ == '__main__':
    main()
