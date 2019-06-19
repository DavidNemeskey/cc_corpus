#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Removes *exact* duplicate paragraphs that occur in *the same document*. The
emphasized words is what differentiates this script from frequent_paragraphs.py.
And the speed, of course, as this one is much faster to run...
"""

from argparse import ArgumentParser
from collections import defaultdict, Counter
import logging
from multiprocessing import Pool
import os
from typing import Any, Dict
from urllib.parse import urlsplit

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.utils import Stats


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the corpus directory')
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


CollectStats = Stats.create('docs', 'ps', 'affected_docs',
                            'affected_ps', 'ps_copies')  # type: Any


def collect_stats(input_file: str) -> Dict[str, CollectStats]:
    """Collects statistics about the prevalence of the phenomenon in domains."""
    stats = {}
    for doc in parse_file(input_file, True, False, False):
        domain = urlsplit(doc.attrs['url']).netloc
        stat = stats.setdefault(domain, CollectStats())
        stat.docs += 1
        stat.ps += len(doc.paragraphs)

        c = Counter(doc.paragraphs)
        doc_affected = False
        for p, freq in c.most_common():
            if freq == 1:
                break
            doc_affected = True
            stat.affected_ps += 1
            stat.ps_copies += freq - 1
        if doc_affected:
            stat.affected_docs += 1

    return stats


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    input_files = os.listdir(args.input_dir)
    logging.info('Scheduled {} files for filtering.'.format(len(input_files)))

    with Pool(args.processes) as pool:
        sum_stats = defaultdict(CollectStats)
        for stats in pool.imap_unordered(input_files):
            for domain, stat in stats.items():
                sum_stats[domain] += stat
        pool.close()
        pool.join()

    for domain, stat in sum_stats.items():
        print('{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(
            domain, stat.docs, stat.ps, stat.affected_docs,
            stat.affected_ps, stat.ps_copies))

    logging.info('Done.')


if __name__ == '__main__':
    main()
