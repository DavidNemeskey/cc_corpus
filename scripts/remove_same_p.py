#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Removes *exact* duplicate paragraphs that occur in *the same document*. The
emphasized words is what differentiates this script from frequent_paragraphs.py.
And the speed, of course, as this one is much faster to run...
"""

from argparse import ArgumentParser
from collections import defaultdict, Counter
from functools import partial
import logging
from multiprocessing import Pool
import os
import os.path as op
from typing import Any, Dict
from urllib.parse import urlsplit

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file
from cc_corpus.utils import collect_inputs, openall, Stats


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the corpus directory')
    parser.add_argument('--min-length', '-l', type=int, default=1,
                        help='the minimum paragraph length in characters.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    subparsers = parser.add_subparsers(
        help='This script can collect statistics of duplicate paragraphs or '
             'remove them.'
    )
    parser_stats = subparsers.add_parser(
        'statistics', aliases=['stats'],
        help='Collects per-domain statistics of duplicate paragraphs.'
    )
    parser_stats.set_defaults(command='statistics')
    parser_remove = subparsers.add_parser(
        'remove', help='Removes duplicate paragraphs from documents.'
    )
    parser_remove.set_defaults(command='remove')
    parser_remove.add_argument('--output-dir', '-o', required=True,
                               help='the output directory.')
    args = parser.parse_args()

    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


CollectStats = Stats.create('docs', 'ps', 'affected_docs',
                            'affected_ps', 'ps_copies')  # type: Any


def collect_stats(input_file: str, min_length: int) -> Dict[str, CollectStats]:
    """Collects statistics about the prevalence of the phenomenon in domains."""
    stats = {}
    for doc in parse_file(input_file, True, False, True):
        domain = urlsplit(doc.attrs['url']).netloc
        stat = stats.setdefault(domain, CollectStats())
        stat.docs += 1
        stat.ps += len(doc.paragraphs)

        c = Counter([p for p in doc.paragraphs if len(p) >= min_length])
        doc_affected = False
        for p, freq in c.most_common():
            if freq == 1:
                break
            logging.debug('{}: {} x {}'.format(doc.attrs['url'], freq, p))
            doc_affected = True
            stat.affected_ps += 1
            stat.ps_copies += freq - 1
        if doc_affected:
            stat.affected_docs += 1

    return stats


def remove_same_ps(input_file: str, min_length: int,
                   output_dir: str) -> Dict[str, CollectStats]:
    """Removes duplicate paragraphs from documents."""
    stats = {}
    with openall(op.join(output_dir, op.basename(input_file)), 'wt') as outf:
        for doc in parse_file(input_file, True, False, True):
            domain = urlsplit(doc.attrs['url']).netloc
            stat = stats.setdefault(domain, CollectStats())
            stat.docs += 1
            stat.ps += len(doc.paragraphs)

            seen_ps, kept_ps = set(), []
            for p in doc.paragraphs:
                if p not in seen_ps:
                    seen_ps.add(p)
                    kept_ps.append(p)
                else:
                    stat.ps_copies += 1

            if len(doc.paragraphs) != len(kept_ps):
                stat.affected_docs += 1
                doc.paragraphs = kept_ps
            print(doc, file=outf)

    return stats


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    input_files = collect_inputs([args.input_dir])
    logging.info('Scheduled {} files for processing.'.format(len(input_files)))

    if args.command == 'statistics':
        f = partial(collect_stats, min_length=args.min_length)
    else:
        f = partial(remove_same_ps, min_length=args.min_length,
                    output_dir=args.output_dir)
        if not op.isdir(args.output_dir):
            os.makedirs(args.output_dir)

    with Pool(args.processes) as pool:
        sum_stats = defaultdict(CollectStats)
        for stats in pool.imap_unordered(f, input_files):
            for domain, stat in stats.items():
                sum_stats[domain] += stat
        pool.close()
        pool.join()

    if args.command == 'statistics':
        for domain, stat in sorted(sum_stats.items()):
            if stat.affected_docs > 0:
                print('{}\t{}\t{}\t{}\t{}\t{}'.format(
                    domain, stat.docs, stat.ps, stat.affected_docs,
                    stat.affected_ps, stat.ps_copies))
    else:
        sum_stat = CollectStats()
        for stat in sum_stats.values():
            sum_stat += stat
        logging.info('Filtered {} paragraphs from {} affected documents '
                     '(out of {} in {}).'.format(
            sum_stat.ps_copies, sum_stat.affected_docs,
            sum_stat.ps, sum_stat.docs))

    logging.info('Done.')


if __name__ == '__main__':
    main()
