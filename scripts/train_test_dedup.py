#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A script that can do two things: collect the n-grams from a
(tiny, because of the naive implementation!) test corpus and
finds all documents in the training corpus that contain any
of them.
"""

from argparse import ArgumentParser
from functools import partial
from itertools import chain
import logging
from multiprocessing import Pool
import os
from pathlib import Path

from more_itertools import unique_everseen, windowed
from cc_corpus.corpus import Document, parse_file
from cc_corpus.utils import ispunct, openall, otqdm


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory, parsed recursively.')
    parser.add_argument('--output-file', '-o', type=Path, required=True,
                        help='the output file. Depending on the mode, '
                             'either the n-gram file or the list of document '
                             'ids that match it.')
    parser.add_argument('--n', '-n', type=int, required=True,
                        help='the order of the n-grams.')
    subparsers = parser.add_subparsers(
        help='Execution steps.', required=True)
    parser_collect = subparsers.add_parser(
        'collect', help='Collect n-grams from the test corpus.'
    )
    parser_collect.set_defaults(command='collect')
    parser_dedup = subparsers.add_parser(
        'deduplicate', aliases=['dedup'],
        help='Find the documents in the training corpus that match '
             'any of the n-grams.'
    )
    parser_dedup.add_argument(
        '--min-count', '-m', type=int, required=True,
        help='the minimum number of matches needed to mark a document.')
    parser_dedup.add_argument(
        '--ngrams-file', '-f', type=Path, required=True,
        help='the n-grams file collected in the first step.')
    parser_dedup.set_defaults(command='deduplication')

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


def convert_document(doc: Document) -> list[str]:
    """Converts a document to a list of whitespace-separated tokens."""
    return ''.join(c for c in doc.content().lower() if not ispunct(c)).split()


def collect_ngrams(input_file: Path, n: int) -> set[tuple[str]]:
    """Collects all n-grams from _input_file_."""
    n_grams = set()
    for doc in parse_file(input_file):
        content = convert_document(doc)
        if len(content) >= n:
            n_grams.update(windowed(content, n))
    return n_grams


collected_n_grams = None


def load_ngrams(ngrams_file: Path):
    global collected_n_grams
    with openall(ngrams_file, 'rt') as inf:
        collected_n_grams = {
            tuple(line.split()) for line in inf
        }


def find_matches(input_file: Path, n: int, min_count: int) -> list[str]:
    docs = []
    for doc in parse_file(input_file):
        content = convert_document(doc)
        to_add = True
        if len(content) >= n:
            n_grams = set(windowed(content, n))
            if len(n_grams & collected_n_grams) >= min_count:
                to_add = False
                # docs.append(doc.id)
        if to_add:
            docs.append(doc.id)
    return docs


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    os.nice(20)
    logging.info(args)

    input_files = [f for f in args.input_dir.glob('**/*')
                   if not f.name.startswith('.') and f.is_file()]

    if args.command == 'collect':
        fn = partial(collect_ngrams, n=args.n)
        pool = Pool(args.processes)
        task_np = 'n-gram collection'
        task_ving = 'Collecting from'
    elif args.command == 'deduplication':
        fn = partial(find_matches, n=args.n, min_count=args.min_count)
        pool = Pool(args.processes,
                    initializer=load_ngrams,
                    initargs=[args.ngrams_file])
        task_np = 'deduplication'
        task_ving = 'Deduplicating'

    logging.info(f'Scheduled {len(input_files)} files for {task_np}.')

    with pool:
        it = otqdm(pool.imap_unordered(fn, input_files),
                   f'{task_ving} {args.input_dir.name}...',
                   total=len(input_files))
        it = chain.from_iterable(it)
        if args.command == 'collect':
            it = map(lambda t: ' '.join(t), unique_everseen(it))
        with openall(args.output_file, 'wt') as outf:
            for item in it:
                print(item, file=outf)
        pool.close()
        pool.join()

    logging.info('Done.')


if __name__ == '__main__':
    main()
