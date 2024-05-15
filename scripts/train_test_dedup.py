#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A script that can do two things: collect the n-grams from a
(tiny, because of the naive implementation!) test corpus and
finds all documents in the training corpus that contain any
of them.
"""

from argparse import ArgumentParser
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, InitVar
from functools import partial
from itertools import chain
import logging
from multiprocessing import Pool
import os
from pathlib import Path
from typing import Any, ClassVar

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


collected_n_grams: Counter = None


def load_ngrams(ngrams_file: Path):
    global collected_n_grams
    with openall(ngrams_file, 'rt') as inf:
        collected_n_grams = {
            tuple(line.split()) for line in inf
        }


@dataclass
class DocFound:
    doc_id: str
    common_ratio: float
    common: set[tuple[str]]


def find_matches(
    input_file: Path, n: int, min_count: int
) -> list[DocFound]:
    docs = []
    for doc in parse_file(input_file):
        content = convert_document(doc)
        if len(content) >= n:
            n_grams = set(windowed(content, n))
            if len(common := n_grams & collected_n_grams) >= min_count:
                docs.append(DocFound(doc.id, len(common) / len(n_grams), common))
    return docs


@dataclass
class Runner:
    fn: Callable[[Path], list[Any]] = field(default=None, init=False, repr=False)
    n: int
    pool: Pool = field(default=None, init=False, repr=False)
    processes: InitVar[int]

    def __post_init__(self, processes: int):
        self.pool = Pool(processes)

    def run(self, input_dir: Path, output_file: Path):
        raise NotImplementedError()

    def input_it(self, input_dir: Path) -> Iterable[Any]:
        input_files = [f for f in input_dir.glob('**/*')
                       if not f.name.startswith('.') and f.is_file()]

        logging.info(f'Scheduled {len(input_files)} files for {self.task_np}.')

        it = otqdm(self.pool.imap_unordered(self.fn, input_files),
                   f'{self.task_ving} {input_dir.name}...',
                   total=len(input_files))
        return chain.from_iterable(it)


@dataclass
class Collector(Runner):
    task_np: ClassVar[str] = 'n-gram collection'
    task_ving: ClassVar[str] = 'Collecting from'

    def __post_init__(self, processes: int):
        super.__post_init__(processes)
        self.fn = partial(collect_ngrams, n=self.n)

    def run(self, input_dir: Path, output_file: Path):
        with self.pool:
            it = map(lambda t: ' '.join(t),
                     unique_everseen(self.input_it(input_dir)))
            with openall(output_file, 'wt') as outf:
                for item in it:
                    print(item, file=outf)
            self.pool.close()
            self.pool.join()


@dataclass
class Deduplicator(Runner):
    task_np: ClassVar[str] = 'deduplication'
    task_ving: ClassVar[str] = 'Deduplicating'

    ngrams_file: InitVar[Path]
    min_count: int

    def __post_init__(self, processes: int, ngrams_file: Path):
        self.fn = partial(find_matches, n=self.n, min_count=self.min_count)
        self.pool = Pool(processes,
                         initializer=load_ngrams,
                         initargs=[ngrams_file])

    def run(self, input_dir: Path, output_file: Path):
        collected_n_grams = Counter()
        with self.pool:
            it = self.input_it(input_dir)
            with openall(output_file, 'wt') as outf:
                for doc_found in it:
                    info = (
                        f'{doc_found.doc_id}\t{len(doc_found.common)}\t'
                        f'{doc_found.common_ratio}\t{next(iter(doc_found.common))}'
                    )
                    print(info, file=outf)
                    collected_n_grams.update(doc_found.common)

            self.pool.close()
            self.pool.join()

        with openall(
            output_file.stem + '_ngram_stats' + output_file.suffix, 'wt'
        ) as outf:
            for ngram, freq in collected_n_grams.most_common():
                print(f'{" ".join(ngram)}\t{freq}', file=outf)


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    os.nice(20)
    logging.info(args)

    if args.command == 'collect':
        runner = Collector(args.n, args.processes)
    elif args.command == 'deduplication':
        runner = Deduplicator(args.n, args.processes,
                              args.ngrams_file, args.min_count)

    runner.run(args.input_dir, args.output_file)

    logging.info('Done.')


if __name__ == '__main__':
    main()
