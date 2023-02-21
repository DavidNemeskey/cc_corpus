#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Does cross-deduplication for a series of batches. It selects which batch to do
next and cross-deduplicates it using the functions in lsh.py
"""

from argparse import ArgumentParser
import logging
from pathlib import Path
import re

from lsh import cumulative_directory_deduplication


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory that contains the self-'
                             'deduplicated subdirectories.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the directory that contains the fully-'
                             'deduplicated subdirectories.')
    parser.add_argument('--permutations', '-p', type=int, default=256,
                        help='the number of permutations per paragraph (256).')
    parser.add_argument('--threshold', '-t', type=float, default=0.9,
                        help='the Jaccard similarity threshold (0.9).')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1). Note that in order '
                             'to deduplicate documents, much memory might be '
                             'needed, so it is a good idea to be conservative '
                             'with the number of processes.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    if not args.output_dir.is_dir():
        parser.error('The directory for the batches must exist.')
    return args


def has_minhash_content(directory: Path) -> bool:
    """
    Tells whether a given directory contains all the following files or not:
    1.doc_ids, 1.files, 1.minhashes
    """
    return ((directory / '1.doc_ids').is_file()
            and (directory / '1.files').is_file()
            and (directory / '1.minhashes').is_file())


def collect_processed_batches(finished_batches_dir: Path) -> list[Path]:
    """
    Collects the directories within the finished_batches_dir
    which have date-like names and contain the following files:
    1.doc_ids, 1.files, 1.minhashes
    """
    collected_dirs = []
    for directory in finished_batches_dir.iterdir():
        if re.match('^[0-9_]+$', directory.name):
            if has_minhash_content(directory):
                collected_dirs.append(directory)
        else:
            logging.info(f'Directory name {directory.name} was not date-like')
    collected_dirs.sort()
    logging.info(f'The following directories contain fully processed '
                 f'batches: {", ".join(str(d) for d in collected_dirs)}')
    return collected_dirs


def find_batch_to_process(self_dedup_dir: Path,
                          finished_batch_names: list[str]) -> Path:
    """
    Finds the directory whose name when interpreted as a date is the earliest
    and is not contained in the blacklist of already finished batches
    """
    for directory in sorted(self_dedup_dir.iterdir()):
        if directory.name in finished_batch_names:
            continue
        if has_minhash_content(directory):
            logging.info(f'The first valid batch is {directory}')
            return directory
    return None


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    while True:
        processed_batches = collect_processed_batches(args.output_dir)
        processed_batch_names = {x.name for x in processed_batches}
        current_batch = find_batch_to_process(
            args.input_dir,
            processed_batch_names
        )
        if not current_batch:
            logging.info('No more batches to process.')
            break
        current_output = args.output_dir / current_batch.name
        cumulative_directory_deduplication(current_batch,
                                           current_output,
                                           args.output_dir,
                                           args.processes,
                                           args.permutations,
                                           args.threshold)


if __name__ == '__main__':
    main()
