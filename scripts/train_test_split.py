#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from functools import partial
from itertools import islice
import logging
from multiprocessing import Pool
import os
from pathlib import Path
import random
from typing import Optional

from cc_corpus.utils import consume, openall, otqdm, notempty


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory, parsed recursively.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the output directory.')
    parser.add_argument('--excluded-dir', '-e', type=Path,
                        help='the directory where the non-sampled documents '
                             'are written. Optional.')
    parser.add_argument('--sample-rate', '-r', type=float, default=10,
                        help='the sample rate for the test set, in '
                             'percent (10).')
    parser.add_argument('--seed', '-s', type=int, default=42,
                        help='the random seed (42).')
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


def sample(input_file: Path, input_dir: Path, output_dir: Path,
           excluded_dir: Optional[Path], sample_rate: float):
    if excluded_dir:
        excluded_file = notempty(openall(excluded_dir / input_file, 'wt'))
    else:
        excluded_file = open(os.devnull, 'wt')

    with (
        openall(input_dir / input_file, 'rt') as inf,
        notempty(openall(output_dir / input_file, 'wt')) as outf,
        excluded_file as exclf
    ):
        for line in map(str.strip, inf):
            if random.random() < sample_rate:
                print(line, file=outf)
            else:
                print(line, file=exclf)


def create_directories(base_dir: Path, relative_dirs: list[Path]):
    if base_dir:
        for relative_dir in relative_dirs:
            (base_dir / relative_dir).mkdir(parents=True, exist_ok=True)


def delete_empty_directories(base_dir: Path, relative_dirs: list[Path]):
    if base_dir:
        for relative_dir in relative_dirs[::-1]:
            full_dir = base_dir / relative_dir
            if sum(1 for _ in islice(full_dir.iterdir(), 1)) == 0:
                full_dir.rmdir()


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    os.nice(20)

    glob_paths = (f for f in args.input_dir.glob('**/*')
                  if not f.name.startswith('.'))
    relative_files = []
    relative_dirs = ['.']

    # First, let's create all possible output directories
    for path in glob_paths:
        (relative_dirs if path.is_dir() else relative_files).append(
            path.relative_to(args.input_dir)
        )

    logging.info(f'{len(relative_files)=}')
    logging.info(f'{len(relative_dirs)=} {relative_dirs=}')
    create_directories(args.output_dir, relative_dirs)
    create_directories(args.excluded_dir, relative_dirs)

    logging.info(f'Scheduled {len(relative_files)} files for sampling.')
    random.seed(args.seed)

    with Pool(args.processes) as pool:
        fn = partial(sample, input_dir=args.input_dir,
                     output_dir=args.output_dir,
                     excluded_dir=args.excluded_dir,
                     sample_rate=args.sample_rate / 100)
        consume(otqdm(pool.imap_unordered(fn, relative_files),
                      f'Sampling {args.input_dir.name}...',
                      total=len(relative_files)))
        pool.close()
        pool.join()

    # Finally, let's delete all empty putput directories
    delete_empty_directories(args.output_dir, relative_dirs)
    delete_empty_directories(args.excluded_dir, relative_dirs)

    logging.info('Done.')


if __name__ == '__main__':
    main()
