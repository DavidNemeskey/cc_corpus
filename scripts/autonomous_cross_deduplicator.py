#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Does cross-deduplication for a series of batches. Parallel processing version.
It is given a target range of batches, and cross-deduplicates them with every
older one using the functions in lsh.py
Important note: this version treats a batch done if there is a DONE.txt in the
minhash_full directory. If some of the batches are already done by other modes
they must be marked this way.
"""

from argparse import ArgumentParser
from functools import partial
import logging
import cc_corpus.istarmap   # It is here because this patches multiprocessing.
from multiprocessing import Pool
from pathlib import Path

from lsh import deduplicate_other


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory that contains the self-'
                             'deduplicated subdirectories.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the directory that contains the fully-'
                             'deduplicated subdirectories.')
    parser.add_argument('--from-dir', '-f',
                        help='the earlies batch to work on. Only use this if'
                             'other tasks are working on the preceding'
                             'batches, otherwise it will stall forever.')
    parser.add_argument('--upto-dir', '-u',
                        help='the latest batch to work on.')
    parser.add_argument('--temp-dir', '-T', type=Path,
                        help='the directory used to temporarily store partial '
                             'results. The default is the system tmp dir.')
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
    if not args.input_dir.is_dir():
        parser.error('The input directory for the batches must exist.')
    if args.temp_dir and not args.temp_dir.is_dir():
        parser.error('The temporary directory, if set, must exist.')
    if args.from_dir:
        fd_path = args.input_dir / args.from_dir
        if not (fd_path.is_dir() and has_minhash_content(fd_path)):
            parser.error('The from-dir is not a proper input batch')
    if args.upto_dir:
        ud_path = args.input_dir / args.upto_dir
        if not (ud_path.is_dir() and has_minhash_content(ud_path)):
            parser.error('The upto-dir is not a proper input batch')
    return args


def has_minhash_content(directory: Path) -> bool:
    """
    Tells whether a given directory contains all the following files or not:
    1.doc_ids, 1.files, 1.minhashes
    """
    return ((directory / '1.doc_ids').is_file()
            and (directory / '1.files').is_file()
            and (directory / '1.minhashes').is_file())


def assemble_targets(input_dir: Path, output_dir: Path,
                     from_dir: Path, upto_dir):
    """
    Returns a list of tuples. The tuples contain the parameters for the
    deduplicate_other() method:
    The input (minhash self) dir of the target
    The list of dirs of older batches (minhash full)
    The output (minhash full) dir for the target.

    Ignores dirs which do not contain the required files
    """
    list_of_dirs = [dir.name for dir in sorted(input_dir.iterdir())
                    if has_minhash_content(dir)]
    if upto_dir:
        upto_i = list_of_dirs.index(upto_dir)
        list_of_dirs = list_of_dirs[:upto_i+1]
    if from_dir:
        from_i = list_of_dirs.index(from_dir)
        target_list = list_of_dirs[from_i:]
    else:
        from_i = 0
        target_list = list_of_dirs
    logging.debug(f'The list of targets is: {target_list} \n They start from '
                  f'pos {from_i} of the relevant history: {list_of_dirs}')
    pairings = []
    for index, target in enumerate(target_list):
        # TODO we do not support multiple minhash files per batch.
        target_as_input = input_dir / target / '1'
        target_as_output = output_dir / target
        past = [output_dir / dir / '1' for dir
                in list_of_dirs[:from_i + index]]
        pairings.append((target_as_input, past, target_as_output,))
    return pairings


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    pairings = assemble_targets(args.input_dir, args.output_dir,
                                args.from_dir, args.upto_dir)
    # Create the output folders if missing:
    for _, _, output_dir in pairings:
        output_dir.mkdir(parents=True, exist_ok=True)

    f = partial(deduplicate_other, threshold=args.threshold,
                permutations=args.permutations)
    with Pool(args.processes) as p:
        for _ in p.istarmap(f, pairings):
            pass
        p.close()
        p.join()


if __name__ == '__main__':
    main()
