#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Same as autonomous_cross_deduplicator.py, but keeps all batches in memory and
so is much faster. The downside is, obviously, the memory usage.
"""

from argparse import ArgumentParser
from contextlib import closing
import logging
from pathlib import Path
import sys

from datasketch import MinHashLSH

from cc_corpus.deduplication import BatchWriter, read_batch, read_batch_to_lsh
from cc_corpus.utils import otqdm
from lsh import check_batch, mark_as_done


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory that contains the self-'
                             'deduplicated subdirectories.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the directory that contains the fully-'
                             'deduplicated subdirectories.')
    parser.add_argument('--done-dir', '-d', type=Path, action='append',
                        default=[],
                        help='a directory that contains fully-deduplicated '
                             'subdirectories unrelated to the current input. '
                             'This could be the URLs downloaded from another '
                             'TLD, a different data source, etc. Can be '
                             'specified more than once.')
    parser.add_argument('--permutations', '-p', type=int, default=256,
                        help='the number of permutations per paragraph (256).')
    parser.add_argument('--threshold', '-t', type=float, default=0.9,
                        help='the Jaccard similarity threshold (0.9).')
    parser.add_argument('--temp-dir', '-T', type=Path,
                        help='the directory used to temporarily store partial '
                             'results. The default is the system tmp dir.')
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
    return parser.parse_args()


def has_minhash_content(directory: Path) -> bool:
    """
    Tells whether a given directory contains all the following files or not:
    1.doc_ids, 1.files, 1.minhashes
    """
    return (directory.is_dir()
            and (directory / '1.doc_ids').is_file()  # noqa
            and (directory / '1.files').is_file()  # noqa
            and (directory / '1.minhashes').is_file())  # noqa


def collect_input_dirs(main_input_dir: Path) -> list[str]:
    return {directory.name for directory in main_input_dir.iterdir()
            if has_minhash_content(directory)}


def collect_completed_dirs(main_output_dir: Path) -> list[str]:
    return {directory.name for directory in main_output_dir.iterdir()
            if has_minhash_content(directory) and check_batch(directory)}


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    input_dirs = collect_input_dirs(args.input_dir)
    done_dirs = collect_completed_dirs(args.output_dir)
    dirs_to_read = sorted(input_dirs & done_dirs)
    logging.info('The following directories have already been processed: ' +
                 ", ".join(str(d) for d in sorted(dirs_to_read)))
    if len(dirs_to_go := sorted(input_dirs - done_dirs)) == 0:
        logging.info('Nothing to deduplicate.')
        sys.exit(0)
    logging.info('The following directories will be deduplicated: ' +
                 ", ".join(str(d) for d in sorted(dirs_to_go)))

    other_done_batches = sorted(
        other_done_dir / d for other_done_dir in args.done_dirs
        for d in collect_completed_dirs(other_done_dir)
    )
    logging.info('The following additional, already processed directories '
                 'will be included: '
                 ", ".join(str(d) for d in sorted(other_done_batches)))

    lsh = MinHashLSH(threshold=args.threshold, num_perm=args.permutations)

    for other_done_batch in otqdm(
        other_done_batches, 'Reading additional deduplicated directories...'
    ):
        read_batch_to_lsh(other_done_batch / '1', lsh)

    for dir_to_read in otqdm(dirs_to_read,
                             'Reading previously deduplicated directories...'):
        read_batch_to_lsh(args.output_dir / dir_to_read / '1', lsh)

    for dir_to_go in otqdm(dirs_to_go, 'Deduplicating...'):
        input_batch_dir = args.input_dir / dir_to_go
        output_batch_dir = args.output_dir / dir_to_go
        output_batch_dir.mkdir(parents=True, exist_ok=True)

        num_docs, num_kept = 0, 0
        with closing(BatchWriter(sys.maxsize, output_batch_dir, 1, 1)) as bw:
            for in_file, results in read_batch(input_batch_dir / '1'):
                doc_ids, minhashes = [], []
                for doc_id, minhash in zip(results['id'], results['minhash']):
                    num_docs += 1
                    key = '\t'.join(doc_id)
                    if not lsh.query(minhash):
                        lsh.insert(key, minhash)
                        doc_ids.append(doc_id)
                        minhashes.append(minhash)
                        num_kept += 1
                bw.write_results(in_file, {'id': doc_ids, 'minhash': minhashes})

        mark_as_done(output_batch_dir)
        logging.info(f'Processed batch {input_batch_dir}; kept {num_kept} '
                     f'out of {num_docs} documents.')

    logging.info('Done.')


if __name__ == '__main__':
    main()
