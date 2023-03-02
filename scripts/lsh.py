#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Deduplicates the documents with Locality Sensitive Hashing, based on the files
written by minhash.py.
"""

from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from functools import partial
import logging
from multiprocessing import Pool
import os
from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory

from datasketch import MinHashLSH
from multiprocessing_logging import install_mp_handler

from cc_corpus.deduplication import BatchWriter, find_all_batches, \
    read_batch, read_batch_to_memory


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the input directory that contains the minhash '
                             'batches to deduplicate.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the directory to which the updated minhash '
                             'files are written.')
    parser.add_argument('--threshold', '-t', type=float, default=0.9,
                        help='the Jaccard similarity threshold (0.9).')
    parser.add_argument('--permutations', '-p', type=int, default=256,
                        help='the number of permutations per paragraph (256).')
    parser.add_argument('--skip-same-doc', '-s', action='store_true',
                        help='if true, does not deduplicate paragraphs from '
                             'the same document.')
    parser.add_argument('--temp-dir', '-T', type=Path,
                        help='the directory used to temporarily store partial '
                             'results')
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
    subparsers = parser.add_subparsers(
        help='Choose between two deduplication tasks.')
    parser_self = subparsers.add_parser(
        'self', aliases=['auto'], help='Fully deduplicate a corpus.')
    parser_self.set_defaults(command='self')
    parser_other = subparsers.add_parser(
        'other', aliases=['cross'],
        help='Remove all documents from a corpus that are found in another.'
    )
    parser_other.set_defaults(command='other')
    parser_other.add_argument('--cross-dir', '-c', type=Path, required=True,
                              help='the directory that contains the minhash '
                                   'values for the corpus to '
                                   'cross-deduplicate with.')

    parser_cumulative_cross = subparsers.add_parser(
        'cumulative', help='Remove all documents from a corpus that are found'
                           ' in any of the earlier corpora'
    )
    parser_cumulative_cross.set_defaults(command="cumulative")
    parser_cumulative_cross.add_argument(
        '--cumulative-dir', '-c', type=Path, required=True,
        help='the directory which contains the subdirectories with the minhash'
             ' values of the corpora to be used as the basis for '
             'deduplication')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    if args.command == 'other' and not args.cross_dir.is_dir():
        parser.error('The minhash directory for the other corpus (-c) '
                     'must exist.')
    return args


def deduplicate_self(file_prefix: Path, output_dir: Path,
                     threshold: float, permutations: int):
    """
    Deduplicates a set of minhashed documents (3 files with the same minhash
    prefix) and writes them to output_dir.

    Warning: only works for full documents at this point!
    """
    lsh = MinHashLSH(threshold=threshold, num_perm=permutations)
    file_base = file_prefix.name
    logging.info(f'Processing batch {file_base}...')
    total_read = 0
    duplicate_urls = 0
    with closing(BatchWriter(sys.maxsize, output_dir,
                             len(file_base), int(file_base))) as bw:
        for input_file, results in read_batch(file_prefix):
            minhashes, new_minhashes = results['minhash'], []
            doc_ids, new_doc_ids = results['id'], []
            total_read += len(doc_ids)
            input_duplicate_urls = 0
            for doc_id, minhash in zip(doc_ids, minhashes):
                key = '_'.join(doc_id)
                if key in lsh:
                    input_duplicate_urls += 1
                    continue
                if not lsh.query(minhash):
                    lsh.insert(key, minhash)
                    new_minhashes.append(minhash)
                    new_doc_ids.append(doc_id)
            bw.write_results(input_file,
                             {'id': new_doc_ids, 'minhash': new_minhashes})
            duplicate_urls += input_duplicate_urls
            logging.debug('Kept {} documents out of {} in file {}; '
                          '{} duplicate urls.'.format(
                              len(new_doc_ids), len(doc_ids),
                              input_file, input_duplicate_urls))
    logging.info('Deduplicated batch {}; kept {} documents out of {}; '
                 '{} duplicate urls.'.format(
                     file_base, bw.total_written, total_read, duplicate_urls))
    return bw.total_written, total_read


def read_batch_to_lsh(batch: Path, threshold: float, permutations: int) \
        -> MinHashLSH:
    lsh = MinHashLSH(threshold=threshold, num_perm=permutations)
    for input_file, results in read_batch(batch):
        for doc_id, minhash in zip(results['id'], results['minhash']):
            lsh.insert('\t'.join(doc_id), minhash)
    return lsh


def deduplicate_other(main_batch: Path,
                      batches_to_subtract: list[Path],
                      output_dir: Path,
                      threshold: float,
                      permutations: int):
    """
    Removes all documents from a set of minhashed documents (3 files with the
    same minhash prefix) that occur in other batches. Both main_batch and
    batches_to_subtract should be batch prefixes.

    Warning: only works for full documents at this point!
    """
    main_base = main_batch.name
    logging.info(f'Processing input batch {main_base}...')
    main_batch_data = read_batch_to_memory(main_batch)
    initial_len = len(main_batch_data)

    # Now, remove all documents in it that are contained in the batches
    # to subtract:
    for batch in batches_to_subtract:
        initial_batch_len = len(main_batch_data)
        lsh = read_batch_to_lsh(batch, threshold, permutations)
        main_batch_data = [x for x in main_batch_data if not lsh.query(x[1])]
        logging.info(
            f'Cross-deduplicated input batch {main_base} with cross batch '
            f'{batch.name}: {initial_batch_len} -> {len(main_batch_data)} '
            'documents'
        )
    # We print the documents left:
    with closing(BatchWriter(sys.maxsize, output_dir,
                             len(main_base), int(main_base))) as bw:
        # We have to organize the data into subsets per source file:
        current_docfile = None
        doc_ids = []
        minhashes = []
        for doc_id, minhash, docfile in main_batch_data:
            if docfile == current_docfile:
                # We are collecting doc_ids and minhashes per source files.
                doc_ids.append(doc_id)
                minhashes.append(minhash)
            else:
                # We reached the contents of another source .gz file.
                # write data here
                if doc_ids:
                    bw.write_results(current_docfile, {'id': doc_ids,
                                                       'minhash': minhashes})
                current_docfile = docfile
                doc_ids = []
                minhashes = []
        # To write the data for the last source file:
        if doc_ids:
            bw.write_results(current_docfile, {'id': doc_ids,
                                               'minhash': minhashes})

    logging.info(f'Processed input batch {main_base}; '
                 f'kept {len(main_batch_data)} out of {initial_len} documents'
                 )
    return len(main_batch_data), initial_len


def single_directory_deduplication(input_dir: Path,
                                   output_dir: Path,
                                   processes: int,
                                   permutations: int,
                                   threshold: float):
    """The "real" main function of the "self" mode."""
    working_dir = output_dir / 'self'
    working_dir.mkdir(parents=True, exist_ok=True)

    batch_prefixes = find_all_batches(input_dir)
    logging.info(f'Found a total of {len(batch_prefixes)} batches '
                 f'in {input_dir}.')

    # First, deduplicate documents _within_ the same batch
    original_doc_num, self_doc_num, final_doc_num = 0, 0, 0
    with Pool(processes) as pool:
        f = partial(deduplicate_self, output_dir=working_dir,
                    threshold=threshold, permutations=permutations)
        for new_num, old_num in pool.map(f, batch_prefixes):
            original_doc_num += old_num
            self_doc_num += new_num
    pool.close()
    pool.join()

    logging.info(f'Self deduplication done; in all, kept '
                 f'{self_doc_num} documents out of {original_doc_num}.')

    # Now, we need to do the deduplication between batches. The idea here is
    # to load one batch into memory, and delete all documents from it that are
    # also present in any of the other batches (more precisely, we only need to
    # do the upper triangle matrix: batch b_i is deduplicated with batches b_j,
    # where j > i).
    # At this point, we do all work in output_dir.
    # Yes, there is no need to send the last batch through this round, except
    # for counting final_doc_num.
    batch_prefixes = find_all_batches(working_dir)
    batches_to_subtract = [
        find_all_batches(working_dir, int(file_prefix.name))
        for file_prefix in batch_prefixes
    ]

    with ProcessPoolExecutor(max_workers=processes) as executor:
        f = partial(deduplicate_other, output_dir=output_dir,
                    threshold=threshold, permutations=permutations)
        final_doc_num = sum(num for num, _ in
                            executor.map(f, batch_prefixes,
                                         batches_to_subtract))

    logging.info('Full deduplication done; in all, kept '
                 '{} documents out of {}.'.format(final_doc_num,
                                                  original_doc_num))

    # Let's delete the intermediate directory.
    shutil.rmtree(working_dir)


def pairwise_directory_deduplication(input_dir: Path,
                                     output_dir: Path,
                                     cross_dir: Path,
                                     processes: int,
                                     permutations: int,
                                     threshold: float):
    """The "real" main function of the "other" mode."""
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_prefixes = find_all_batches(input_dir)
    logging.info(f'Found a total of {len(batch_prefixes)} batches '
                 f'in {input_dir}.')

    batches_to_subtract = find_all_batches(cross_dir)
    logging.info(f'Found a total of {len(batches_to_subtract)} batches in '
                 f'{cross_dir} to deduplicate against.')

    with ProcessPoolExecutor(max_workers=processes) as executor:
        f = partial(deduplicate_other, batches_to_subtract=batches_to_subtract,
                    output_dir=output_dir,
                    threshold=threshold, permutations=permutations)
        original_doc_num, final_doc_num = 0, 0
        for new_num, old_num in executor.map(f, batch_prefixes):
            original_doc_num += old_num
            final_doc_num += new_num

    logging.info(f'Cross deduplication done; in all, kept '
                 f'{final_doc_num} documents out of {original_doc_num}.')


def collect_previous_dirs(path: Path, deadline_date: str) -> list[Path]:
    """
    Collects the directories which are directly under the path given
    and whose name, when interpreted as a date, are earlier than the
    deadline_date
    """

    logging.info(f"We are looking for dirs older than {deadline_date} "
                 f"in {path}")
    # We suppose that the directories obey our strict naming convention:
    # string comparison of directory names coincides with date order.
    collected_dirs = sorted(directory for directory in path.iterdir()
                            if directory.name < deadline_date)
    logging.info(f'The following directories have been collected as the '
                 f'cumulative past: '
                 f'{", ".join(str(d) for d in collected_dirs)}')
    return collected_dirs


def cumulative_directory_deduplication(input_dir: Path,
                                       output_dir: Path,
                                       cumulative_dir: Path,
                                       temp_dir: Path,
                                       processes: int,
                                       permutations: int,
                                       threshold: float):
    """The "real" main function of the "cumulative" mode."""

    # We suppose here that the final part of the input directory is a
    # date-like string e.g.: 06_filtered/2022_12/
    input_date = input_dir.name
    past_batches = collect_previous_dirs(cumulative_dir, input_date)
    number_of_past_batches = len(past_batches)

    # If this is the earliest batch: just copy the input to the output
    if number_of_past_batches == 0:
        logging.info(f'No previous directories found: copying {input_dir} '
                     f'to {output_dir}...')
        shutil.copytree(input_dir, output_dir)
    else:
        with TemporaryDirectory(dir=temp_dir) as tmp_root_dir:
            logging.debug(f'Created temporary directory {tmp_root_dir}.')
            current_input_dir = input_dir
            for i, past_batch in enumerate(past_batches, start=1):
                if i == number_of_past_batches:
                    # This is the last cross-deduplication, the results will
                    # go to the final output dir
                    current_output_dir = output_dir
                else:
                    # There are still cross-deduplications to do, the results
                    # will go to the tmp output dir.
                    current_output_dir = Path(tmp_root_dir).joinpath(
                        f'{input_date}_against_{past_batch.name}'
                    )
                logging.info(f'Cross-deduplicating {current_input_dir} with '
                             f'{past_batch}, moving results '
                             f'to {current_output_dir}')
                pairwise_directory_deduplication(current_input_dir,
                                                 current_output_dir,
                                                 past_batch, processes,
                                                 permutations, threshold)
                # We cannot (at least, should not) keep all the temporary
                # directories on disk at the same time, so let's just
                # delete the input directory once it's not needed anymore
                # Obviously, we mustn't delete the original input...
                if i != 1:
                    logging.debug('Deleting temporary input directory '
                                  f'{current_input_dir}...')
                    shutil.rmtree(current_input_dir)
                current_input_dir = current_output_dir


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()
    os.nice(20)

    if args.command == "self":
        single_directory_deduplication(args.input_dir, args.output_dir,
                                       args.processes, args.permutations,
                                       args.threshold)
    elif args.command == "other":
        pairwise_directory_deduplication(args.input_dir, args.output_dir,
                                         args.cross_dir, args.processes,
                                         args.permutations, args.threshold)
    elif args.command == "cumulative":
        cumulative_directory_deduplication(args.input_dir, args.output_dir,
                                           args.cumulative_dir, args.temp_dir,
                                           args.processes, args.permutations,
                                           args.threshold)


if __name__ == '__main__':
    main()
