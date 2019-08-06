#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shuffles the documents in a set of tsv file. Useful if the corpus is used for
training machine learning models, so that the input is sufficiently varied.

The same number of output files will be created as input files.
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Manager, Pool
import os
from queue import Empty
import random
import sys

from multiprocessing_logging import install_mp_handler

from cc_corpus.tsv import parse_file
from cc_corpus.utils import collect_inputs, notempty, openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('input_dirs', nargs='+',
                        help='the input directories.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--documents', '-d', type=int,
                        help='the number of documents an input file '
                             'contains. This is the same as the number of '
                             'documents an output file will contain.')
    parser.add_argument('--zeroes', '-Z', type=int, default=4,
                        help='the number of zeroes in the output files\' names.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of consumer and producer processes to '
                             'use (max is the num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()

    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


def consumer(input_files, queue):
    input_names = [os.basename(f) for f in input_files]
    infs = [parse_file(f) for f in input_files]
    docs_read = 0

    # Get rid of the header
    for inf in infs:
        next(inf)

    while infs:
        i = random.randint(0, len(infs) - 1)
        docs_read += 1
        try:
            queue.put(next(infs[i]))
        except StopIteration:
            logging.info(f'Finished reading {input_names[i]}.')
            del infs[i]
            del input_names[i]

    return docs_read


def producer(output_files, queue, header, documents):
    output_names = [os.basename(f) for f in output_files]
    outfs = [notempty(openall(f)) for f in output_files]
    written = [0 for _ in outfs]
    docs_written = 0

    # Write the header
    for outf in outfs:
        print(header, file=outf)

    while outfs:
        i = random.randint(0, len(outfs) - 1)
        try:
            doc = queue.get(timeout=60)
            print(doc, file=outfs[i])
            written[i] += 1
            docs_written += 1
            if written[i] == documents:
                logging.info(f'Written {documents} documents to '
                             f'{output_names[i]}; closing...')
                outfs[i].close()
                del outfs[i]
                del written[i]
                del output_names[i]
        except Empty:
            logging.info('Timeout waiting for queue; cleaning up...')
            break

    # Close any dangling output files
    for i in range(len(outfs)):
        logging.info(f'Written {written[i]} documents to '
                     f'{output_names[i]}; closing...')
        outfs[i].close()

    return docs_written


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    input_files = collect_inputs(args.input_dirs)
    logging.info('Scheduled {} files for shuffling.'.format(len(input_files)))
    if not input_files:
        logging.critical('No input files!')
        sys.exit(1)

    output_files = [os.path.join(args.output_dir, os.path.basename(f))
                    for f in input_files]

    with openall(input_files[0]) as inf:
        header = inf.readline().strip()

    with Pool(args.processes) as inpool, Pool(args.processes) as outpool:
        m = Manager()
        queue = m.Queue(maxsize=1000)

        # Each worker gets a chunk of all input / output files
        input_chunks = [input_files[i:i + args.processes]
                        for i in range(0, len(input_files), args.processes)]
        output_chunks = [output_files[i:i + args.processes]
                         for i in range(0, len(output_files), args.processes)]

        consumer_f = partial(consumer, queue=queue)
        docs_read = sum(inpool.map(consumer_f, input_chunks))
        producer_f = partial(producer, queue=queue, header=header,
                             documents=args.documents)
        docs_written = sum(outpool.map(producer_f, output_chunks))

        logging.debug('Joining processes...')
        inpool.close()
        outpool.close()
        inpool.join()
        outpool.join()
        logging.debug('Joined processes.')

        if docs_read != docs_written:
            logging.error(f'The number of documents read ({docs_read}) and '
                          f'the number of documents written ({docs_written}) '
                          f'differs!')

    logging.info('Done.')


if __name__ == '__main__':
    main()
