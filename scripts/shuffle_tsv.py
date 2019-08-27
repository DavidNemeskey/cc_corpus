#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shuffles the documents in a set of tsv file. Useful if the corpus is used for
training machine learning models, so that the input is sufficiently varied.

The same number of output files will be created as input files.

The script starts a number of producer processes that read the input files and
a number of consumer processes that write to the new (shuffled) files.
"""

from argparse import ArgumentParser
from functools import partial
import logging
from multiprocessing import Lock, Manager, Pool, Queue, Value
import os
from queue import Empty
import random
import sys
from typing import List

from multiprocessing_logging import install_mp_handler

from cc_corpus.tsv import parse_file
from cc_corpus.utils import collect_inputs, notempty, openall, split_into


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
    parser.add_argument('--queue-size', '-q', type=int, default=1000,
                        help='the maximum number of elements that the queue '
                             'between the producers and consumers can hold. '
                             'The default is 1000.')
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


def producer(input_files: List[str], queue: Queue,
             num_readers: Value, lock: Lock) -> int:
    """
    Reads the input files and puts them into a queue.

    :param input_files: list of input file names.
    :param queue: the queue shared with all processes.
    :param num_readers: a shared variable holding the number of readers that
                        are still active. When this function fininshes, it
                        decreases this value by one.
    :param lock: a lock that regulates access to *num_readers*.
    :returns: the number of documents read.
    """
    logging.info(f'Producer started with {len(input_files)} files.')
    input_names = [os.path.basename(f) for f in input_files]
    infs = [parse_file(f) for f in input_files]
    docs_read = 0

    # Get rid of the header
    for inf in infs:
        next(inf)

    while infs:
        i = random.randint(0, len(infs) - 1)
        try:
            try:
                doc = next(infs[i])
                docs_read += 1
                if docs_read % 1000 == 0:
                    logging.debug(f'Producer has read {docs_read} documents.')
                queue.put(doc)
            except StopIteration:
                logging.info(f'Finished reading {input_names[i]}.')
                del infs[i]
                del input_names[i]
        except:
            logging.exception(f'Exception reading {input_names[i]}!')
            sys.exit(2)

    with lock:
        num_readers.value -= 1
        val = num_readers.value

    logging.info(f'Producer finished; read {docs_read} documents; '
                 f'num_readers is at {val}.')
    return docs_read


def consumer(output_files: List[str], queue: Queue, header: str,
             documents: int, num_readers: Value, lock: Lock) -> int:
    """
    Reads :class:`Document`s from the shared queue and writes them to one of
    the output files at random.

    :param output_files: list of output file names.
    :param queue: the queue shared with all processes.
    :param header: the header of the tsv files. Written to all output files.
    :param documents: the number of documents to write to an output file.
    :param num_readers: a shared variable holding the number of readers that
                        are still active. This function exits if two conditions
                        are met: the queue is empty and *num_readers* is 0.
    :param lock: a lock that regulates access to *num_readers*.
    :returns: the number of documents written.
    """
    logging.info(f'Consumer started with {len(output_files)} files.')
    output_names = [os.path.basename(f) for f in output_files]
    outfs = [notempty(openall(f, 'wt')) for f in output_files]
    written = [0 for _ in outfs]
    docs_written = 0

    # Write the header
    for outf in outfs:
        print(header, file=outf)

    while outfs:
        i = random.randint(0, len(outfs) - 1)
        try:
            print(queue.get(timeout=5), file=outfs[i])
            written[i] += 1
            docs_written += 1
            if docs_written % 1000 == 0:
                logging.debug(f'Consumer has written {docs_written} documents.')
            if written[i] == documents:
                logging.info(f'Written {documents} documents to '
                             f'{output_names[i]}; closing...')
                outfs[i].close()
                del outfs[i]
                del written[i]
                del output_names[i]
        except Empty:
            with lock:
                if num_readers.value == 0:
                    logging.info('Timeout waiting for queue; cleaning up...')
                    break
        except:
            logging.exception(f'Exception writing {output_names[i]}!')
            sys.exit(3)

    # Close any dangling output files
    for i in range(len(outfs)):
        logging.info(f'Written {written[i]} documents to '
                     f'{output_names[i]}; closing...')
        outfs[i].close()

    logging.info(f'Consumer finished; written {docs_written} documents.')
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
        num_readers = m.Value('I', args.processes)
        lock = m.Lock()

        # Each worker gets a chunk of all input / output files
        input_chunks = list(split_into(input_files, args.processes))
        output_chunks = list(split_into(output_files, args.processes))

        producer_f = partial(producer, queue=queue,
                             num_readers=num_readers, lock=lock)
        inresult = inpool.map_async(producer_f, input_chunks)
        consumer_f = partial(consumer, queue=queue, header=header,
                             documents=args.documents, num_readers=num_readers,
                             lock=lock)
        outresult = outpool.map_async(consumer_f, output_chunks)

        docs_read, docs_written = sum(inresult.get()), sum(outresult.get())

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
