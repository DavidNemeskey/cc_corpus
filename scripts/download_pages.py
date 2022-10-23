#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This is the new downloader script that groups byte ranges by WARC file and
downloads the ranges pertaining to a single file in one request.

Steps:
    1. Create a list of ranges per file and write to one byte range list file
       per WARC.
    2. Sort these files.
    3. Download the ranges, write to another set of files.
    4. Read the index again, based on that read the range list, take the ranges
       needed from each file and write them to the new (final) output files.

That last step might be very slow on a HDD, but since we have SSDs, it should
not be a big problem. Too bad we have to open and close file handles all the
time.
"""

from argparse import ArgumentParser
from concurrent.futures import as_completed, ThreadPoolExecutor
import gzip
import hashlib
from itertools import groupby
import logging
import math
from operator import itemgetter
import os
from pathlib import Path
from queue import Queue
import signal
import subprocess
import threading
import time

import requests

from cc_corpus.utils import notempty, openall, otqdm


def parse_arguments():
    parser = ArgumentParser(
        description='CDX Index Batch Document Downloader')
    parser.add_argument('input_pattern',
                        help='Input glob pattern, e.g. "index/*.gz". Must '
                             'be quoted to avoid shell replacement.')
    parser.add_argument('index_output_dir', type=Path,
                        help='The directory to which the new, sorted index '
                             'files are written.')
    parser.add_argument('data_output_dir', type=Path,
                        help='The directory to which the downloaded pages are '
                             'written. The numbering will be consistent with '
                             'the files in index_output_dir, which is required '
                             'by remove_boilerplate.py.')
    parser.add_argument('--out-filename', '-of', default='common_crawl',
                        help='Output filename part (default: common_crawl).')
    parser.add_argument('--retry', '-r', type=int, default=10,
                        help='Number of retries on downloading or '
                             'decompression errors (default: 10)')
    parser.add_argument('--chunksize', '-c', type=int, default=99*1000*1000,
                        help='Chunk size in bytes (default: 99 MB)')
    parser.add_argument('--ext', '-e', default='txt.gz',
                        help='Out file extension (default: txt.gz)')
    parser.add_argument('--padding', '-p', default=4,
                        help='Padding for chunk numbering (default: 4)')
    parser.add_argument('-t', '--tmp',
                        help='The name of the temporary directory. Defaults to '
                             'the system default.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes (actually, threads) '
                             'to use (max is the num of cores, default: 1)')
    parser.add_argument('-L', '--log-level', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')

    args = parser.parse_args()
    # num_procs = len(os.sched_getaffinity(0))
    # if args.processes < 1 or args.processes > num_procs * 2 + 1:
    #     parser.error('Number of processes must be between 1 and {}'.format(
    #         num_procs * 2 + 1))
    return args


class RotatedGzip:
    """
    Writes all documents into a "single" rotated file. The file name contains a
    chunk counter. When the file reaches a certain size, it is closed and a new
    one is opened with increased chunk counter.
    """
    def __init__(self, output_dir: str, chunk_size: int, name: str,
                 padding: int = 4, extension: str = 'txt.gz'):
        """
        :param output_dir: the output directory.
        :param chunk_size: the maximum size of a file chunk (in bytes).
        :param file_name: the name of the output file.
        :param padding: the width of the chunk counter, padded with 0s.
        :param extension: the extension of the output file.
        """
        self.chunk_size = chunk_size
        os.makedirs(os.path.abspath(output_dir), exist_ok=True)
        self.output_dir = output_dir
        self.format_string = name + '_{0:0' + str(padding) + 'd}.' + extension
        self.counter = -1
        self._fh = None
        self.open_file()

    def write(self, item, *args):
        """
        Writes ``item`` (a gzipped data chunk) into the current file. The
        varargs are not used.
        """
        # TODO decompressed text?
        size = len(item)
        if self.total + size > self.chunk_size:  # Rotate
            self.close()
            self.open_file()
        self._fh.write(item)
        self.total += size

    def open_file(self):
        """Opens a new chunk."""
        self.total = 0
        self.counter += 1
        # Handle the case when some chunks already exist. Should they though?
        while self._fh is None:
            self.current_file = os.path.join(
                os.path.realpath(self.output_dir),
                os.path.basename(self.format_string.format(self.counter))
            )
            try:
                self._fh = gzip.open(self.current_file, 'xb')
            except FileExistsError:
                self.counter += 1

    def close(self):
        """Closes the file handle of the file currently being written."""
        if self._fh:
            self._fh.close()
            self._fh = None
            # Delete the current file if it is empty. See utils.notempty().
            if self.total == 0:
                os.unlink(self.current_file)


def step1(glob_pattern: str, out_dir: Path) -> int:
    # time ls *.gz | parallel zcat | sort -k 3,3 -k 4,4n > ../ide
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / 'sorted_index.gz'
    if not out_file.exists():
        os.system(f'ls {glob_pattern} | parallel zcat | sort -k 3,3 -k 4,4n '
                  f'| gzip > {out_file}')


def download_ranges(warc_file_name: str,
                    offsets_and_lengths: list[tuple[int, int]],
                    retry_left: int) -> list[bytes]:
    """
    Downloads a list of ranges from a WARC file document and returns it decompressed.

    :param warc_file_name: the name of the WARC file to download from.
    :param offset: the offset of the document in the WARC file.
    :param length: the (compressed) size of the document.
    :param retry_left: the number of retries left.
    """
    range_str = ', '.join(f'{offset}-{offset + length -1}' for offset, length in
                          offsets_and_lengths)
    sum_length = sum(length for _, length in offsets_and_lengths)
    byte_range = f'bytes={range_str}'
    content = b''
    while len(content) == 0 and retry_left > 0:
        retry_left -= 1
        try:
            r = requests.get(
                f'https://ds5q9oxwqwsfj.cloudfront.net/{warc_file_name}',
                headers={'Range': byte_range}, stream=True, timeout=60
            )
        except Exception as e:
            logging.exception(f'Exception {e} with file {warc_file_name}.')
            continue

        if r.status_code == 206:
            try:
                for chunk in r.iter_content(sum_length):
                    content = content + chunk
                    if len(content) >= sum_length:
                        break
                content = content[:sum_length]
                break
            except Exception as e:
                logging.exception(f'Exception while reading: {e}')
                content = b''
                continue
        elif r.status_code == 200:
            logging.error(f'Had to download {warc_file_name} as {byte_range} '
                          'was not available.')
            continue
        elif r.status_code == 404:
            logging.error(f'{warc_file_name} not found (404).')
            break
        else:
            continue

    # Decompression
    if len(content) == sum_length:
        curr_idx = 0
        pages = []
        for _, length in offsets_and_lengths:
            pages.append(content[curr_idx:curr_idx + length])
            curr_idx += length
        return pages
    else:
        raise ValueError(f'Could not download ranges from {warc_file_name}.')


def step2(ranges_dir: Path, num_threads: int, index_out_dir: Path,
          data_out_dir: Path, retries: int, chunk_size: int, file_prefix: str,
          doc_padding: int, extension: str):
    """The actual downloading of byte ranges collected in step1."""
    logging.info(f'{num_threads=}')
    q = Queue(num_threads * 2)

    # Signal handling so that the script can be interrupted / terminated
    # gracefully. See https://stackoverflow.com/questions/65832061/
    exiting = threading.Event()
    def signal_handler(signum, frame):  # noqa
        logging.warn(f'Received signal {signum}. Exiting...')
        exiting.set()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # The number of lines in the input, so that we know how many zeros to use
    # for padding
    ranges_file = ranges_dir / 'sorted_index.gz'
    num_lines = int(
        subprocess.check_output(f'zcat {ranges_file} | wc -l',
                                shell=True, encoding='utf-8').strip()
    )
    lines_per_file = 1000
    num_files = num_lines / num_threads / lines_per_file
    file_padding = f'{{:0{math.floor(math.log10(num_files)) + 1}}}'

    # TODO use a condition to signal end of processing
    def producer():
        """Groups ranges in the index by warc and puts them into the queue."""
        logging.info('Producer running')
        with openall(ranges_file, 'rt') as inf:
            it = (line.strip().split() for line in inf)
            for warc, ranges in groupby(it, key=itemgetter(2)):
                if exiting.is_set():
                    break
                q.put((warc, list(ranges)))
        # To signal the end of processing
        logging.info('Producer ended!')
        q.put((None, (None, None)))

    def consumer(tid: int):
        """
        Downloads the byte ranges read from the queue and writes them to disk.
        """
        logging.info(f'Consumer {tid} started....')
        chunk, written = 1, 0

        def open_files():
            """Opens a new output index and document file."""
            file_name = f'{file_prefix}_{tid}_{file_padding.format(chunk)}.gz'
            return (
                notempty(openall(index_out_dir / f'{file_name}', 'wt')),
                RotatedGzip(str(data_out_dir), chunk_size,
                            os.path.splitext(file_name)[0], doc_padding,
                            extension)
            )

        outf, doc_file = open_files()
        try:
            while not exiting.is_set():
                warc, lines = q.get(True, 5)
                if warc is not None:
                    logging.info(f'{tid} got {warc} with {len(lines)} ranges.')
                    ranges = [(int(line[3]), int(line[4])) for line in lines]
                    st = time.time()
                    downloaded = download_ranges(warc, ranges, retries)
                    logging.info(f'Downloaded in {time.time() - st} seconds.')
                    for line, doc in zip(lines, downloaded):
                        print(line, file=outf)
                        doc_file.write(doc)
                        written += 1
                        if written == lines_per_file:
                            outf.close()
                            doc_file.close()
                            chunk, written = chunk + 1, 0
                            outf, doc_file = open_files()
                else:
                    # Put the item signalling end of processing back so that
                    # other threads get it as well.
                    q.put((warc, lines))
                    break
        except:  # noqa
            logging.exception(f'Exception in {tid}')
        finally:
            logging.info(f'Consumer {tid} finished, written {chunk} files.')
            outf.close()
            doc_file.close()

    index_out_dir.mkdir(parents=True, exist_ok=True)
    data_out_dir.mkdir(parents=True, exist_ok=True)

    thread_padding = f'{{:0{math.floor(math.log10(num_threads)) + 1}}}'
    with ThreadPoolExecutor(max_workers=num_threads + 1) as executor:
        executor.submit(producer)
        for tid in range(1, num_threads + 1):
            executor.submit(consumer, thread_padding.format(tid))


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(threadName)-10s)- %(levelname)s - %(message)s'
    )

    input_str = str((Path(os.getcwd()) / args.input_pattern).resolve())
    input_hash = hashlib.sha224(input_str.encode('utf-8')).hexdigest()
    ranges_dir = Path(args.tmp) / f'ranges_{input_hash}'
    if ranges_dir.is_dir() and (ranges_dir / "sorted_index.gz").is_file():
        logging.info('Ranges already computed, skipping...')
    else:
        step1(args.input_pattern, ranges_dir)

    step2(ranges_dir, args.processes, args.index_output_dir,
          args.data_output_dir, args.retry, args.chunksize, args.out_filename,
          args.padding, args.ext)


if __name__ == '__main__':
    main()
