#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from contextlib import closing
import glob
import gzip
import itertools
import logging
import multiprocessing
import os
import queue
import sys
import threading
import time

# Boto3
import boto3
from botocore.client import Config
from botocore import UNSIGNED


def parse_arguments():
    parser = argparse.ArgumentParser('CDX Index Batch Document Downloader')
    parser.add_argument('-o', '--output-dir', required=True,
                        help='Output dir of log files and pages directory')
    parser.add_argument('-of', '--out-filename', default=None,
                        help='Output filename part (default: batch name or '
                             'input file name)')
    parser.add_argument('-r', '--retry', type=int, default=10,
                        help='Number of retries on downloading or '
                             'decompression errors (default: 10)')
    parser.add_argument('-c', '--chunksize', type=int, default=99*1000*1000,
                        help='Chunk size in bytes (default: 99 MB)')
    parser.add_argument('-e', '--ext', default='txt.gz',
                        help='Out file extension (default: txt.gz)')
    parser.add_argument('-p', '--padding', default=4,
                        help='Padding for chunk numbering (default: 4)')
    parser.add_argument('-P', '--perdoc', action='store_true',
                        help='One file per document grouped by the TLD (default: no)')
    parser.add_argument('-L', '--log-level', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--single-threaded', action='store_true',
                       help='Singlethreaded (read from STDIN). '
                            'Use multithreaded for reading multiple files '
                            'with glob pattern (default: multithreaded)')
    group.add_argument('-i', '--input-pattern',
                       help='Input glob pattern, with full path')

    args = parser.parse_args()

    if not args.single_threaded and args.input_pattern is None:
        parser.error('Must choose singlethreaded to read from STDIN or supply '
                     'an input pattern!')
    return args


class RotatedGzip:
    """
    Writes all documents into a "single" rotated file. The file name contains a
    chunk counter. When the file reaches a certain size, it is closed and a new
    one is opened with increased chunk counter.
    """
    def __init__(self, output_dir: str, batch_name: str, chunk_size: int,
                 name: str = None, padding: int = 4, extension: str = 'txt.gz'):
        """
        :param output_dir: the output directory.
        TODO
        :param batch_name: the "name" of the batch. This is the "base name"
                     (without the extension) of the original index file.
        :param chunk_size: the maximum size of a file chunk (in bytes).
        :param file_name: the name of the output file. If not specified, it
                          defaults to ``batch_name``.
        :param padding: the width of the chunk counter, padded with 0s.
        :param extension: the extension of the output file.
        """
        self.chunk_size = chunk_size
        if name is None:
            logging.info('No output filename specified; using batch name: '
                         '{0}'.format(batch_name))
            name = batch_name
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


class FilePerDocument:
    """
    Writes each document to its own file name. An alternative to
    :class:`RotatedGzip`.
    """
    def write(self, decompressed_text, file_name):
        """
        Writes ``document`` to ``file_name``. Creates all the intermediary
        directories.
        """
        os.makedirs(os.path.dirname(out_gz_file_name), exist_ok=True)
        with gzip.open(file_name, 'wb') as f:
            f.write(decompressed_text)

    def close(self):
        """Just so that it is compatible with :class:`RotatedGzip`."""
        pass


def filter_stream(stream, out_dir, conn, retries, prefilter_stream):
    start_time = time.time()
    for line_no, line in enumerate(stream, start=1):
        (filename, domain, url, warc_file, offset_str,
         length_str, response, mime_type) = line.split(' ', maxsplit=7)
        # Print every url in debug mode
        logging.debug('Downloading URL #{0}: {1}'.format(line_no, url))

        filename_str = os.path.basename(filename.replace('.gz', ''))
        line = ' '.join((filename_str, domain, url, warc_file, offset_str,
                         length_str, response, mime_type))

        document = download_file(conn, warc_file, int(offset_str),
                                 int(length_str), line, retries)
        # None or gzip_text
        if len(document) > 0:
            out_file = warc_file.replace('/', '_').replace(
                '.warc.gz', '-{0}-{1}.warc.gz'.format(offset_str, length_str))
            out_gz_file_name = os.path.join(out_dir, 'pages', domain, filename_str, out_file)
            yield filename_str, domain, line, document, out_gz_file_name
        else:
            logging.info('Could not download URL {}'.format(url))
    else:
        try:
            # Raises NameError if the stream is empty
            logging.info('Downloaded a total of {} URLs in {} seconds.'.format(
                line_no, time.time() - start_time))
        except NameError:
            pass


def process_stream(conn, stream, output_dir, retries, rotate_info):
    """
    Processes
    """
    # ENTRIES EXPECTED TO BE sorted by filename (and optionally by domain) to
    # be grouped by filename
    for batch_name, group in itertools.groupby(filter_stream(stream, output_dir, conn, retries), key=lambda x: x[0]):
        if len(rotate_info) > 0:
            writer = RotatedGzip(output_dir, batch_name, *rotate_info)
        else:
            writer = FilePerDocument()
        with closing(writer) as w:
            for _, _, line, document, out_file_name in group:
                w.write(document, out_file_name)


if __name__ == '__main__':
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(threadName)-10s)- %(levelname)s - %(message)s'
    )

    single_threaded = args.single_threaded
    output_dir = args.output_dir
    glob_pattern = args.input_pattern
    retry = args.retry
    if not args.perdoc:
        rotate_details = args.chunksize, args.out_filename, args.padding, args.ext
    else:
        rotate_details = ()

    # filter_and_sort_opts = (preprocessed_stream,
    #                         lambda x: x)

    if single_threaded:
        # Boto3 Amazon anonymous login
        session = boto3.session.Session()
        c = session.client('s3', config=Config(signature_version=UNSIGNED))

        process_stream(c, sys.stdin, output_dir, retry, rotate_details)
    else:
        num_of_threads = int(multiprocessing.cpu_count() * 5)  # Heuristic number...
        q = queue.Queue(maxsize=2 * num_of_threads)

        # In a persistent connection process a whole gz file and ask for another
        def worker(conn):
            while True:
                file_name = q.get()
                process_index_gz_file(conn, file_name, output_dir,
                                      retry, rotate_details)
                q.task_done()

        # Initate boto3 sessions...
        session = boto3.session.Session()
        # Start num_of_threads many boto sessions to process a gzip file with worker
        for i in range(num_of_threads):
            logging.warning('Creating thread no. {0}'.format(i))
            c = session.client('s3', config=Config(signature_version=UNSIGNED))
            t = threading.Thread(target=worker, args=(c,))
            t.daemon = True
            t.start()

        # Put the gzip files into the queue to be processed
        for fname in glob.glob(glob_pattern):
            q.put(fname)

        q.join()  # block until all tasks are done

    logging.info('Done.')
