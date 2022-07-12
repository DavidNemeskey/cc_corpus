#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from contextlib import closing
import glob
import gzip
import itertools
import logging
import multiprocessing
from operator import itemgetter
import os
import queue
import sys
import threading
import time
from typing import Generator, TextIO, Tuple
import zlib

import requests

from cc_corpus.utils import openall


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='CDX Index Batch Document Downloader')
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
    def __init__(self, output_dir):
        """
        :param output_dir: the base of the output directory hierarchy.
        """
        self.output_dir = output_dir

    def write(self, decompressed_text, file_name):
        """
        Writes ``document`` to :attr:`FilePerDocument.output_dir`/``file_name``.
        Creates all the intermediary directories.
        """
        full_path = os.path.join(self.output_dir, file_name)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with gzip.open(full_path, 'wb') as f:
            f.write(decompressed_text)

    def close(self):
        """Just so that it is compatible with :class:`RotatedGzip`."""
        pass


def download_range(warc_file_name: str, offset: int, length: int,
                   entry_str: str, retry_left: int) -> bytes:
    """
    Downloads a document and returns it decompressed.

    :param warc_file_name: the name of the WARC file to download from.
    :param offset: the offset of the document in the WARC file.
    :param length: the (compressed) size of the document.
    :param entry_str: the index line that corresponds to the document being
                      downloaded. For logging purposes only.
    :param retry_left: the number of retries left.
    """
    offset_end = offset + length - 1
    byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)
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
                for chunk in r.iter_content(length):
                    content = content + chunk
                    if len(content) >= length:
                        break
                content = content[:length]
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
    if content:
        try:
            return zlib.decompress(content, zlib.MAX_WBITS | 32)
        except zlib.error:
            logging.exception(
                f'Decompression error occured ({retry_left}):'
                f'\t\t{entry_str}\t\t'
            )
        except: # noqa
            logging.exception('Some other error while decompressing')

    return ''


def download_stream(stream: TextIO, retries: int) -> Generator[
    Tuple[str, str, bytes, str], None, None
]:
    """
    Downloads all documents in the stream and yields all information necessary
    to process each: the batch name (name of the index file), the index line,
    the decompressed document and the name of the individual output file. The
    latter might or might not be used by the caller (depending on the value of
    the file-per-doc option).

    :param stream: the index stream.
    :param retries: the number of times download is attempted for a document.
    """
    start_time = time.time()
    line_no = 0
    for line_no, line in enumerate(stream, start=1):
        (filename, domain, url, warc_file, offset_str,
         length_str, response, mime_type) = line.split(' ', maxsplit=7)
        # Print every url in debug mode
        logging.debug('Downloading URL #{0}: {1}'.format(line_no, url))

        batch_name = os.path.basename(filename.replace('.gz', ''))
        line = ' '.join((batch_name, domain, url, warc_file, offset_str,
                         length_str, response, mime_type))

        document = download_range(warc_file, int(offset_str),
                                  int(length_str), line, retries)
        # None or gzip_text
        if len(document) > 0:
            out_file = warc_file.replace('/', '_').replace(
                '.warc.gz', '-{0}-{1}.warc.gz'.format(offset_str, length_str))
            out_gz_file_name = os.path.join('pages', domain, batch_name, out_file)
            yield batch_name, line, document, out_gz_file_name
        else:
            logging.info('Could not download URL {}'.format(url))
    else:
        logging.info('Downloaded a total of {} URLs in {} seconds.'.format(
            line_no, time.time() - start_time))


def process_stream(stream: TextIO, output_dir: str, retries: int,
                   rotate_info: Tuple):
    """
    Processes a stream of index lines: downloads the URLs corresponding to each.

    :param stream: the index stream. Each line must contain the following seven
                   fields, separated by spaces: index file name, domain, url,
                   the WARC file that contains the document, its offset and
                   length, the HTML status code and mime type of the document.
    :param output_dir: the directory to which the documents are downloaded.
    :param retries: the number of times download is attempted for a document.
    :param rotate_info: details for gzip file rotation. If empty: each
                        document is written to a separate file.
    """
    # ENTRIES EXPECTED TO BE sorted by filename (and optionally by domain) to
    # be grouped by filename
    for batch_name, group in itertools.groupby(
        download_stream(stream, retries), key=itemgetter(0)
    ):
        if len(rotate_info) > 0:
            writer = RotatedGzip(output_dir, batch_name, *rotate_info)
        else:
            writer = FilePerDocument(output_dir)
        with closing(writer) as w:
            for _, line, document, out_file_name in group:
                w.write(document, out_file_name)


def process_index_file(filename: str, output_dir: str, retries: int,
                       rotate_info: Tuple):
    """
    Processes an index file: downloads all URLs in it. This functions is
    basically a wrapper to :func:`process_stream`. ``filename`` is the name of
    the index file; the rest of the parameters are the same as for
    :func:`process_stream`.
    """
    logging.info('Starting file {}...'.format(filename))
    with openall(filename) as inpfh:
        process_stream(('{} {}'.format(filename, line) for line in inpfh),
                       output_dir, retries, rotate_info)
    logging.info('Finished file {}.'.format(filename))


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

    if single_threaded:
        process_stream(sys.stdin, output_dir, retry, rotate_details)
    else:
        num_of_threads = int(multiprocessing.cpu_count() * 5)  # Heuristic number...
        q = queue.Queue(maxsize=2 * num_of_threads)

        # In a persistent connection process a whole gz file and ask for another
        def worker():
            while True:
                file_name = q.get()
                process_index_file(file_name, output_dir, retry, rotate_details)
                q.task_done()

        # Start num_of_threads many boto sessions to process a gzip file with worker
        for i in range(num_of_threads):
            logging.warning('Creating thread no. {0}'.format(i))
            t = threading.Thread(target=worker, args=())
            t.daemon = True
            t.start()

        # Put the gzip files into the queue to be processed
        for fname in glob.glob(glob_pattern):
            q.put(fname)

        q.join()  # block until all tasks are done

    logging.info('Done.')
