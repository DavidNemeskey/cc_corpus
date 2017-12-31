#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard lib
import argparse
import queue
import threading
import multiprocessing
import glob
import socket
import logging
import re
import sys
import os
import errno
import zlib
import gzip
import datetime
import itertools
import xml.sax.saxutils

# Boto3
import boto3
from botocore.client import Config
from botocore import UNSIGNED

# Boto3 Error handling
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.vendored.requests.packages.urllib3.exceptions import ReadTimeoutError

# JusText
import justext
from lxml.etree import ParserError


# Logging
def setup_logging(log_dir):
    mkdir_p(os.path.abspath(log_dir))
    hostname = socket.gethostname()
    pid = str(os.getpid()) + ' ' + str(datetime.datetime.now())
    out_filename = os.path.join(log_dir, '{0}_{1}_download.log'.format(hostname, pid))
    logging.basicConfig(filename=out_filename,
                        level=logging.INFO,
                        format='(%(asctime)-15s ('
                               + hostname + '(' + pid + ')) %(threadName)-10s) %(levelname)s:%(name)s %(message)s',
                        filemode='a')

    class StreamToLogger(object):
        """
        Fake file-like stream object that redirects writes to a logger instance.
        """

        def __init__(self, logger, log_level=logging.INFO):
            self.logger = logger
            self.log_level = log_level
            self.linebuf = ''

        def write(self, buf):
            for line in buf.rstrip().splitlines():
                self.logger.log(self.log_level, line.rstrip())

        def flush(self):
            pass  # https://stackoverflow.com/a/20525834

    # Logger
    stdout_logger = logging.getLogger('STDOUT')
    so = StreamToLogger(stdout_logger)
    sys.stdout = so
    stderr_logger = logging.getLogger('STDERR')
    se = StreamToLogger(stderr_logger, logging.ERROR)
    sys.stderr = se

# -------------------------------------------------Download-------------------------------------------------


def skip_action(warc1, warc2, name, entry_str):
    out = '\r\n\r\n'.join((warc1, warc2)).replace('\r\n', '\t')
    logging.warning('Skipping({0}):\t\t{1}\t\t{2}'.format(name, entry_str, out))


def boilerplate_remove(inp_text, stopwordlist, entry_str):
    warc1, warc2, text = inp_text.split(b'\r\n\r\n', 2)
    length = len(text)
    if length <= 13:  # Threshold minimum: '<html></html>' is 13 long
        skip_action(warc1, warc2, 'LengthError({0})'.format(length), entry_str)
        return None
    try:
        paragraphs = justext.justext(text, stopwordlist)
    # TypeError JusText bug, AssertionError lxml bug...
    except (ParserError, UnicodeDecodeError, TypeError, AssertionError) as err:
        # Do not distinguish between the different errors
        skip_action(warc1, warc2, err.__class__.__name__ + str(length), entry_str)
        return None

    # Escape paragraph for parsable XML
    text_removed = '\n\n'.join(('<p>\n{0}\n</p>'.format(xml.sax.saxutils.escape(paragraph.text))
                                for paragraph in paragraphs if not paragraph.is_boilerplate))
    if len(text_removed) == 0:
        skip_action(warc1, warc2, 'JusTextBadError({0})'.format(length), entry_str)
        return None

    _, domain, filename, url, warc_file, offset_str, length_str, response, mime_type = entry_str.split(' ', 8)
    filename = filename.replace('.gz', '')
    return '<doc domain="{0}" index="{1}" url="{2}" warc-file="{3}" offset="{4}" length="{5}" response="{6}"' \
           ' mime-type="{7}">\n<meta>\n<request>\n{8}\n</request>\n' \
           '<response>\n{9}\n</response>\n</meta>\n{10}\n</doc>\n\n\n'.\
        format(domain, filename, url, warc_file, offset_str, length_str, response, mime_type, warc1, warc2,
               text_removed).encode('UTF-8')


def mkdir_p(out_file_name_w_path):
    # Write to file
    # https://stackoverflow.com/a/12517490
    if not os.path.exists(out_file_name_w_path):
        try:
            os.makedirs(out_file_name_w_path)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def write_gzip(decompressed_text, out_gz_file_name):
    mkdir_p(os.path.dirname(out_gz_file_name))
    with gzip.open(out_gz_file_name, 'wb') as f:
        f.write(decompressed_text)


class RotatedGzip:
    def __init__(self, out_d, batch_name, chunk_size, name, padding, ext):
        self.chunk_size = chunk_size
        if name is None:
            logging.warning('No output filename specified using batch name: {0}'.format(batch_name))
            name = batch_name
        mkdir_p(os.path.abspath(out_d))
        self.out_dir = out_d
        self.format_string = name + '_{0:0' + str(padding) + 'd}.' + ext
        self.counter = 0
        self.total = 0
        self._fh = None
        self.open_file()

    def __del__(self):
        self._fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.__del__()

    def write(self, item, _):
        size = len(item)
        if self.total + size > self.chunk_size:  # Rotate
            self._fh.close()
            self.counter += 1
            self.open_file()
        self._fh.write(item)
        self.total += size

    def open_file(self):
        self.total = 0
        self._fh = None
        while self._fh is None:
            fn = os.path.join(os.path.realpath(self.out_dir), os.path.basename(self.format_string.format(self.counter)))
            try:  # Atomically open create a new file or increment counter till can not create it...
                self._fh = gzip.open(fn, 'xb')
            except FileExistsError:
                self.counter += 1


def download_file(s3, warc_file_name, offset, length, entry_str, retry_left):
    offset_end = offset + length - 1
    byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)
    decompressed_text = b''
    while len(decompressed_text) == 0 and retry_left > 0:
        retry_left -= 1
        # This is a gzip bytearray (str)
        try:
            gzip_text = s3.get_object(Bucket='commoncrawl', Key=warc_file_name, Range=byte_range)['Body'].read()
        except (ClientError, ReadTimeoutError, EndpointConnectionError) as ex:
            if hasattr(ex, 'response') and ex.response['Error']['Code'] == 'NoSuchKey':
                logging.warning('NoSuchKey: {0}'.format(entry_str))
                retry_left = 0  # Skip later probes
            # ReadTimeoutError has no property response
            elif hasattr(ex, 'response') and ex.response['Error']['Code'] == 'InternalError' or \
                    ex.__class__.__name__ in {'ReadTimeoutError', 'EndpointConnectionError'}:
                logging.warning('InternalError({0}): {1}'.format(ex, entry_str))
            else:  # This shouldn't happen...
                logging.warning('Other Error({0}): {1}'.format(ex, entry_str))
                retry_left = 0  # Skip later probes
            continue  # Skip decompression test

        try:  # Test decompression to avoid decompression errors later
            # https://stackoverflow.com/a/2695575
            decompressed_text = zlib.decompress(gzip_text, zlib.MAX_WBITS | 32)
        except zlib.error:
            logging.warning('Decompression error is occured:\t\t{0}\t\t'.format(entry_str))
            decompressed_text = b''

    return decompressed_text


def filter_stream(stream, out_dir, conn, retries):
    # Regexes
    domain_re = re.compile('^https?://((www|ww2|ww3|www2|www3)[.])?([^/]+)(:[0-9]+)?/.*')
    replace_re = re.compile('[?].*')

    for num, line in enumerate(stream):
        line = line.strip()
        filename, url, warc_file, offset_str, length_str, response, mime_type = line.split(' ', 6)
        if response != '200':
            logging.warning('Skipping entry because response is not 200 ({0}) {1}'.format(num, response))
            continue
        if url.endswith('/robots.txt'):
            logging.debug('Skipping robots.txt URL {0}'.format(url))
            continue
        mime_type = mime_type.replace(';', ' ').replace(',', ' ').replace('\\"', '').replace('"', '').split(None, 1)[0]
        if mime_type not in {'*/*', 'all', 'aplication/pdf', 'aplication/valami', 'application/atom+xml',
                             'application/pls+xml', 'application/rss+xml', 'application/smil+xml', 'application/text',
                             'application/unknown', 'application/valami', 'application/x-dtbresource+xml',
                             'application/xhtml+xml', 'application/xml', 'application/xml+rss', 'application/x-rss+xml',
                             'application/x-unknown-content-type', 'charset=iso-8859-2', 'charset=ISO-8859-2',
                             'charset=utf-8', 'charset=UTF-8', 'Document', 'file', 'file/unknown', 'text', 'text/*',
                             'text/htm', 'texthtml', 'text/HTML', 'Text/html', 'TEXT/HTML', 'text/html', 'text/rss+xml',
                             'text/text', 'text/txt', 'text/x-httpd-php', 'text/xml', 'txt/html', 'unk',
                             'unknown/unknown', 'x-unknown/unknown'}:
            logging.warning('Skipping entry because response mime-type not in list ({0}) {1}'.format(num, mime_type))
            continue
        filename_str = os.path.basename(filename.replace('.gz', ''))
        if num % 100 == 0:
            logging.warning('Downloading URL ({0}) {1}'.format(num, url))
        logging.debug('Downloading URL ({0}) {1}'.format(num, url))  # Print every url in debug mode
        m = domain_re.match(url)
        if m:
            domain = replace_re.sub('', m.group(3))
        else:
            domain = 'NONE'
        out_file = warc_file.replace('/', '_').replace('.warc.gz', '-{0}-{1}.warc.gz'.format(offset_str, length_str))

        out_gz_file_name = os.path.join(out_dir, 'pages', domain, filename_str, out_file)
        line = ' '.join((filename_str, domain, line))

        document = download_file(conn, warc_file, int(offset_str), int(length_str), line, retries)
        # None or gzip_text
        if len(document) > 0:
            yield filename_str, domain, line, document, out_gz_file_name


def process_stream(conn, stream, out_dir, remove_boilerplate, retries, rotate_info):
    # sorted by filename and domain, and grouped by filename
    for batch_name, group in itertools.groupby(sorted(filter_stream(stream, out_dir, conn, retries),
                                                      key=lambda x: x[0:1]), key=lambda x: x[0]):
        if len(rotate_info) > 0:
            write_file = RotatedGzip(out_dir, batch_name, *rotate_info).write
        else:
            write_file = write_gzip
        for _, _, line, document, out_file_name in group:
            if remove_boilerplate[0]:
                document = boilerplate_remove(document, remove_boilerplate[1], line)
                if document is None:
                    continue
            write_file(document, out_file_name)


def process_index_gz_file(conn, filename, out_dir, remove_boilerplate, num_retries, rotate_det):
    logging.warning('Starting batch {0}'.format(filename))
    filename_str = filename.replace('.gz', '')
    with gzip.open(filename) as inpfh:
        process_stream(conn, (' '.join((filename_str, line.decode('UTF-8'))) for line in inpfh),
                       out_dir, remove_boilerplate, num_retries, rotate_det)


# -------------------------------------------------END Download-------------------------------------------------


def get_args():
    parser = argparse.ArgumentParser('CDX Index Batch Document Downloader')

    parser.add_argument('-b', '--boilerplate-language', default=None,
                        help='Boilerplate removal language (e.g. Hungarian, default: no boilerlplate removal)')
    parser.add_argument('-o', '--output-dir', default=os.path.dirname(os.path.realpath(__file__)),
                        help='Output dir of log files and pages directory (default: the current script\'s dir)')
    parser.add_argument('-of', '--out-filename',
                        help='Output filename part (default: batch name or input file name)')
    parser.add_argument('-r', '--retry', default=10,
                        help='Num. of retries on downloading or decompression errors (default: 10)')
    parser.add_argument('-c', '--chunksize', default=99*1000*1000,
                        help='Chunk size (default: 99 MB)')
    parser.add_argument('-e', '--ext', default='txt.gz',
                        help='Out file extension (default: txt.gz)')
    parser.add_argument('-p', '--padding', default=4,
                        help='Padding for numbering (default: 4)')
    parser.add_argument('-P', '--perdoc', action='store_true',
                        help='One file per document grouped by the TLD (default: no)')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--single-threaded', action='store_true',
                       help='Singlethreaded (read from STDIN).'
                             ' Use multithreaded for reading multiple files with glob pattern (default: singlethreaded')
    group.add_argument('-i', '--input-pattern',
                       help='Input glob pattern, with full path')

    r = parser.parse_args()

    r.remove_boilerplate = r.boilerplate_language is not None
    if not r.single_threaded and r.input_pattern is None:
        print('Must choose singlethreaded to read from STDIN or supply an input pattern!', file=sys.stderr)
        exit(1)

    return r


if __name__ == '__main__':

    # Parse arguments
    args = get_args()
    single_threaded = args.single_threaded
    remove_boilerplate_content = args.remove_boilerplate
    stopwordlist_lang = args.boilerplate_language
    output_dir = args.output_dir
    glob_pattern = args.input_pattern
    retry = args.retry
    if not args.perdoc:
        rotate_details = args.chunksize, args.out_filename, args.padding, args.ext
    else:
        rotate_details = ()

    stoplist = None
    if remove_boilerplate_content:
        try:
            stoplist = justext.get_stoplist(stopwordlist_lang)
        except ValueError as e:
            print(e, file=sys.stderr)
            exit(1)

    setup_logging(output_dir)

    if single_threaded:
        # Boto3 Amazon anonymous login
        session = boto3.session.Session()
        c = session.client('s3', config=Config(signature_version=UNSIGNED))

        process_stream(c, sys.stdin, output_dir, (remove_boilerplate_content, stoplist), retry, rotate_details)

    else:
        num_of_threads = int(multiprocessing.cpu_count() * 3.25)  # Heuristic number...
        q = queue.Queue(maxsize=2 * num_of_threads)

        # In a presistent connection process a whole gz file and ask for another
        def worker(conn, out_dir, stopwords):
            while True:
                fn, rem_bp = q.get()
                process_index_gz_file(conn, fn, out_dir, (rem_bp, stopwords), retry, rotate_details)
                q.task_done()

        # Initate boto3 sessions...
        session = boto3.session.Session()
        # Start num_of_threads many boto session to process a gzip file with worker
        for i in range(num_of_threads):
            logging.warning('Creating thread no. {0}'.format(i))
            c = session.client('s3', config=Config(signature_version=UNSIGNED))
            t = threading.Thread(target=worker, args=(c, output_dir, stoplist,))
            t.daemon = True
            t.start()

        # Put the gzip files into the queue to be processed
        for fname in glob.glob(glob_pattern):
            q.put((fname, remove_boilerplate_content))

        q.join()  # block until all tasks are done
