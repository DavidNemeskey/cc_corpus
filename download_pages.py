#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Standard lib
import argparse
import Queue
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
    warc1, warc2, text = inp_text.split('\r\n\r\n', 2)
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

    paragraphs_marked = (u'<p>\n{0}\n</p>'.format(paragraph.text)
                         for paragraph in paragraphs if not paragraph.is_boilerplate)
    text_removed = u'\n\n'.join(paragraphs_marked).encode('UTF-8') + '\n'
    if len(text_removed) <= 1:
        skip_action(warc1, warc2, 'JusTextBadError({0})'.format(length), entry_str)
        return None

    return '\r\n\r\n'.join((warc1, warc2, text_removed))


def mkdir_p(out_file_name_w_path):
    # Write to file
    # https://stackoverflow.com/a/12517490
    if not os.path.exists(os.path.dirname(out_file_name_w_path)):
        try:
            os.makedirs(os.path.dirname(out_file_name_w_path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def download_and_write(conn, warc_file, length_str, offset_str, out_gz_file_name, rm_boilerplate, line, retry=True):
    document = download_file(conn, warc_file, int(offset_str), int(length_str), line)
    if document is not None:  # None or gzip_text
        try:  # Test decompression
            decompressed_text = zlib.decompress(document, zlib.MAX_WBITS | 32)
        except zlib.error:
            if retry:
                download_and_write(conn, warc_file, length_str, offset_str, out_gz_file_name, rm_boilerplate, line,
                                   retry=False)
            else:
                logging.warning('Skipping because decompression error is occured after retry:\t\t{0}\t\t'.format(line))
            return

        if rm_boilerplate[0]:
            opener = gzip.open
            document = boilerplate_remove(decompressed_text, rm_boilerplate[1], line)
            if not document:
                return     # Do not write file, if there is no valuable content
        else:
            opener = open  # Put the whole into a gzip archive

        mkdir_p(out_gz_file_name)
        # https://stackoverflow.com/a/2695575
        with opener(out_gz_file_name, 'w') as f:
            f.write(document)


def download_file(s3, warc_file_name, offset, length, entry_str, retry=True):
    offset_end = offset + length - 1
    byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)
    # This is a gzip bytearray (str)
    try:
        gzip_text = s3.get_object(Bucket='commoncrawl', Key=warc_file_name, Range=byte_range)['Body'].read()
    except (ClientError, ReadTimeoutError, EndpointConnectionError) as ex:
        if hasattr(ex, 'response') and ex.response['Error']['Code'] == 'NoSuchKey':
            logging.warning("NoSuchKey: {0}".format(entry_str))
        # ReadTimeoutError has no property response
        elif hasattr(ex, 'response') and ex.response['Error']['Code'] == 'InternalError' or\
                ex.__class__.__name__ in {'ReadTimeoutError', 'EndpointConnectionError'}:
            if retry:
                return download_file(s3, warc_file_name, offset, length, entry_str, retry=False)
            logging.warning("InternalError({0}): {1}".format(ex, entry_str))
        else:
            logging.warning("Other Error({0}): {1}".format(ex, entry_str))
        return
    return gzip_text


def process_stream(conn, stream, out_dir, remove_boilerplate):
    # Regexes
    domain_re = re.compile('^https?://((www|ww2|ww3|www2|www3)[.])?([^/]+)(:[0-9]+)?/.*')
    replace_re = re.compile('[?].*')

    for num, line in enumerate(stream):
        line = line.strip()
        filename, url, warc_file, offset_str, length_str, response, mime_type = line.split(' ', 6)
        if response != "200":
            logging.warning("Skipping entry because response is not 200 ({0}) {1}".format(num, response))
            continue
        if url.endswith('/robots.txt'):
            logging.debug("Skipping robots.txt URL {0}".format(url))
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
            logging.warning("Skipping entry because response mime-type not in list ({0}) {1}".format(num, mime_type))
            continue
        filename_str = filename.replace('.gz', '')
        if num % 100 == 0:
            logging.warning("Downloading URL ({0}) {1}".format(num, url))
        logging.debug("Downloading URL ({0}) {1}".format(num, url))  # Print every url in debug mode
        m = domain_re.match(url)
        if m:
            domain = replace_re.sub('', m.group(3))
        else:
            domain = 'NONE'
        out_file = warc_file.replace('/', '_').replace('.warc.gz', '-{0}-{1}.warc.gz'.format(offset_str, length_str))

        out_gz_file_name = os.path.join(out_dir, 'pages', domain, filename_str, out_file)
        line = ' '.join((filename_str, domain, line))
        download_and_write(conn, warc_file, length_str, offset_str, out_gz_file_name, remove_boilerplate, line)


def process_index_gz_file(conn, filename, out_dir, remove_boilerplate):
    logging.warning("Starting batch {0}".format(filename))
    filename_str = filename.replace('.gz', '')
    with gzip.open(filename) as inpfh:
        process_stream(conn, (' '.join((filename_str, line)) for line in inpfh), out_dir, remove_boilerplate)


# -------------------------------------------------END Download-------------------------------------------------


def get_args():
    parser = argparse.ArgumentParser('CDX Index API Client')

    parser.add_argument('-b', '--boilerplate-language', default=None,
                        help='Boilerplate removal language (e.g. Hungarian, default: no boilerlplate removal)')
    parser.add_argument('-o', '--output-dir', default=os.path.dirname(os.path.realpath(__file__)),
                        help='Output dir of log files and pages directory (default: the current script\'s dir)')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--single-threaded', action='store_true',
                       help='Singlethreaded (read from STDIN).'
                             ' Use multithreaded for reading multiple files with glob pattern (default: singlethreaded')
    group.add_argument('-i', '--input-pattern',
                       help='Input glob pattern, with full path')

    r = parser.parse_args()

    r.remove_boilerplate = r.boilerplate_language is not None
    if not r.single_threaded and r.input_pattern is None:
        sys.stderr.write('Must choose singlethreaded to read from STDIN or supply an input pattern!\n')
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

    stoplist = None
    if remove_boilerplate_content:
        try:
            stoplist = justext.get_stoplist(stopwordlist_lang)
        except ValueError as e:
            sys.stderr.write(str(e) + '\n')
            exit(1)

    setup_logging(output_dir)

    if single_threaded:
        # Boto3 Amazon anonymous login
        session = boto3.session.Session()
        c = session.client('s3', config=Config(signature_version=UNSIGNED))

        process_stream(c, sys.stdin, output_dir, (remove_boilerplate_content, stoplist))

    else:
        num_of_threads = multiprocessing.cpu_count() * 3.25  # Heuristic number...
        q = Queue.Queue(maxsize=2 * num_of_threads)

        # In a presistent connection process a whole gz file and ask for another
        def worker(conn, out_dir, stopwords):
            while True:
                fn, rem_bp = q.get()
                process_index_gz_file(conn, fn, out_dir, remove_boilerplate=(rem_bp, stopwords))
                q.task_done()

        # Initate boto3 sessions...
        session = boto3.session.Session()
        # Start num_of_threads many boto session to process a gzip file with worker
        for i in range(num_of_threads):
            logging.warning("Creating thread no. {0}".format(i))
            c = session.client('s3', config=Config(signature_version=UNSIGNED))
            t = threading.Thread(target=worker, args=(c, output_dir, stoplist,))
            t.daemon = True
            t.start()

        # Put the gzip files into the queue to be processed
        for fname in glob.glob(glob_pattern):
            q.put((fname, remove_boilerplate_content))

        q.join()  # block until all tasks are done
