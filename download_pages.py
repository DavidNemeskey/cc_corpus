#!/usr/bin/env python
# -*- coding: utf-8 -*-

# boto3 Amazon anonymous login
# import boto3
#  conn = boto3.client('s3', config=Config(signature_version=UNSIGNED))
from botocore.exceptions import ClientError
import boto3.session
from botocore import UNSIGNED
from botocore.client import Config
import justext
from lxml.etree import ParserError

import Queue
import threading
import os
import errno
import glob
import re
import gzip
import logging
import zlib
import os.path
import sys
import socket
import codecs


blacklist = set()
stopwordlist = justext.get_stoplist('Hungarian')


def skip_action(warc1, warc2, name, entry_str):
    out = '\r\n\r\n'.join((warc1, warc2)).replace('\r\n', '\t')
    logging.warning('Skipping({0}):\t\t{1}\t\t{2}'.format(name, entry_str, out))


def boilerplate_remove(inp_text, entry_str):
    warc1, warc2, text = inp_text.split('\r\n\r\n', 2)
    length = len(text)
    if length <= 4:  # Threshold
        skip_action(warc1, warc2, 'LengthError({0})'.format(length), entry_str)
        return None
    try:
        paragraphs = justext.justext(text, stopwordlist)  # TODO: wire out language selction to CLI
    except (ParserError, UnicodeDecodeError) as err:
        # Do not distinguish between the different errors
        skip_action(warc1, warc2, err.__class__.__name__ + str(length), entry_str)
        return None

    paragraphs_marked = (u'<p>\n{0}\n</p>'.format(paragraph.text)
                         for paragraph in paragraphs if not paragraph.is_boilerplate)
    text_removed = u'\n\n'.join(paragraphs_marked).encode('UTF-8') + '\n'

    return '\r\n\r\n'.join((warc1, warc2, text_removed))


def write_file(out_gzip_file_name, gzip_text_str, remove_boilerplate, entry_str):
    # Write to file
    # https://stackoverflow.com/a/12517490
    if not os.path.exists(os.path.dirname(out_gzip_file_name)):
        try:
            os.makedirs(os.path.dirname(out_gzip_file_name))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    if remove_boilerplate:
        # https://stackoverflow.com/a/2695575
        with gzip.open(out_gzip_file_name, 'w') as f:
            ret = boilerplate_remove(zlib.decompress(gzip_text_str, 16 + zlib.MAX_WBITS), entry_str)
            if ret:
                f.write(ret)

    else:
        # Put the whole into a gzip archive
        with open(out_gzip_file_name, 'w') as f:
            f.write(gzip_text_str)


def download_file(s3, offset, length, warc_file_name, entry_str):
    offset_end = offset + length - 1
    byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)
    # This is a gzip bytearray (str)
    try:
        gzip_text = s3.get_object(Bucket='commoncrawl', Key=warc_file_name, Range=byte_range)['Body'].read()
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            logging.warning("NoSuchKey: {0}".format(entry_str))
        else:
            raise
        return
    return gzip_text


def process_index_gz_file(conn, filename, remove_boilerplate):
    logging.warning("Starting batch {0}".format(filename))
    filename_str = filename.replace('.gz', '')
    domain_re = re.compile('^https?://((www|ww2|ww3|www2|www3)[.])?([^/]+)(:[0-9]+)?/.*')
    replace_re = re.compile('[?].*')
    with gzip.open(filename) as inpfh:
        for num, line in enumerate(inpfh):
            line = line.strip()
            url, warc_file, offset_str, length_str = line.split(' ')
            entry_str = '\t'.join((filename, url, warc_file, offset_str, length_str))

            if num % 100 == 0:
                logging.warning("Downloading URL ({0}) {1}".format(num, url))
            logging.debug("Downloading URL ({0}) {1}".format(num, url))
            m = domain_re.match(url)
            if m:
                domain = replace_re.sub('', m.group(3))
            else:
                domain = 'NONE'
            out_file = warc_file.rsplit('/', 1)[1]. \
                replace('.warc.gz', '-{0}-{1}.warc.gz'.format(offset_str, length_str))

            out_gz_file_name = os.path.join(os.getcwd(), 'pages', domain, filename_str, out_file)
            ret = download_file(conn, int(offset_str), int(length_str), warc_file, entry_str)
            if ret is not None:  # None or gzip_text
                write_file(out_gz_file_name, ret, remove_boilerplate, entry_str)

###################################################


hostname = socket.gethostname()
logging.basicConfig(filename='{0}_download.log'.format(hostname), level=logging.INFO,
                    format='(%(asctime)-15s (' + hostname + ') %(threadName)-10s) %(message)s',)


num_of_threads = 80
q = Queue.Queue(maxsize=2*num_of_threads)


# In a presistent connection process a whole gz file and ask for another
def worker(conn):
    while True:
        fn, rem_bp = q.get()
        process_index_gz_file(conn, fn, remove_boilerplate=rem_bp)
        q.task_done()


blacklistfile = 'blacklist.txt'
if os.path.exists(blacklistfile):
    with codecs.open(blacklistfile, encoding='UTF-8') as fh:
        i = 0
        for i, l in enumerate(fh):
            blacklist.add(l.strip())
        logging.warning("Adding blacklist {0} element".format(i))

# Initate boto3 sessions...
session = boto3.session.Session()
# Start num_of_threads many boto session to process a gzip file with worker
for i in range(num_of_threads):
    logging.warning("Creating thread no. {0}".format(i))
    c = session.client('s3', config=Config(signature_version=UNSIGNED))
    t = threading.Thread(target=worker, args=(c,))
    t.daemon = True
    t.start()

remove_boilerplate_content = bool(sys.argv[2] if len(sys.argv) >= 3 else 1)  # True
# Put the gzip files into the queue to be processed
for fname in glob.glob(os.path.join(sys.argv[1], '*.gz')):
    q.put((fname, remove_boilerplate_content))

q.join()  # block until all tasks are done
