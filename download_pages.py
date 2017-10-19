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


def boilerplate_remove(inp_text):
    warc1, warc2, text = inp_text.split('\r\n\r\n', 2)
    paragraphs = justext.justext(text, justext.get_stoplist('Hungarian'))  # TODO: wire out language selction to CLI
    paragraphs_marked = (u'<p>\n{0}\n</p>'.format(paragraph.text)
                         for paragraph in paragraphs if not paragraph.is_boilerplate)
    text_removed = u'\n\n'.join(paragraphs_marked).encode('UTF-8') + '\n'
    return '\r\n\r\n'.join((warc1, warc2, text_removed))


def wirte_file(out_gzip_file_name, gzip_text_str, remove_boilerplate):
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
            f.write(boilerplate_remove(zlib.decompress(gzip_text_str, 16 + zlib.MAX_WBITS)))

    else:
        # Put the whole into a gzip archive
        with open(out_gzip_file_name, 'w') as f:
            f.write(gzip_text_str)


def download_file(s3, offset, length, warc_file_name, url, filename):
    offset_end = offset + length - 1
    byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)
    # This is a gzip bytearray (str)
    try:
        gzip_text = s3.get_object(Bucket='commoncrawl', Key=warc_file_name, Range=byte_range)['Body'].read()
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            logging.warning("NoSuchKey: {0} url in {1} file for key {2} .".format(url, filename, warc_file_name))
        else:
            raise
        return
    return gzip_text


def process_index_gz_file(conn, filename, remove_boilerplate):
    filename_str = filename.replace('.gz', '')
    domain_re = re.compile('^https?://((www|ww2|ww3|www2|www3)[.])?([^/]+)(:[0-9]+)?/.*')
    replace_re = re.compile('[?].*')
    with gzip.open(filename) as inpfh:
        for num, line in enumerate(inpfh):
            line = line.strip()
            url, warc_file, offset_str, length_str = line.split(' ')
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
            ret = download_file(conn, int(offset_str), int(length_str), warc_file, url, filename)
            if ret is not None:  # None or gzip_text
                wirte_file(out_gz_file_name, ret, remove_boilerplate)

###################################################


logging.basicConfig(level=logging.INFO, format='(%(threadName)-10s) %(message)s',)


num_of_threads = 80
q = Queue.Queue(maxsize=2*num_of_threads)


# In a presistent connection process a whole gz file and ask for another
def worker(conn):
    while True:
        fn, rem_bp = q.get()
        logging.warning("Downloading links from {0}".format(fn))
        process_index_gz_file(conn, fn, remove_boilerplate=rem_bp)
        q.task_done()


# Initate boto3 sessions...
session = boto3.session.Session()
# Start num_of_threads many boto session to process a gzip file with worker
for i in range(num_of_threads):
    logging.warning("Creating thread no. {0}".format(i))
    c = session.client('s3', config=Config(signature_version=UNSIGNED))
    t = threading.Thread(target=worker, args=(c,))
    t.daemon = True
    t.start()

remove_boilerplate_content = bool(sys.argv.get(2, 1))  # True
# Put the gzip files into the queue to be processed
for fname in glob.glob(os.path.join(sys.argv[1], '*.gz')):
    q.put((fname, remove_boilerplate_content))

q.join()  # block until all tasks are done
