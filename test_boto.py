#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Tries to find out what the best way is to download file segments."""

from argparse import ArgumentParser
from itertools import groupby
import logging
from operator import attrgetter
import random
import time
import zlib

# Boto3
import boto3
from botocore.client import Config
from botocore import UNSIGNED

# Boto3 Error handling
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.vendored.requests.packages.urllib3.exceptions import ReadTimeoutError


class Record():
    def __init__(self, warc, offset, length):
        self.warc = warc
        self.offset = int(offset)
        self.length = int(length)

    def __repr__(self):
        return '({}, {}, {})'.format(self.warc, self.offset, self.length)


def parse_arguments():
    parser = ArgumentParser('Tries to find out what the best way is to ' +
                            'download file segments.')
    parser.add_argument('segments_file',
                        help='the file that lists the segments to download: '
                             'a tsv of file path, offset and length.')
    return parser.parse_args()


def download_file(s3, record, retry_left):
    warc_file_name = record.warc
    offset = record.offset
    length = record.length

    offset_end = offset + length - 1
    byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)
    decompressed_text = b''
    while len(decompressed_text) == 0 and retry_left > 0:
        retry_left -= 1
        # This is a gzip bytearray (str)
        try:
            gzip_text = s3.get_object(Bucket='commoncrawl', Key=warc_file_name, Range=byte_range)['Body'].read()
        except (ClientError, ReadTimeoutError, EndpointConnectionError) as ex:
            print('ex', ex)
            print('response', ex.response)
            if hasattr(ex, 'response') and ex.response['Error']['Code'] == 'NoSuchKey':
                logging.warning('NoSuchKey: {0}'.format(record))
                retry_left = 0  # Skip later probes
            # ReadTimeoutError has no property response
            elif hasattr(ex, 'response') and ex.response['Error']['Code'] == 'InternalError' or \
                    ex.__class__.__name__ in {'ReadTimeoutError', 'EndpointConnectionError'}:
                logging.warning('InternalError({0}): {1}'.format(ex, record))
            else:  # This shouldn't happen...
                logging.warning('Other Error({0}): {1}'.format(ex, record))
                retry_left = 0  # Skip later probes
            continue  # Skip decompression test

        try:  # Test decompression to avoid decompression errors later
            # https://stackoverflow.com/a/2695575
            decompressed_text = zlib.decompress(gzip_text, zlib.MAX_WBITS | 32)
        except zlib.error:
            logging.warning('Decompression error is occured ({0}):\t\t{1}\t\t'.format(retry_left, record))
            decompressed_text = b''

    return decompressed_text, retry_left


def download_by_segments(client, group, func_name):
    """Downloads the segments in a WARC."""
    retries = 0
    errors = 0
    start_time = time.time()
    for num_records, record in enumerate(group):
        text, retry_left = download_file(client, record, 5)
        if not text:
            errors += 1
        retries += (4 - retry_left)

    seconds = time.time() - start_time
    num_bytes = sum(record.length for record in group)
    print('{}: {}, {} segments, '.format(func_name, group[0].warc, len(group)) +
          '{} bytes, {} retries, {} errors: '.format(num_bytes, retries, errors) +
          '{:.2f} seconds => '.format(seconds) +
          '{:.2f} seconds per segment, '.format(len(group) / seconds) +
          '{:.2f} bytes per second.'.format(num_bytes / seconds))


def download_in_order(client, group):
    """Downloads the segments in a WARC file in order."""
    download_by_segments(client, group, 'download_in_order')


def download_shuffled(client, group):
    """Downloads the segments in a WARC file in shuffled order."""
    random.shuffle(group)
    download_by_segments(client, group, 'download_shuffled')


def download_full(client, group):
    """Downloads the stream in full (at least until the last segment)."""
    bytes_read = 0
    i = 0
    retries = 0
    errors = 0
    start_time = time.time()
    while i <= len(group) and retries < 10:
        try:
            body = client.get_object(Bucket='commoncrawl', Key=group[0].warc)['Body']
            for i, record in enumerate(group):
                while record.offset > bytes_read:
                    text_to_skip = body.read(record.offset - bytes_read)
                    bytes_read += len(text_to_skip)
                end = record.offset + record.length
                gzip_text = ''
                while end > bytes_read:
                    gzip_read = body.read(end - bytes_read)
                    gzip_text += gzip_read
                    bytes_read += len(gzip_read)
                try:  # Test decompression to avoid decompression errors later
                    # https://stackoverflow.com/a/2695575
                    decompressed_text = zlib.decompress(gzip_text, zlib.MAX_WBITS | 32)
                except zlib.error:
                    logging.warning('Decompression error is occured ({0}):\t\t{1}\t\t'.format(retry_left, entry_str))
                    decompressed_text = b''
        except (ClientError, ReadTimeoutError, EndpointConnectionError) as ex:
            errors += 1
            if hasattr(ex, 'response') and ex.response['Error']['Code'] == 'NoSuchKey':
                logging.warning('NoSuchKey: {0}'.format(entry_str))
                break
            # ReadTimeoutError has no property response
            elif hasattr(ex, 'response') and ex.response['Error']['Code'] == 'InternalError' or \
                    ex.__class__.__name__ in {'ReadTimeoutError', 'EndpointConnectionError'}:
                logging.warning('InternalError({0}): {1}'.format(ex, entry_str))
                num_retries += 1
            else:  # This shouldn't happen...
                logging.warning('Other Error({0}): {1}'.format(ex, entry_str))
                break

    seconds = time.time() - start_time
    num_bytes = sum(record.length for record in group)
    print('download_full: {}, {} segments, '.format(group[0].warc, len(group)) +
          '{} bytes, {} retries, {} errors: '.format(num_bytes, retries, errors) +
          '{:.2f} seconds => '.format(seconds) +
          '{:.2f} seconds per segment, '.format(len(group) / seconds) +
          '{:.2f} seconds per byte.'.format(num_bytes / seconds))


def main():
    args = parse_arguments()
    with open(args.segments_file) as inf:
        groups = [list(g) for _, g in
                   groupby((Record(*line.strip().split()) for line in inf),
                           key=attrgetter('warc'))]

        # Boto3 Amazon anonymous login
        session = boto3.session.Session()
        c = session.client('s3', config=Config(signature_version=UNSIGNED))

        for i, group in enumerate(groups):
            print(i, group[0].warc, len(group))
            if i % 3 == 0:
                download_in_order(c, group)
            elif i % 3 == 1:
                download_shuffled(c, group)
            else:
                download_full(c, group)


if __name__ == '__main__':
    main()
