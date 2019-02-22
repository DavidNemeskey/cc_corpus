#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extracts HTML pages with specified URLs from a WARC file. This is so that we
can have a look at the pages that something (e.g. boilerplate removal) messes
up.
"""

from argparse import ArgumentParser
import io
import os
import re

import warc

from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--warc', '-w', dest='warc', required=True,
                        help='the WARC file to read.')
    input = parser.add_mutually_exclusive_group()
    input.add_argument('--urls', '-u',
                       help='a file that contains the urls to extract. One of '
                            '-u or -l must be specified.')
    input.add_argument('--log-file', '-l',
                       help='loads all URLs that the boilerplate removal '
                            'threw away from the WARC file. Needs the '
                            'log file. One of -u or -l must be specified.')
    parser.add_argument('--output-dir', '-o',
                        help='the directory to which the HTML files are '
                             'written. If omitted, everything is printed to '
                             'stdout.')
    return parser.parse_args()


def urls_from_file(urls_file):
    with openall(urls) as inf:
        return [u for u in inf.read().split('\n') if u]

def urls_from_log(log_file, warc_file):
    warc_m = re.match(r'(.+?)_\d+\.warc(\.gz)$', warc_file)
    if warc_m:
        warc_name = warc_m.group(1) + warc_m.group(2)
    else:
        warc_name = warc_file

    urls = []
    start_p = re.compile(r' - (\d+) - INFO - Processing (.+?)...$')
    url_p = re.compile(r" - (\d+) - INFO - Nothing's left of (.+?) after "
                       r"boilerplate removal")
    end_p = re.compile(r' - (\d+) - INFO - Processed (.+?)...$')

    catching = False
    with openall(log_file) as inf:
        for line in inf:
            if not catching:
                ms = start_p.search(line)
                if ms and ms.group(2) == warc_name:
                    catching = ms.group(1)
            else:
                mu = url_p.search(line)
                if mu and mu.group(1) == catching:
                    urls.append(mu.group(2))
                else:
                    me = end_p.search(line)
                    if me and me.group(1) == catching:
                        catching = False
                        break

    return urls


def main():
    args = parse_arguments()

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    if args.urls:
        urls = urls_from_file(args.urls)
    else:
        urls = urls_from_log(args.log_file, args.warc)

    for record in warc.open(args.warc):
        url = record.header['WARC-Target-URI']
        if url in urls:
            page = record.payload.read().split(b'\r\n\r\n', maxsplit=1)[1]
            file_name = os.path.join(
                args.output_dir,
                url.rstrip(os.path.sep).replace(os.path.sep, '_')
            )
            if args.output_dir:
                with openall(file_name, 'wb') as outf:
                    outf.write(page)
            else:
                print(page)


if __name__ == '__main__':
    main()
