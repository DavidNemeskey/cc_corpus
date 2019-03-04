#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extracts documents or paragraphs with specified IDs from corpus files. The input
is taken from the files created by minhash.py (because this is where it actually
makes sense to check the documents...)
"""

from argparse import ArgumentParser
import io
import os
import os.path as op
import re

from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--line', '-l', action='append', default=[],
                        help='A line in the doc_ids file to extract. It '
                             'corresponds to a single document / paragraph. '
                             'Can be specify more than once.')
    parser.add_argument('--line-file', '-L',
                        help='An lsh.py result file. It extracts all line '
                             'file that contains a single document ID per '
                             'line. See -i.')
    parser.add_argument('--minhash_file', '-m', action='append', default=[],
                        help='A minhash file prefix (e.g. dir/01 if there are '
                             'files 01.files and 01.doc_ids in dir). '
                             'Can be specify more than once.')
    parser.add_argument('--minhash-dir', '-M',
                        help='A minhash directory. All files in it are scanned '
                             'for the documents requested. See -m.')
    return parser.parse_args()


def collect_minhash_dir(minhash_dir):
    """Collects minhash data files from the specified directory."""
    return sorted([op.join(minhash_dir, f)[:-8]
                   for f in os.listdir(minhash_dir) in f.endswith('.doc_ids')])


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

