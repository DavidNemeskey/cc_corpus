#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Python version of the old filter_index.sh script. Filters problematic entries
from the downloaded index. In particular,

1. Filter URL-s with robots.txt at the end
2. Keep entries with status 200 only
3. Normalize mime-type field
4. Prefix domain (sed)
5. Keep only the allowed mime-types (fgrep)
6. Filter entries permanently resulting in decompression errors (bad index?) (fgrep)
7. Sort by the domain
"""

from argparse import ArgumentParser
import mimetypes
import os
import re
from typing import Generator, Iterator, Pattern, Set, Tuple
from urllib.parse import urlsplit

from cc_corpus.utils import openall


# typedefs
FieldIt = Iterator[Tuple[str, ...]]
FieldGen = Generator[Tuple[str, ...], None, None]

# regex patterns
robotsp = re.compile(r'/robots\.txt')
mime1p = re.compile(r'^[\\/"]*(.+?)[\\/"]*$')
mime2p = re.compile(r'[,;].*')
mime_validp = re.compile(r'^(?:[-\w]+|[*])/(?:[-+.\w]+|[*])$')
wwwp = re.compile(r'^(?:www|ww2|ww3|www2|www3)[.]')


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('input_dir', help='the input directory.')
    parser.add_argument('output_dir', help='the output directory.')
    parser.add_argument('--allowed-mimes', '-a', required=True,
                        help='the list of allowed mime types (in a file).')
    parser.add_argument('--bad-index', '-b', default=None,
                        help='a list of regexes that describe entries '
                             'permanently resulting in decompression errors '
                             '(bad index?)')
    return parser.parse_args()


def read_fields(ins: Iterator[str]) -> FieldGen:
    """Explodes the lines in the index to a tuple of six fields."""
    for url, warc, offset, length, status, mime_type, *mime_cont in (
        line.strip().split(' ') for line in ins
    ):
        yield url, warc, offset, length, status, mime_type


def basic_filter(ins: FieldIt) -> FieldGen:
    """Filters robots.txt files and pages with statuses other than 200."""
    for url, warc, offset, length, status, mime_type in ins:
        if not robotsp.search(url) and int(status) == 200:
            yield url, warc, offset, length, status, mime_type


def mime_filter(ins: FieldIt, allowed_mimes: Set[str]) -> FieldGen:
    """
    Filters invalid mime types.

    :param allowed_mimes: a set of allowed mime types.
    """
    for url, warc, offset, length, status, mime_type in ins:
        # Get rid of \/" around the mime type
        mime_type = mime1p.match(mime_type).group(1)
        # Delete parameters and multiple types
        mime_type = mime2p.sub('', mime_type)
        # text\html -> text/html
        mime_type = mime_type.replace('\\', '/')
        if not mime_validp.match(mime_type):
            mime_type, _ = mimetypes.guess_type(mime_type)
        if mime_type in allowed_mimes:
            yield url, warc, offset, length, status, mime_type


def http_filter(ins: FieldIt) -> FieldGen:
    """Prepends the domain to the fields."""
    for url, warc, offset, length, status, mime_type in ins:
        domain = urlsplit(url).netloc
        yield wwwp.sub('', domain), url, warc, offset, length, status, mime_type


def bad_index_filter(ins: FieldIt, bad_indexp: Pattern) -> FieldGen:
    """
    Filters lines that are contained (regex-wise) in the bad index list.
    The regex must cover the whole line.
    """
    for fields in ins:
        if not bad_indexp.match(' '.join(fields)):
            yield fields


def read_bad_index(bad_index_file: str) -> Pattern:
    """
    Reads the bad index file and returns a regex pattern that encompasses all
    the individual patterns in the file.
    """
    if bad_index_file:
        with openall(bad_index_file) as inf:
            return re.compile('^{}$'.format('|'.join(
                '(?:{})'.format(line.strip()) for line in inf)))
    else:
        return None


def read_allowed_mimes(allowed_mimes_file: str) -> Set[str]:
    """Reads the allowed mimes list."""
    with openall(allowed_mimes_file) as inf:
        return set(line.strip() for line in inf)


def filter_file(input_file: str, output_file: str,
                allowed_mimes: Set[str], bad_indexp: Pattern):
    with openall(input_file) as inf, openall(output_file, 'wt') as outf:
        it = read_fields(inf)
        it = basic_filter(it)
        it = mime_filter(it, allowed_mimes)
        it = http_filter(it)
        if bad_indexp:
            it = bad_index_filter(it, bad_indexp)
        for fields in it:
            print(' '.join(fields), file=outf)


def main():
    args = parse_arguments()

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    mimetypes.init()
    allowed_mimes = read_allowed_mimes(args.allowed_mimes)
    bad_indexp = read_bad_index(args.bad_indexp)

    for input_file in os.listdir(args.input_dir):
        filter_file(os.path.join(args.input_dir, input_file),
                    os.path.join(args.output_dir, input_file),
                    allowed_mimes, bad_indexp)


if __name__ == '__main__':
    main()
