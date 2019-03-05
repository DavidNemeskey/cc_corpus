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
    parser.add_argument('--line', '-l', dest='lines', action='append',
                        default=[],
                        help='A line in the doc_ids file to extract. It '
                             'corresponds to a single document / paragraph. '
                             'Can be specified more than once.')
    parser.add_argument('--line-file', '-L',
                        help='An lsh.py result file. It extracts all line '
                             'file that contains a single document ID per '
                             'line. See -i.')
    parser.add_argument('--head', '-H', metavar='NUM', type=int, default=100,
                        help='To be used in conjunction with -L. Only collect '
                             'the first NUM documents from the file. Usually '
                             'slightly more than NUM documents will be '
                             'collected, as the script will always keep groups '
                             'of similar documents together. '
                             'The default is 100.')
    parser.add_argument('--minhash_file', '-m',
                        help='A minhash file prefix (e.g. dir/01 if there are '
                             'files 01.files and 01.doc_ids in dir).')
    return parser.parse_args()


def collect_lines_from_file(line_file, head):
    """Collects approximately the first head line numbers from the file."""
    lines = set()
    with openall(line_file) as inf:
        for docs in (line.strip().split() for line in inf):
            lines.union(map(int, docs))
            if len(lines) >= head:
                break
    return lines


def extract_documents(docs_to_extract, doc_file):
    """
    Extracts the documents specified (by a url: line number dictionary) from
    the corpus file doc_file. The line number is only for display.
    """
    for doc in parse_file(doc_file, meta=False):
        url = doc.attrs.get('url')
        if url in docs_to_extract:
            print('{}\t{}\n{}\n\n\n'.format(
                url, docs_to_extract[url], doc.content()))


def collect_documents(minhash_prefix, lines):
    block_lines = 0  # The first line that falls in the current document file
    next_line = 0  # The index of the next line in the list lines
    with openall(minhash_prefix + '.files') as filef:
        with openall(minhash_prefix + '.doc_ids') as linef:
            for doc_file, num_lines, _, offset in (l.strip().split() for l in filef):
                # Let's find the last line that is still in the current file
                last_line = next_line
                while block_lines <= lines[last_line] < block_lines + num_lines:
                    last_line += 1
                if last_line != next_line:
                    docs_to_extract = {}
                    # There are such lines. Let's read them!
                    linef.seek(offset)
                    for i, url in enumerate(linef, start=block_lines + 1):
                        if i == lines[next_line]:
                            docs_to_extract[url] = i
                            next_line += 1
                    assert next_line == last_line
                    extract_documents(docs_to_extract, doc_file)
                block_lines += num_lines
    assert next_line == len(lines)


def main():
    args = parse_arguments()

    os.nice(20)

    # Collect all lines we want to extract
    if args.line_file:
        lines = collect_lines_from_file(args.line_file, args.head)
    lines.union(args.lines)
    lines = sorted(lines)

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
