#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extracts documents or paragraphs with specified IDs from corpus files. The input
is taken from the files created by minhash.py (because this is where it actually
makes sense to check the documents...)
"""

from argparse import ArgumentParser
import os

from cc_corpus.corpus import parse_file
from cc_corpus.utils import openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
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


def collect_lines_from_file(line_file, head, *extra_lines):
    """
    Collects approximately the first head line numbers from the file.
    Also appends to the returned list all additional lines in *extra_lines.
    """
    lines = []
    lines_set = set()
    with openall(line_file) as inf:
        for new_lines in ([int(e) for e in l.strip().split()] for l in inf):
            new_lines = [line for line in new_lines if line not in lines_set]
            lines += new_lines
            lines_set.update(new_lines)
            if len(lines) >= head:
                break
    lines += [int(line) for line in extra_lines if line not in lines_set]
    return lines


def extract_documents(docs_to_extract, doc_file):
    """
    Extracts the documents specified (by a url: line number dictionary) from
    the corpus file doc_file. The line number is only for display.
    """
    for doc in parse_file(doc_file, meta=False):
        url = doc.attrs.get('url')
        if url in docs_to_extract:
            yield doc, docs_to_extract[url]


def collect_documents(minhash_prefix, lines):
    block_lines = 0  # The first line that falls in the current document file
    next_line = 0  # The index of the next line in the list lines
    with openall(minhash_prefix + '.files') as filef:
        with openall(minhash_prefix + '.doc_ids') as linef:
            for doc_file, num_lines, _, offset in (l.strip().split() for l in filef):
                num_lines, offset = int(num_lines), int(offset)
                # Let's find the last line that is still in the current file
                last_line = next_line
                while (
                    last_line < len(lines) and
                    block_lines <= lines[last_line] < block_lines + num_lines
                ):
                    last_line += 1
                if last_line != next_line:
                    docs_to_extract = {}
                    # There are such lines. Let's read them!
                    linef.seek(int(offset))
                    for i, url in enumerate(linef, start=block_lines + 1):
                        if i == lines[next_line]:
                            docs_to_extract[url.strip()] = i
                            next_line += 1
                            if next_line == last_line:
                                break
                    yield from extract_documents(docs_to_extract, doc_file)
                block_lines += num_lines
    assert next_line == len(lines)


def main():
    args = parse_arguments()

    os.nice(20)

    # Collect all lines we want to extract
    if args.line_file:
        lines = collect_lines_from_file(args.line_file, args.head, *args.lines)
    lines = sorted(lines)

    for doc, line in collect_documents(args.minhash_file, lines):
        print('{}\t{}\n\n{}\n\n'.format(
            doc.attrs['url'], line, doc.content()))


if __name__ == '__main__':
    main()
