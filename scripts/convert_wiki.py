#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Converts Wikipedia extracts created by
`WikiExtractor.py <https://github.com/attardi/wikiextractor>`_ into the general
cursor format.
"""

from argparse import ArgumentParser
from contextlib import closing
from functools import partial
from io import StringIO
import json
import logging
from multiprocessing import Manager, Pool
import os
import os.path as op
from queue import Empty
import re

from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import BatchWriter, Document
from cc_corpus.utils import collect_inputs, headtail, openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input', '-i', dest='inputs', required=True,
                        action='append', default=[],
                        help='the files/directories of Wikipedia extracts.')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory.')
    parser.add_argument('--documents', '-d', type=int,
                        help='the number of documents a file should contain.')
    parser.add_argument('--zeroes', '-Z', type=int, default=4,
                        help='the number of zeroes in the output files\' names.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()

    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


class Unit:
    def __init__(self, children=None):
        self.children = children or []

    def add(self, unit: 'Unit') -> 'Unit':
        """
        Adds a unit under this one.

        :returns: the new unit.
        """
        self.children.append(unit)
        return unit

    def __getitem__(self, index: int) -> 'Unit':
        """Short for `self.children[index]`."""
        return self.children[index]

    def __iter__(self):
        """
        Iterates through the content :class:`Unit`s in this :class:`Unit`.
        """
        yield from self.children

    def __bool__(self):
        return any(bool(c) for c in self.children)


class WikiPage(Unit):
    """:class:`Unit` representing a whole Wikipedia page (extract)."""
    def __init__(self, attrs, children=None):
        self.attrs = attrs
        super().__init__(children)


class Section(Unit):
    """:class:`Unit` representing a section."""


class List(Unit):
    """:class:`Unit` representing an ordered or unordered list."""


class Text(Unit):
    """:class:`Unit` representing a paragraph of text."""


class Title(Unit):
    """:class:`Unit` representing the title of a section."""


class DocumentConverter:
    """Converts a :class:`WikiPage` to a :class:`Document`."""
    def __init__(self):
        pass

    def __call__(self, wikipage):
        doc = Document(attrs=wikipage.attrs, paragraphs=[])
        for section in wikipage:
            title, ps = headtail(section)
            doc.paragraphs.append(title[0])
            for p in ps:
                doc.paragraphs.append('\n'.join(child for child in p))
        return doc


def first_section(inf):
    """The first line is the title, which should be a section..."""
    it = map(str.rstrip, inf)
    for line in it:
        yield f'Section::::{line}.'
        break
    yield from it


def process_file(filename, converter, queue):
    logging.info('Processing file {}...'.format(filename))
    section_p = re.compile('^Section::::(.+)[.]$')
    bullet_p = re.compile('^BULLET::::(.+)$')
    with openall(filename, 'rt') as inf:
        for page in inf:
            j = json.loads(page)
            extract = j.pop('text')
            # doc = Document(attrs=j, paragraphs=[])
            wp = WikiPage(j)
            section, text, lst = None, None, None
            # The first line is the title, which should be a section...
            for line in first_section(StringIO(extract)):
                sm = section_p.search(line)
                if sm:
                    section = wp.add(Section([Title([sm.group(1)])]))
                else:
                    # Empty line: "close" the last paragraph
                    if not line:
                        text = lst = None
                    else:
                        bm = bullet_p.search(line)
                        if bm:
                            # First bullet: let's start a new (list) paragraph
                            if not lst:
                                lst = section.add(List())
                            lst.add(bm.group(1))
                        else:
                            # Text: open a new text paragraph
                            if not text:
                                text = section.add(Text())
                            text.add(line)
            queue.put(converter(wp))
    logging.info('Finished processing file {}...'.format(filename))


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not op.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    input_files = sorted(collect_inputs(args.inputs))
    logging.info('Scheduled {} files for conversion.'.format(len(input_files)))

    writer = BatchWriter(args.documents, args.output_dir, args.zeroes)
    with Pool(args.processes) as pool, closing(writer) as bw:
        m = Manager()
        queue = m.Queue(args.processes * 10)
        converter = DocumentConverter()
        f = partial(process_file, queue=queue, converter=converter)
        res = pool.map_async(f, input_files)
        while True:
            try:
                document = queue.get(True, 5)
                bw.write(document)
            except Empty:
                # get() here will, on error, raise the same exception as
                # encountered in the workers
                if res.ready():
                    res.get()
                    break

    logging.info(f'Written {bw.total_written} documents into {bw.batch} files.')


if __name__ == '__main__':
    main()
