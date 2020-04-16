#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Contains code that works with the tsv format output by emtsv."""

import re
from typing import Generator, List, TextIO, Union

from cc_corpus.utils import IllegalStateError, openall


class Unit:
    """
    A unit of text structure: a sentence, paragraph or document. It consists
    of a single comment and a list of other units of strings. Both lists
    may be empty.
    """
    def __init__(self, comment=None, content=None):
        self.comment = comment
        self.content = content or []

    def add(self, unit: 'Unit'):
        """Adds a unit under this one."""
        self.content.append(unit)

    def __iter__(self):
        """
        Iterates through the content :class:`Unit`s in this :class:`Unit`.
        """
        yield from self.content

    def __str__(self):
        return self.comment + '\n' + '\n'.join(str(unit) for unit in self.content)

    def __bool__(self):
        return bool(self.comment or self.content)

    def __len__(self):
        return sum(len(c) for c in self.content)


class Sentence(Unit):
    """:class:`Unit` representing a sentence."""
    def __len__(self):
        return len(self.content)

    def __str__(self):
        return (self.comment + '\n' +
                '\n'.join(str(unit) for unit in self.content) + '\n')


class Paragraph(Unit):
    """:class:`Unit` representing a paragraph."""


class Document(Unit):
    """:class:`Unit` representing a document."""


def parse(input: TextIO, use_headers: bool = True) -> Generator[
        Union[List, Document], None, None]:
    """
    Enumerates Documents in a text stream in the tsv format emitted by emtsv.
    If the *use_headers* parameters is ``True`` (the default), the stream is
    expected to have the headers as the first line. This header is then
    returned first (as a :class:`list`).

    Documents are returned as :class:`Document`s. These are like the root of
    a DOM tree, only in this case the tree is only three levels deep:

    - the root is a :class:`Document`,
    - which contains :class:`Paragraph`s,
    - which, in turn, contains :class:`Sentence`s.
    """
    newdocp = re.compile('^# newdoc id = ')
    newparp = re.compile('^# newpar id = ')
    textp = re.compile('^# text = ')
    commentp = re.compile('^# ')

    if use_headers:
        yield input.readline().rstrip('\n').split('\t')

    document = paragraph = sentence = None
    for line_no, line in enumerate(map(str.strip, input), start=1):
        if commentp.match(line):
            if newdocp.match(line):
                if document:
                    yield document
                document, paragraph, sentence = Document(line), None, None
            elif newparp.match(line):
                paragraph = Paragraph(line)
                document.add(paragraph)
            elif textp.match(line):
                sentence = Sentence(line)
                paragraph.add(sentence)
        else:
            if not sentence:
                raise IllegalStateError(f'Error on line {line_no}: sentence '
                                        'starts without "text" comment.')
            if line:
                sentence.add(line)

    if document:
        yield document


def parse_file(tsv_file: str, use_headers: bool = True) -> Generator[
        Union[List, Document], None, None]:
    """
    Same as :func:`parse`, but expects the name of a file as the first argument.
    """
    with openall(tsv_file) as inf:
        yield from parse(inf, use_headers)

clean_sgp = re.compile('\[([1-3])\](?:\[Sg\]|\[S\]\[g\])')
clean_plp = re.compile('\[([1-3])\](?:\[Pl\]|\[P\]\[l\])')
clean_slashp = re.compile('^\[([NV])\]')
doublep = re.compile('\[\[+')

def clean_xpostag(xpostag):
    """Cleans the xpostag from errors in emMorph."""
    xpostag = xpostag.replace('[]', '')
    xpostag = clean_sgp.sub('[\\1Sg]', xpostag)
    xpostag = clean_plp.sub('[\\1Pl]', xpostag)
    xpostag = clean_slashp.sub('[/\\1]', xpostag)
    xpostag = doublep.sub('[', xpostag)
    return xpostag
