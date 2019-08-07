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


class Sentence(Unit):
    """:class:`Unit` representing a sentence."""


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
            sentence.add(line)

    if document:
        yield document


def parse_file(corpus_file: str, use_headers: bool = True) -> Generator[
        Union[List, Document], None, None]:
    """
    Same as :func:`parse`, but expects the name of a file as the first argument.
    """
    with openall(corpus_file) as inf:
        yield from parse(inf, use_headers)