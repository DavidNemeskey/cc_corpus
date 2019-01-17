#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Contains code that works with the semi-XML format of the corpus."""

from collections import OrderedDict
import concurrent.futures as cf
import io
from queue import Empty, Queue
import re
import sys
import threading

from cc_corpus.utils import openall


class ParseError(Exception):
    """Raised if the file or stream is not in the corpus XML format."""
    pass


class Document:
    uri_p = re.compile('^WARC-Target-URI: (.+?)$', re.M)

    def __init__(self, attrs=None, meta=None, paragraphs=None):
        self.attrs = attrs
        self.meta = meta
        self.paragraphs = paragraphs

    def content(self):
        """
        Returns the textual content of the document, without any markup (i.e.
        <p> tags).
        """
        return '\n'.join(self.paragraphs)

    def wc(self, p=False, w=False, c=False):
        """
        Returns the number of paragraphs (p), words (w) or characters (c) in
        the document. If all three arguments are False, returns a three-tuple
        of each of them. Otherwise returns a single number.
        """
        ps = self.paragraphs
        if ps:
            if not (p or w or c):
                return (
                    len(ps),
                    sum(len(p.split()) for p in ps),
                    sum(len(p) for p in ps) + len(ps) - 1
                )
            elif p:
                return len(ps)
            elif w:
                return sum(len(p.split()) for p in ps)
            else:
                return sum(len(p) for p in ps) + len(ps) - 1
        else:
            return 0 if p or w or c else (0, 0, 0)

    def __len__(self):
        """
        The length (in characters) of the document. Same as len(self.content()).
        """
        if self.paragraphs:
            return sum(len(p) for p in self.paragraphs) + len(self.paragraphs) - 1
        else:
            return 0

    def __str__(self):
        """Returns the "corpus format" text representation of the document."""
        buffer = io.StringIO()
        if self.attrs:
            print('<doc ' + ' '.join('{}="{}"'.format(k, v) for k, v
                                     in self.attrs.items()) + '>', file=buffer)
        else:
            print('<doc>', file=buffer)
        if self.meta:
            print('<meta>', file=buffer)
            for k, v in self.meta.items():
                print('<{0}>\n{1}\n<{0}>'.format(k, v), file=buffer)
            print('</meta>', file=buffer)
        if self.paragraphs:
            for p in self.paragraphs[:-1]:
                print('<p>\n{}\n</p>\n'.format(p), file=buffer)
            print('<p>\n{}\n</p>'.format(self.paragraphs[-1]), file=buffer)
        print('</doc>', file=buffer)
        return buffer.getvalue()

    def __repr__(self):
        """
        A short representation of the document: the URL, if available, else the
        first paragraph.
        """
        if self.attrs and 'url' in self.attrs:
            return 'Document(url: {})'.format(self.attrs['url'])
        elif self.meta and 'request' in self.meta:
            m = Document.uri_p.search(self.meta['request'])
            if m:
                return 'Document(url: {})'.format(m.group(1))
        # Could not get the URL
        if self.paragraphs:
            return 'Document("{}...")'.format(self.paragraphs[0])
        else:
            return 'Document()'


class SAXParser:
    """A SAX-like parser for the corpus format."""
    tag_p = re.compile(r'^<([^\s>]+)((?:\s+\S+="[^"]*")*)\s*>$')
    attrs_p = re.compile(r'(\S+)="([^"]*)"')

    def __init__(self, handler, attrs=True):
        """
        If attrs is False, does not create the tag attribute dictionary.
        The default is True.
        """
        self.handler = handler
        self.attrs = attrs

    def parseFile(self, filename):
        """Parses a file in corpus format. Calls parse() behind the scenes."""
        with openall(filename, 'rt') as inf:
            self.parse(inf, filename)

    def parse(self, corpus_stream, filename=None):
        """
        Parses a stream in corpus format. The filename parameter is optional,
        and is used for debugging.
        """
        stack = []
        fn_msg = 'in file {} '.format(filename) if filename else ''
        for line_no, line in enumerate(map(str.strip, corpus_stream), start=1):
            try:
                m = SAXParser.tag_p.match(line)
                if m:
                    tag, attrs = m.groups()
                    if not tag.startswith('/'):
                        stack.append(tag)
                        if self.attrs and attrs:
                            attrs = OrderedDict(m.groups() for m in
                                                SAXParser.attrs_p.finditer(attrs))
                        else:
                            attrs = None
                        self.handler.startElement(tag, attrs)
                    else:
                        if not stack or stack.pop() != tag[1:]:
                            msg = 'Closed unpaired tag {} {}on line {}'.format(
                                tag[1:], fn_msg, line_no)
                            raise ParseError(msg)
                        self.handler.endElement(tag[1:])
                else:
                    self.handler.line(line)
            except ParseError as pe:
                # Re-raise
                pe.args = (pe.args[0] + ' {}on line {}'.format(fn_msg, line_no),)
                raise pe
            except Exception as e:
                raise ParseError('Error {}on line {}'.format(fn_msg, line_no)) from e
        if stack:
            raise ParseError('Stream ended with unclosed tags {}'.format(
                '/'.join(stack)))


class CorpusHandler:
    def __init__(self, queue, attrs=True, meta=True, content=True, **kwargs):
        """For the arguments, consult the documentation for parse()."""
        self.queue = queue
        self.attrs = attrs
        self.meta = meta
        self.content = content
        self.meta_fields = dict(kwargs)

        self.need_meta = self.meta or self.meta_fields
        self.stack = set()
        self.doc = None
        self.skip_to_end = None
        self.meta_data = None

    def new_doc(self, attrs):
        return Document(attrs, OrderedDict() if self.need_meta else None,
                        [] if self.content else None)

    def startElement(self, tag, attrs):
        if not self.skip_to_end:
            if tag in self.stack:
                raise ParseError('Recursive tag declaration: <{}>'.format(tag))
            if 'doc' not in self.stack:
                if tag == 'doc':
                    self.stack.add('doc')
                    self.doc = self.new_doc(attrs if self.attrs else None)
                else:
                    raise ParseError('Unexpected tag <{}>'.format(tag))
            else:
                if tag == 'meta':
                    if self.need_meta:
                        self.stack.add('meta')
                    else:
                        self.skip_to_end = 'meta'
                elif tag == 'p':
                    self.stack.add('p')
                    self.doc.paragraphs.append([])
                elif 'meta' in self.stack:
                    if self.meta_fields[tag] if tag in self.meta_fields else self.meta:
                        # Meta field
                        self.meta_data = []
                else:
                    raise ParseError('Unexpected tag <{}>'.format(tag))

    def endElement(self, tag):
        if self.skip_to_end:
            if tag == self.skip_to_end:
                self.skip_to_end = None
            else:
                return

        self.stack.discard(tag)
        if tag == 'doc':
            self.queue.put(self.doc)
            self.doc = None
        elif tag == 'meta':
            if not self.content:
                self.skip_to_end = 'doc'
        elif tag == 'p':
            self.doc.paragraphs[-1] = '\n'.join(self.doc.paragraphs[-1])
        elif 'meta' in self.stack:
            # Finish any meta field
            if self.meta_data is not None:
                self.doc.meta[tag] = '\n'.join(self.meta_data)
                self.meta_data = None
        else:
            raise ParseError('Unexpected tag </{}>'.format(tag))

    def line(self, line):
        if 'p' in self.stack:
            self.doc.paragraphs[-1].append(line)
        elif self.meta_data is not None:
            self.meta_data.append(line)


def _parse(input, parse_fn, attrs=True, meta=True, content=True, **meta_fields):
    """Common background function for parse and parse_file."""
    queue = Queue()
    ch = CorpusHandler(queue, attrs, meta, content, **meta_fields)
    sp = SAXParser(ch, attrs)

    with cf.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(parse_fn, sp, input)
        while future.running():
            try:
                yield queue.get(timeout=0.1)
            except Empty:
                pass
        return future.result()


def parse(corpus_stream, attrs=True, meta=True, content=True, **meta_fields):
    """
    Enumerates Documents in a text stream in the corpus format. The rest of the
    parameters specify what parts of the documents to keep:
    - attrs: the attributes of the doc tag;
    - meta: the meta fields;
    - content: the textual content;
    - meta_fields: can be used to include / exclude specific meta fields. This
                   setting takes precedence over the general meta argument.
    """
    yield from _parse(corpus_stream, SAXParser.parse,
                      attrs, meta, content, **meta_fields)


def parse_file(corpus_file, attrs=True, meta=True, content=True, **meta_fields):
    """
    Enumerates Documents in a text file in the corpus format. The arguments
    behave the same as in parse().
    """
    yield from _parse(corpus_file, SAXParser.parseFile,
                      attrs, meta, content, **meta_fields)
