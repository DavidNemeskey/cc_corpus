#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Contains code that works with the semi-XML format of the corpus."""

from collections import OrderedDict
import io
import re

from cc_corpus.utils import openall


class ParseError(Exception):
    """Raised if the file or stream is not in the corpus XML format."""
    pass


class Document:
    uri_p = re.compile('^WARC-Target-URI: (.+?)$', re.M)

    def __init__(self, attrs=True, meta=True, content=True):
        self.attrs = OrderedDict() if attrs else None
        self.meta = OrderedDict() if meta else None
        self.paragraphs = [] if content else None

    def content(self):
        """
        Returns the textual content of the document, without any markup (i.e.
        <p> tags).
        """
        return '\n'.join(self.paragraphs)

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
            print('<p>\n{}\n</p>'.format(p), file=buffer)
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
    tag_p = re.compile(r'^<([^\s>]+)(?:\s+\S+="[^"]*")*\s*>$')
    attrs_p = re.compile(r'(\S)+="([^"]*)"')

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
        for line_no, line in enumerate(map(str.strip, corpus_stream), start=1):
            m = SAXParser.tag_p.match(line)
            if m:
                tag = m.group(1)
                if not tag.startswith('/'):
                    stack.append(tag)
                    if self.attrs:
                        attrs = OrderedDict()
                    else:
                        attrs = None
                    self.handler.startElement(tag, attrs)
                else:
                    if not stack or stack.pop() != tag[1:]:
                        fn = 'in file {} '.format(filename) if filename else ''
                        msg = 'Closed unpaired tag {} {}on line {}'.format(
                            tag[1:], fn, line_no)
                        raise ParseError(msg)
                    self.handler.endElement(tag[1:])
            else:
                self.handler.line(line)
        if stack:
            raise ParseError('Stream ended with unclosed tags {}'.format(
                '/'.join(stack)))
        self.handler.endStream()


class CorpusHandler:
    def __init__(self, queue, attrs=True, meta=True, content=True, **kwargs):
        """For the arguments, consult the documentation for parse()."""
        self.queue = queue
        self.attrs = attrs
        self.meta = meta
        self.content = content
        self.meta_fields = dict(kwargs)

        self.need_meta = self.meta or self.meta_fields

    def startElement(self, tag, attrs):
        pass

    def endElement(self, tag):
        pass

    def line(self, line):
        pass

    def endStream(self):
        pass


def _parse(input, parse_fn, attrs=True, meta=True, content=True, **kwargs):
    """Common background function for parse and parse_file."""
    pass


def parse(corpus_stream, attrs=True, meta=True, content=True, **kwargs):
    """
    Enumerates Documents in a text stream in the corpus format. The rest of the
    parameters specify what parts of the documents to keep:
    - attrs: the attributes of the doc tag;
    - meta: the meta fields;
    - content: the textual content;
    - kwargs: can be used to include / exclude specific meta fields. This
              setting takes precedence over the generic meta argument.
    """
    yield from _parse(corpus_stream, SAXParser.parse,
                      attrs, meta, content, **kwargs)


def parse_file(corpus_file, attrs=True, meta=True, content=True, **kwargs):
    """
    Enumerates Documents in a text file in the corpus format. The arguments
    behave the same as in parse().
    """
    yield from _parse(corpus_file, SAXParser.parseFile,
                      attrs, meta, content, **kwargs)
