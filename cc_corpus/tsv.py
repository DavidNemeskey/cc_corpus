#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Contains code that works with the tsv format output by emtsv."""


class Unit:
    """
    A unit of text structure: a sentence, paragraph or document. It consists
    of a list of comments and a list of other units of strings. Both lists
    may be empty.
    """
    def __init__(self, comments=None, content=None):
        self.comments = comments or []
        self.content = content or []

    def __str__(self):
        return '\n'.join(self.comments) + '\n' + '\n'.join(str(unit) for unit in self.content))



class Document:
    def __init__(self, header=None, paragraphs=None):
        self.header = header
        self.doc = paragraphs


def _parse(input, header=True):
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
        while True:
            try:
                yield queue.get_nowait()
            except Empty:
                break
        return future.result()

