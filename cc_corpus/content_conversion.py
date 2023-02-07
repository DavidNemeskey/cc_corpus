#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The functions in this module convert the content in WARC files into regular
HTML that boilerplate removers can consume based on their content types.
"""

import atoma
# from bs4 import BeautifulSoup
from warc import WARCRecord

from cc_corpus.utils import is_empty


def convert_atom(text: bytes) -> list[str]:
    """Converts an atom feed."""
    def not_empty(elem) -> bool:
        return elem is not None and not is_empty(elem.value)

    feed = atoma.parse_atom_bytes(text)
    chunks = []
    for e in feed.entries:
        # Only keep an item if it contains meaningful text
        if not_empty(e.summary) and not_empty(e.content):
            item_chunks = []
            if not_empty(e.title):
                # TODO JusText always filters this out, fix that...
                item_chunks.append(f'<p>{e.title.value}</p>')
            if not_empty(e.summary):
                item_chunks.append(e.summary.value)
            if not_empty(e.content):
                item_chunks.append(e.content.value)
            chunks.append('\n\n'.join(item_chunks))
    return chunks


def convert_rss(text: bytes) -> list[str]:
    """Converts an RSS feed."""
    def compose_chunk(*pieces) -> str:
        """
        Composes _pieces_ into a single text chunk, adding each only if they
        are not ``None``.
        """
        return '\n\n'.join(f'<p>{piece}</p>' for piece in pieces if piece)

    feed = atoma.parse_rss_bytes(text)
    # Only keep an item if it contains meaningful text
    chunks = ['\n\n'.join(
        compose_chunk(item.title, item.description) for item in feed.items
        if not is_empty(item.title) and not is_empty(item.description)
    )]
    if chunks or not is_empty(feed.description):
        return [compose_chunk(feed.title, feed.description)] + chunks
    else:
        return []


def convert(record: WARCRecord):
    content_type = record["WARC-Identified-Payload-Type"]
    header, text = record.payload.read().split(b'\r\n\r\n', maxsplit=1)
    if content_type == 'application/atom+xml':
        chunks = convert_atom(text)
    elif content_type == 'application/rss+xml':
        chunks = convert_rss(text)
    else:
        chunks.append(text)

    return header, [chunk for chunk in chunks if chunk and chunk.strip()]
    # return header, [str(BeautifulSoup(chunk)) for chunk in chunks
    #                 if chunk and chunk.strip()]
