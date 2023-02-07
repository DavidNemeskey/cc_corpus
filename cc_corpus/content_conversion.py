#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The functions in this module convert the content in WARC files into regular
HTML that boilerplate removers can consume based on their content types.
"""

from typing import Any, Optional

import atoma
# from bs4 import BeautifulSoup
from warc import WARCRecord


def compose_chunk(*pieces: Optional[Any]) -> str:
    """
    Composes _pieces_ into a single text chunk, adding each only if they are
    not ``None``.
    """
    return '\n\n'.join(f'<p>{piece}</p>' for piece in pieces if piece)


def convert(record: WARCRecord):
    content_type = record["WARC-Identified-Payload-Type"]
    header, text = record.payload.read().split(b'\r\n\r\n', maxsplit=1)
    chunks = []
    if content_type == 'application/rss+xml':
        feed = atoma.parse_rss_bytes(text)
        if (chunk := compose_chunk(feed.title, feed.description)):
            chunks.append(chunk)
        for item in feed.items:
            if (chunk := compose_chunk(item.title, item.description)):
                chunks.append(chunk)
    elif content_type == 'application/atom+xml':
        feed = atoma.parse_atom_bytes(text)
        for e in feed.entries:
            feed_chunks = []
            if e.title is not None:
                # TODO JusText always filters this out, fix that...
                feed_chunks.append(f'<p>{e.title.value}</p>')
            if e.summary is not None:
                feed_chunks.append(e.summary.value)
            if e.content is not None:
                feed_chunks.append(e.content.value)
            chunks.append('\n\n'.join(feed_chunks))
    else:
        chunks.append(text)

    return header, [chunk for chunk in chunks if chunk and chunk.strip()]
    # return header, [str(BeautifulSoup(chunk)) for chunk in chunks
    #                 if chunk and chunk.strip()]
