#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MIME-related utility functions."""

import re
from typing import Optional, Union

from bs4 import BeautifulSoup
import magic


mime_patterns = {
    'txt': re.compile('^text/plain$'),
    'html': re.compile('html')
}


def check_mime(
    data: Union[bytes, str], length: int = 2048
) -> tuple[Optional[str], str]:
    """
    Determines the "mime type" (txt, html, etc.) of a document.

    :param data: the content whose mime is in question.
    :param length: how much of the content to use for mime detection. The
                   default is 2048 (bytes / codepoints).
    :return: a 2-tuple, where the second field is the mime type, the first one
             contains our "simplified mime types" (for now, "txt" or "html").
             It will be ``None`` if the document does not have a mime type
             that we can handle.
    """
    mime = magic.from_buffer(data[:length], mime=True)
    for mime_type, p in mime_patterns.items():
        if p.search(mime):
            return (mime_type, mime)
    else:
        return (None, mime)


def normalize_content(content: Union[bytes, str]) -> Optional[Union[bytes, str]]:
    """
    Normalizes the content based on the simplified MIME type (see above,
    :func:`check_mime`) by loading it into BeautifulSoup and then converting it
    (back) to string. By type:

    - HTML pages: returns them (very close to) as-is
    - text: returns them enclosed in ``<html><body>...</body></html>``.

    There are two reasons for doing this. First, some pages downloaded from
    CC do not include the full HTML boilerplate (e.g. ``<html>`` or ``<body>``
    tag is missing); and second, the MIME detection is not very reliable, and
    it might identify HTML fragments as ``text/plain``.

    .. note::
        While we might support ``bytes`` output for other MIME types down the
        line, for HTML and text content, the return type will be ``str``.
    """
    mt, _ = check_mime(content)
    if mt in ['txt', 'html'] :
        bs = BeautifulSoup(content)
        if bs.body is not None:
            return str(bs)
        else:
            return None
    elif mt is None:
        return None
    else:
        raise NotImplementedError(f'Accepted simplified mime {mt} not handled '
                                  'by normalize_content()')
