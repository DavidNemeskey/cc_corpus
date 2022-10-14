#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Boilerplate removal algorithm wrappers."""

from abc import ABC, abstractmethod
import logging
from xml.sax import parseString
from xml.sax.handler import ContentHandler
from xml.etree.ElementTree import ParseError

import justext
import trafilatura


class BoilerplateRemover(ABC):
    """Base class for boilerplate removal algorithm wrappers."""
    def __init__(self, language: str):
        self.language = language

    @abstractmethod
    def remove(self, html: bytes, url: str) -> list[str]:
        """
        Removes boilerplate from the bytestring _html_.

        :param html: the page itself.
        :param url: the URL; for logging purposes.
        :return: the list of content paragraphs found.
        .. todo:: retain information about the type of text (<p>, <li>, etc.)
        """
        pass


class JustextRemover(BoilerplateRemover):
    """Wrapper for JusText."""
    def __init__(self, language):
        super().__init__(language)
        logging.debug(f'Acquiring stopword list for {language}...')
        self.stopwords = justext.get_stoplist(language)
        logging.debug(f'Number of stopwords: {len(self.stopwords)}')

    def remove(self, html: bytes, url: str):
        return [p for p in justext.justext(html, self.stopwords)
                if not p.is_boilerplate]


class TrafilatureRemover(BoilerplateRemover):
    """Wrapper for Trafilature's boilerplate removal function."""
    def remove(self, html: bytes, url: str):
        xml = trafilatura.extract(html, url, output_format='xml',
                                  target_language=self.language,
                                  include_tables=False)
        h = TrafilaturaHandler()
        if xml is None:
            # How is this even possible?
            # logging.info('Trafilatura returned None.')
            return []
        else:
            parseString(xml, h)
            return h.result()


class TrafilaturaHandler(ContentHandler):
    """SAX content handler for the type of XML returned by Trafilatura."""
    DATA_TAGS = {'p', 'list', 'head', 'hi'}

    def __init__(self):
        self.curr_tags = []
        self.tag_content = []
        self.doc_content = []

    def startElement(self, tag, attrs):
        self.curr_tags.append(tag)
        self.tag_content = []

    def endElement(self, tag):
        if self.curr_tags[-1] != tag:
            raise ParseError(f'Closing tag {tag} does not match opening tag '
                             f'{self.curr_tags[-1]}.')
        elif self.tag_content:
            self.doc_content.append(' '.join(self.tag_content))
        self.curr_tags.pop()

    def characters(self, content):
        if self.curr_tags[-1] in self.DATA_TAGS:
            if (text := content.strip()):
                self.tag_content.append(text)

    def result(self):
        """Returns the output of the parsing of a single document."""
        return self.doc_content
