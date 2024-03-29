#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Removes boilerplate from WARC segments and converts them to the corpus's
semi-XML format.
"""

from argparse import ArgumentParser
from collections import namedtuple
from collections.abc import Generator
from fnmatch import fnmatch
import functools
import gzip
import io
from itertools import chain
import logging
from multiprocessing import Pool
import os
from pathlib import Path
import re
import xml.sax.saxutils

from multiprocessing_logging import install_mp_handler
import warc
from warc import WARCRecord

from cc_corpus.boilerplate import (
    BoilerplateRemover, JustextNonRemover, JustextRemover, TrafilatureRemover
)
from cc_corpus.corpus import Document
from cc_corpus.content_conversion import convert
from cc_corpus.utils import consume, openall, otqdm, unquote_inf


IndexTuple = namedtuple('IndexTuple', ['index', 'domain', 'url', 'warc',
                                       'offset', 'length', 'status', 'mime'])
whitelist = set()
matcher3dots = re.compile(r'^\w+\.\.\.$')
matcher3punct = re.compile(r'.*[^\w\s]{3,}')


class IndexWarcReader:
    """
    Reads index files and the files with the downloaded WARC segments in
    parallel.

    .. note::
    In the class description, "WARC file" means "a file that contains
    downloaded WARC segments from Common Crawl". One difference to a CC WARC
    file is that these files only contain the responses.

    .. todo::
    This class is a not really a "reader", as it basically does everything:
    it reads the warc and index files, removes boilerplate and writes the
    output file as well. These functions should be split into separate classes
    / (Python) functions.
    """
    def __init__(self, warc_dir: Path, output_dir: Path,
                 remover: BoilerplateRemover, token_filtering: bool,
                 paragraph_patterns: Path):
        """
        Creates a new IndexWarcReader with the specified index and warc
        directories. These must be compatible, i.e. the WARC directory should
        contain the downloaded segments corresponding to the files in the
        index directory.

        warc_dir: the directory with the WARC files.
        output_dir: the output directory
        remover: the boilerplate removal algorithm wrapper.
        """
        self.warc_dir = warc_dir
        self.output_dir = output_dir
        self.remover = remover
        self.token_filtering = token_filtering
        if paragraph_patterns:
            self.paragraph_pattern = self.read_patterns(paragraph_patterns)
        else:
            self.paragraph_pattern = None
        # This is the output stream
        self.outf = None

    @staticmethod
    def read_patterns(pattern_file: str | Path) -> list[re.Pattern]:
        """Reads the paragraph patterns."""
        with open(pattern_file, 'rt') as inf:
            patterns = [line.rsplit('\t', 1)[0] for line in map(str.strip, inf)]
        return re.compile('|'.join(f'(?:{p})' for p in patterns), re.I)

    def read(self, index_file):
        """
        Enumerates the index and WARC records in the specified index file and
        the matching WARC files. Calls the specified function with the two
        records.
        """
        index_iter = self.index_lines(index_file)
        warc_iter = self.warc_records(index_file)
        index_id = 0
        for warc_record in warc_iter:
            url = warc_record['WARC-Target-URI']
            for index in index_iter:
                index_id += 1
                if unquote_inf(index.url) == unquote_inf(url):
                    self.process_record(index_id, index, warc_record)
                    break
            else:
                raise ValueError(f'URL {url} was not found in index')

    def process_record(self, index_id: int, index, warc: WARCRecord):
        """Writes the output file."""
        # We need the WARC header...
        bio = io.BytesIO()
        warc.header.write_to(bio)
        # And the HTML header and text as well. jusText can handle bytes
        # header, text = warc.payload.read().split(b'\r\n\r\n', maxsplit=1)
        try:
            header, chunks = convert(warc)
            paragraphs = list(chain.from_iterable(
                self.remover.remove(chunk, index.url) for chunk in chunks
            ))
        # TypeError JusText bug, AssertionError, ValueError JusText bug on comment...
        except:  # noqa
            # Do not distinguish between the different errors
            logging.exception('Exception removing boilerplate from record '
                              f'{index} on line {index_id} ({index.url}).')
            return

        # Escape paragraph for parsable XML
        escaped_paragraphs = [
            ' '.join(xml.sax.saxutils.escape(paragraph).split()) for
            paragraph in paragraphs
        ]
        if self.token_filtering:
            cleared_paragraphs = [filter_tokens(paragraph)
                                  for paragraph in escaped_paragraphs]
        else:
            cleared_paragraphs = escaped_paragraphs
        if self.paragraph_pattern:
            cleared_paragraphs = [p for p in cleared_paragraphs
                                  if not self.paragraph_pattern.search(p)]
        if len(cleared_paragraphs) == 0:
            logging.info(f'Nothing\'s left of {index.url} '
                         'after boilerplate removal')
            return

        # We extract the url from the metadata and use it as the id.
        index_dict = index._asdict()
        url = index_dict.pop('url')
        document = Document(
            id=url,
            attrs=index_dict,
            http_meta={"request": bio.getvalue().decode('utf-8').strip(),
                       "response": header.decode('utf-8').strip()},
            paragraphs=cleared_paragraphs
        )

        # This extracts the relevant metadata from the http response part into
        # the attrs (but keeps them in the http response part as well):
        document.extract_http_metadata()

        # print(document, file=self.outf)
        print(document.to_json(), file=self.outf)

        if index_id % 1000 == 0:
            logging.info(f'Removed boilerplate from {index.url} ({index_id})')
        logging.debug(f'Removed boilerplate from {index.url} ({index_id})')

    def index_lines(self, index_file):
        """Enumerates the lines of the index file into IndexTuples."""
        # module = gzip if index_file.suffix == '.gz' else io
        with openall(index_file, 'rt') as inf:
            for line in inf:
                yield IndexTuple(index_file.stem, *line.strip().split())

    def warc_records(self, index_file) -> Generator[WARCRecord]:
        """
        Enumerates WARC records from the WARC files that correspond to
        index_file.
        """
        try:
            for warc_file in self.warc_files_for_index(index_file):
                output_file = warc_file.name.replace('.warc.', '.jsonl.')
                with gzip.open(self.output_dir / output_file,
                               'wt', encoding='utf-8') as outf:
                    self.outf = outf
                    for record in warc.open(str(warc_file)):
                        yield record
        finally:
            self.outf = None

    def warc_files_for_index(self, index_file):
        """Returns all WARC files that correspond to an index file."""
        pattern = index_file.stem + '_*.warc*'
        return sorted([f for f in self.warc_dir.iterdir()
                       if fnmatch(f.name, pattern)])


def parse_arguments():
    parser = ArgumentParser(
        description='Removes boilerplate from WARC segments and converts '
                    'them to the corpus\'s semi-XML format.')
    parser.add_argument('--index-dir', type=Path, required=True,
                        help='the index directory')
    parser.add_argument('--warc-dir', '-i', type=Path, required=True,
                        help='the directory with the WARC segments')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the output directory')
    parser.add_argument('--boilerplate-tool', '-b', default='trafilatura',
                        choices=['dummy', 'justext', 'trafilatura'],
                        help='the boilerplate removal algorithm to use '
                             '(default: trafilatura).')
    parser.add_argument('--boilerplate-language', '-l', default='Hungarian',
                        help='boilerplate removal language (default: Hungarian)')
    parser.add_argument('--token-filtering', '-t', action='store_true',
                        help='do token level filtering')
    parser.add_argument('--token-whitelist', '-tw', type=Path,
                        help='the file containing whitelisted tokens.')
    parser.add_argument('--paragraph-patterns', '-p', type=Path,
                        help='a list of patterns that can be used to filter paragraphs '
                             'remained after boilerplate removal.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {num_procs}')
    return args


def good_token(token):
    # A token is good if:
    # * It is on the whitelist
    # * It is a word ending in ...
    # * It does not have 3 consecutive punctuation marks
    if token in whitelist:
        return True
    if matcher3dots.match(token):
        return True
    if matcher3punct.match(token):
        return False
    return True


def filter_tokens(paragraph: str):
    filtered_tokens = [token for token in paragraph.split()
                       if good_token(token)]
    return " ".join(filtered_tokens)


def process(index_file: Path, warc_dir: Path,
            output_dir: Path, remover: BoilerplateRemover,
            token_filtering: bool, paragraph_patterns: Path):
    """Basically just calls :meth:`IndexWarcReader.read`."""
    logging.info(f'Processing {index_file}...')
    reader = IndexWarcReader(warc_dir, output_dir, remover, token_filtering,
                             paragraph_patterns)
    try:
        reader.read(index_file)
    except:  # noqa
        logging.exception(f'Exception in file {index_file}')
    else:
        logging.info(f'Processed {index_file}...')


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    logging.getLogger('trafilatura').setLevel(logging.ERROR)
    install_mp_handler()

    if args.token_filtering and args.token_whitelist:
        with open(args.token_whitelist, 'rt') as list_file:
            whitelist.update(map(str.strip, list_file))

    try:
        if args.boilerplate_tool == 'justext':
            cls = JustextRemover
        elif args.boilerplate_tool == 'dummy':
            cls = JustextNonRemover
        else:
            cls = TrafilatureRemover
        remover = cls(args.boilerplate_language)
    except ValueError:
        logging.error(
            f'Invalid stopword language {args.boilerplate_language}.')
        exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    os.nice(20)  # Play nice

    args.index_dir.mkdir(parents=True, exist_ok=True)
    index_files = sorted(args.index_dir.iterdir())
    logging.debug(f'{index_files=}')

    fn = functools.partial(process, warc_dir=args.warc_dir,
                           output_dir=args.output_dir,
                           remover=remover,
                           token_filtering=args.token_filtering,
                           paragraph_patterns=args.paragraph_patterns)

    with Pool(args.processes) as pool:
        consume(otqdm(pool.imap_unordered(fn, index_files),
                      f'Removing boilerplate with {args.boilerplate_tool} '
                      f'from {args.warc_dir.name}...', total=len(index_files)))
        pool.close()
        pool.join()

    logging.info('Done.')


if __name__ == '__main__':
    main()
