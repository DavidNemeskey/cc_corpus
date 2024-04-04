#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Deduplicates the urls in the index."""

from argparse import ArgumentParser
from functools import partial, reduce
from itertools import starmap
import logging
import operator
from pathlib import Path
import re
from typing import Any, Callable, Dict, List, Set, Union

from cc_corpus.utils import notempty, openall, otqdm, Stats


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', type=Path, required=True,
                        help='the index directory')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the output directory')
    parser.add_argument('--skip-urls', '-s', type=Path, default=None,
                        help='a file with the list of URLs to skip (i.e. '
                             'drop). Typically, these are URLs already '
                             'downloaded in a previous batch.')
    parser.add_argument('--hash', action='store_true',
                        help='use hashes to store the URLs to skip. Uses less '
                             'memory, but there is a chance for hash collision '
                             '(though not high: all the Hungarian URLs of '
                             '2017-2018 failed to produce one).')
    parser.add_argument('--keep', '-k', choices=['latest', 'biggest'],
                        default='biggest',
                        help='which occurrence to keep. Default: biggest.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    return args


Url = Union[str, int]
UrlList = List[Url]
UrlSet = Set[Url]
UrlFn = Callable[[str], Url]


def read_urls(urls_file: str, url_fn: UrlFn) -> UrlSet:
    """
    Reads URLS from the file ``urls_file``, one per line. The URLs are
    returned in a set; either as a string or as a hash value,
    depending on what the transformation function ``url_fn`` does.

    Using hashes instead of the full url can conserve memory. In our
    experiments, we have not encountered collisions yet.

    Note: no normalization of URLs for now, as the library that I tried was
    slooooooooow. This also means that versions of the same URL might stay in
    the index, including http / https versions. Hopefully,
    document deduplication will take care of this.
    """
    with openall(urls_file) as inf:
        logging.info(f'Loading urls from {urls_file}...')
        urls = set()
        no_urls = 0
        for no_urls, url in enumerate(map(str.strip, inf), start=1):
            urls.add(url_fn(url))
            if no_urls % 5000000 == 0:
                logging.debug('Loaded {} urls from {}...'.format(
                    no_urls, urls_file))
        logging.info('Loaded {} urls from {}; {} unique.'.format(
            no_urls, urls_file, len(urls)))
        return urls


file_name_p = re.compile(r'(\d{4}-\d{2}-\d+).gz$')


class IndexRecord():
    """
    A data class that contains all information about a URL relevant for
    deduplication.
    """
    # TODO: I have a generic solution for this, don't I?
    def __init__(self, warc: str, offset: Union[str, int],
                 length: Union[str, int], index: str = None):
        """
        :param warc: the name of the warc file the file is in (which contains
                     the date)
        :param offset: the offset of the document in the warc file
        :param length: the length of the document
        :param index: the index file
        """
        self.warc = warc
        self.offset = int(offset)
        self.length = int(length)
        self.index = index

    def __eq__(self, other):
        """
        Equality check. All fields must match with the exception of
        :attr:`index`, which might be ``None``, which matches everything.
        """
        if other:
            same = self.warc == other.warc
            same = same and self.offset == other.offset
            same = same and self.length == other.length  # Necessary?
            if self.index and other.index:
                same = same and self.index == other.index
            return same
        else:
            return False

    def __repr__(self):
        return '({}, {}, {} in {})'.format(
            self.warc, self.offset, self.length, self.index)


UrlIndexDict = Dict[Url, IndexRecord]


# -------------------------------- Collection ----------------------------------


CollectStats = Stats.create(
    'overwrite', 'new', 'reject', 'skipped')  # type: Any


def uniq_record(url: Url, record: IndexRecord, uniqs: UrlIndexDict,
                keep: str) -> str:
    """
    Uniq's a record. Returns whether the record is unique (not in uniqs), or is
    the representative of its URL (i.e. it is the latest / biggest). Returns
    a string that describes what happened to the URL (``reject`` / ``new``
    / ``overwrite``).
    """
    if url in uniqs:
        other_record = uniqs[url]
        if keep == 'latest':
            if record.warc <= other_record.warc:
                return 'reject'
        else:
            if record.length <= other_record.length:
                return 'reject'
        ret = 'overwrite'
    else:
        ret = 'new'

    uniqs[url] = record
    return ret


def file_to_dict(index_file: str, keep: str, skip_urls: UrlList, url_fn: UrlFn,
                 global_uniqs: UrlIndexDict):
    """
    Collects all URLs from an index file and deduplicats in two phrases:

    1. First, the index lines / URLs are deduplicated inside the file, in case
       an index file contains the same URL twice (not likely, but who knows?)
    2. Then, the URLs are deduplicated across all files / processes. To achieve
       this, we use a shared dictionary kept in-memory.

    :param index_file: the name of the input file
    :param keep: which record should win: the ``latest`` or ``biggest``
    :param skip_urls: the list of URLs to skip (e.g. because we already have them)
    :param url_fn: the URL transformation function to apply to each URL. In the
                   scope of this program, this is either hashing or nothing.
    :param global_uniqs: the shared dictionary of unique URLs
    """
    logging.info('Collecting URLs from {}...'.format(index_file))
    stats = CollectStats()
    try:
        # In-file deduplication
        with openall(index_file, 'rt') as inf:
            uniqs = {}  # type: UrlIndexDict
            file_id = file_name_p.search(str(index_file)).group(1)
            line_no = 0
            for line_no, line in enumerate(map(str.strip, inf), start=1):
                try:
                    # After filtering, the line is prepended with the "domain"
                    # I skip that and extract it myself
                    url, warc, offset, length = line.split()[:7][-6:-2]
                    if url in skip_urls:
                        stats.skipped += 1
                    else:
                        record = IndexRecord(warc, offset, length, file_id)
                        uniq_record(url_fn(url), record, uniqs, keep)
                except:
                    logging.exception(
                        'Exception in file {}:{}'.format(index_file, line_no))
                    break

            if line_no == 0:
                logging.info('File {} is empty; returning...'.format(index_file))
                return
            logging.info('Self-deduplicated {} URLs in {} to {}; skipped {}.'.format(
                line_no, index_file, len(uniqs), stats.skipped))

        # Global deduplication
        for url, record in uniqs.items():
            stats[uniq_record(url, record, global_uniqs, keep)] += 1

        logging.info('Cross-deduplicated {} URLs in {} to '
                     '{} (overwrote {}; {} new).'.format(
                         len(uniqs), index_file, stats.new + stats.overwrite,
                         stats.overwrite, stats.new))
    except:
        logging.exception(
            'Exception in file {}'.format(index_file))


# -------------------------------- Filtering ----------------------------------


FilterStats = Stats.create(
    'old_files', 'new_files', 'old_urls', 'new_urls')  # type: Any


def filter_file(input_file, output_file, uniqs, url_fn: UrlFn) -> FilterStats:
    """
    Filters an index file; i.e. drops all duplicate URLs.
    :param input_file: the input index file
    :param output_file: the output index file
    :param uniqs: the shared dictionary of unique URLs
    :param url_fn: the URL transformation function to apply to each URL. In the
                   scope of this program, this is either hashing or nothing.
    """
    logging.info('Filtering file {}...'.format(input_file))
    stats = FilterStats(old_files=1)
    with openall(input_file, 'rt') as inf, notempty(openall(output_file, 'wt')) as outf:
        line_no = lines_printed = 0
        for line_no, line in enumerate(map(str.strip, inf), start=1):
            try:
                url, warc, offset, length = line.split()[:7][-6:-2]
                record = IndexRecord(warc, offset, length)
                if record == uniqs.get(url_fn(url)):
                    print(line, file=outf)
                    lines_printed += 1
            except:
                logging.exception(
                    'Exception in file {}:{}'.format(input_file, line_no))

    if line_no:
        logging.info('Kept {} URLs out of {} in {}.'.format(
            lines_printed, line_no, input_file))
        stats.old_urls = line_no
    else:
        logging.info('File {} was empty.'.format(input_file))

    if lines_printed:
        stats.new_files = 1
        stats.new_urls = lines_printed
    return stats


def hash_normalize(url: str) -> Url:
    """
    This is the URL transform function that converts a URL to a hash.

    It cannot be a lambda, because those cannot be pickled.
    """
    return hash(url)


def noop_normalize(url: str) -> Url:
    """This URL transform function just returns the URL as-is."""
    return url


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    url_fn = hash_normalize if args.hash else noop_normalize
    skip_urls = set()
    if args.skip_urls:
        if args.skip_urls.is_dir():
            for url_file in args.skip_urls.iterdir():
                urls_set = read_urls(url_file, url_fn)
                skip_urls.update(urls_set)
        else:
            skip_urls = read_urls(args.skip_urls, url_fn) if args.skip_urls else set()

    input_files = [file for file in args.input_dir.iterdir()]
    basenames = [file.name for file in input_files]

    logging.info('Collected {} index files from {}.'.format(
        len(input_files), args.input_dir))

    # Collect the representative records for all URLs
    global_uniqs = {}

    for input_file in otqdm(input_files, 'Collecting URLs from index...'):
        file_to_dict(input_file, args.keep, skip_urls, url_fn, global_uniqs)
    logging.info('Total number of unique URLs found: {}'.format(
        len(global_uniqs)))

    # And filter from the files all non-representative URLs
    args.output_dir.mkdir(parents=True, exist_ok=True)
    tasks = otqdm(
        zip(input_files, [args.output_dir / f for f in basenames]),
        f'Removing duplicates from {args.input_dir}...', total=len(input_files)
    )

    f = partial(filter_file, uniqs=global_uniqs, url_fn=url_fn)
    sum_stats = reduce(operator.add, starmap(f, tasks), FilterStats())

    logging.info(
        'Done filtering index: index files {} -> {}, URLs {} -> {}.'.format(
            sum_stats.old_files, sum_stats.new_files,
            sum_stats.old_urls, sum_stats.new_urls)
    )


if __name__ == '__main__':
    main()
