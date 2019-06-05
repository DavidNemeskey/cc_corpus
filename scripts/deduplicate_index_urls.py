#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Deduplicates the urls in the index."""

from argparse import ArgumentParser
from collections import Counter
from functools import partial
import logging
from multiprocessing import Manager, Pool
from multiprocessing.synchronize import RLock
import os
import os.path as op
import re
from typing import Any, Callable, Dict, Set, Union

from multiprocessing_logging import install_mp_handler
from url_normalize import url_normalize

from cc_corpus.utils import notempty, openall, Stats


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the index directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--skip-urls', '-s', metavar='URL_FILE', default=None,
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
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1)')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


Url = Union[str, int]
UrlSet = Set[Url]
UrlFn = Callable[[str], Url]


def read_urls(urls_file: str, url_fn: UrlFn) -> UrlSet:
    """
    Reads URLS from the file ``urls_file``, one per line. The URLs are
    normalized and returned in a set; either as a string or as a hash value,
    depending on what the transformation function ``url_fn`` does.

    Using hashes instead of the full url can conserve memory. In our
    experiments, we have not encountered collisions yet.

    Note: normalization keeps the separate http / https versions. Hopefully,
    document deduplication will take care of this.
    """
    with openall(urls_file) as inf:
        urls = set()
        for no_urls, url in enumerate(map(str.strip, inf), start=1):
            urls.add(url_fn(url))
            if no_urls % 1000000 == 0:
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


def uniq_record(url: Url, record: IndexRecord, uniqs: UrlIndexDict,
                keep: str) -> str:
    """
    Uniq's a record. Returns whether the record is uniq (not in uniqs), or is
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


def file_to_dict(index_file: str, keep: str, skip_urls: UrlSet, url_fn: UrlFn,
                 global_uniqs: UrlIndexDict, lock: RLock):
    """
    Collects all URLs from an index file and deduplicats in two phrases:

    1. First, the index lines / URLs are deduplicated inside the file, in case
       an index file contains the same URL twice (not likely, but who knows?)
    2. Then, the URLs are deduplicated across all files / processes. To achieve
       this, we use a shared dictionary kept in-memory.

    :param index_file: the name of the input file
    :param keep: which record should win: the ``latest`` or ``biggest``
    :param skip_urls: the set of URLs to skip (e.g. because we already have them)
    :param url_fn: the URL transformation function to apply to each URL. In the
                   scope of this program, this includes normalization and
                   optional hashing
    :param global_uniqs: the shared dictionary of unique URLs
    :param lock: regulates access to ``global_uniqs``
    """
    logging.info('Collecting URLs from {}...'.format(index_file))
    try:
        # In-file deduplication
        with openall(index_file, 'rt') as inf:
            uniqs = {}  # type: UrlIndexDict
            file_id = file_name_p.search(index_file).group(1)
            for line_no, line in enumerate(map(str.strip, inf), start=1):
                try:
                    # After filtering, the line is prepended with the "domain"
                    # I skip that and extract it myself
                    url, warc, offset, length = line.split()[:7][-6:-2]
                    record = IndexRecord(warc, offset, length, file_id)
                    uniq_record(url_fn(url), record, uniqs, keep)
                except:
                    logging.exception(
                        'Exception in file {}:{}'.format(index_file, line_no))
                    break
            logging.info('Self-deduplicated {} URLs in {} to {}.'.format(
                line_no, index_file, len(uniqs)))

        # Global deduplication
        with lock:
            counts = Counter(uniq_record(url, record, global_uniqs, keep)
                             for url, record in uniqs.items())
            num_uniqs = counts['new'] + counts['overwrite']

        logging.info('Cross-deduplicated {} URLs in {} to '
                     '{} (overwrote {}; {} new).'.format(
                         len(uniqs), index_file, num_uniqs,
                         counts['overwrite'], counts['new']))
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
                   scope of this program, this includes normalization and
                   optional hashing
    """
    logging.info('Filtering file {}...'.format(input_file))
    stats = FilterStats(old_files=1)
    with openall(input_file, 'rt') as inf, notempty(openall(output_file, 'wt')) as outf:
        lines_printed = 0
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
        logging.info('Kept {} URLs out of {} in {}.'.format(
            lines_printed, line_no, input_file))
        stats.old_urls = line_no
        if lines_printed:
            stats.new_files = 1
            stats.new_urls = lines_printed
    return stats


def hash_normalize(url: str) -> Url:
    """
    This is the URL transform function that converts a URL to a hash.

    It cannot be a lambda, because those cannot be pickled.
    """
    return hash(url_normalize(url))


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    url_fn = hash_normalize if args.hash else url_normalize
    skip_urls = read_urls(args.skip_urls, url_fn) if args.skip_urls else set()

    basenames = os.listdir(args.input_dir)
    input_files = [op.join(args.input_dir, f) for f in basenames]

    logging.info('Collected {} index files from {}.'.format(
        len(input_files), args.input_dir))

    # Collect the representative records for all URLs
    m = Manager()
    global_uniqs = m.dict()
    lock = m.RLock()

    with Pool(args.processes) as pool:
        f = partial(file_to_dict, keep=args.keep, skip_urls=skip_urls,
                    url_fn=url_fn, global_uniqs=global_uniqs, lock=lock)
        pool.map(f, input_files)
        logging.info('Total number of unique URLs found: {}'.format(
            len(global_uniqs)))

        pool.close()
        pool.join()

    # And filter from the files all non-representative URLs
    if not os.path.isdir(args.output_dir):
        os.mkdir(args.output_dir)
    tasks = zip(input_files, [op.join(args.output_dir, f) for f in basenames])
    with Pool(args.processes) as pool:
        f = partial(filter_file, uniqs=global_uniqs, url_fn=url_fn)
        sum_stats = FilterStats()
        for stats in pool.starmap(f, tasks):
            sum_stats += stats

        pool.close()
        pool.join()

        logging.info(
            'Done filtering index: index files {} -> {}, URLs {} -> {}.'.format(
                sum_stats.old_files, sum_stats.new_files,
                sum_stats.old_urls, sum_stats.new_urls)
        )


if __name__ == '__main__':
    main()
