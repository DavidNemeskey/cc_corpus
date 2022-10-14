#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Filters the downloaded WARC files. Currently two filters are supported:
    - a URL whitelist
    - a URL blacklist

Note that this script is not intended not be used as a regular step in the
pipeline, but to extract the original page HTML for a subset of the corpus.
"""

from argparse import ArgumentParser
from collections import Counter
from functools import partial
import logging
from multiprocessing import Pool, Manager
import os

from multiprocessing_logging import install_mp_handler
import warc

from cc_corpus.mime import check_mime
from cc_corpus.utils import openall, otqdm, notempty


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', '-i', required=True,
                        help='the download (WARC) directory')
    parser.add_argument('--output-dir', '-o', required=True,
                        help='the output directory')
    parser.add_argument('--keep-urls', '-k', action='append', default=[],
                        help='keeps only the URLs in the specified url file(s).')
    parser.add_argument('--drop-urls', '-d', action='append', default=[],
                        help='drop all URLs in the specified url file(s).')
    parser.add_argument('--use-manager', '-M', action='store_true',
                        help='by default, with -k and -d, the url list is '
                             'loaded by each worker process. This might be '
                             'problematic if the list is huge and the size '
                             'of the memory available is small. -M aims to '
                             'alleviate this problem by only loading the list '
                             'once. However, due to the network communication '
                             'cost, execution time increases by a factor of '
                             '1.5-2.')
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
    if not (args.keep_urls or args.drop_urls):
        parser.error('At least one filter must be specified.')
    return args


def each_doc(doc_iter, stats):
    """
    This function is just there so that we can count the number of documents
    initially.
    """
    doc_no = 0
    for doc_no, doc in enumerate(doc_iter, start=1):
        yield doc
    stats['initial'] = doc_no


# Global, because we only want to create these once per working process
urls_to_drop = None
urls_to_keep = None


def initialize_url_filters(drop_urls, keep_urls):
    global urls_to_drop, urls_to_keep
    if drop_urls:
        urls_to_drop = read_files_into_set(drop_urls)
        logging.info('Loaded {} urls to drop.'.format(len(urls_to_drop)))
    if keep_urls:
        urls_to_keep = read_files_into_set(keep_urls)
        logging.info('Loaded {} urls to keep.'.format(len(urls_to_keep)))


def filter_urls(doc_iter, urls_to_drop, stats):
    doc_no, kept = 0, 0
    for doc_no, warc_record in enumerate(doc_iter, start=1):
        if not warc_record['WARC-Target-URI'] in urls_to_drop:
            kept += 1
            yield warc_record
    if doc_no:
        logging.info('Filtered {} documents with a URL blacklist, kept {}'.format(
            doc_no, kept))
    stats['drop_urls'] = kept


def retain_urls(doc_iter, urls_to_keep, stats):
    doc_no, kept = 0, 0
    for doc_no, warc_record in enumerate(doc_iter, start=1):
        if warc_record['WARC-Target-URI'] in urls_to_keep:
            kept += 1
            yield warc_record
    if doc_no:
        logging.info('Filtered {} documents with a URL whitelist, kept {}'.format(
            doc_no, kept))
    stats['keep_urls'] = kept


def filter_mimes(doc_iter, stats):
    doc_no, kept, mimes = 0, 0, Counter()
    for doc_no, warc_record in enumerate(doc_iter, start=1):
        payload = warc_record.payload.read().split(b'\r\n\r\n', 1)[1]
        warc_record.payload.offset = 0  # So that the record is reuseable
        mt, mime = check_mime(payload)
        mimes[mime] += 1
        if mt:
            kept += 1
            yield warc_record
    if doc_no:
        logging.info('Filtered {} documents based on mime:, kept {}'.format(
            doc_no, kept))
    stats['mimes'] = mimes


def read_files_into_set(files):
    ret = set()
    for f in files:
        with openall(f, 'rt') as inf:
            for line in map(str.strip, inf):
                ret.add(line)
    return ret


def process_file(filename, input_dir, output_dir,
                 keep_urls=None, drop_urls=None):
    input_file = os.path.join(input_dir, filename)
    output_file = os.path.join(output_dir, filename)
    logging.info('Processing file {}...'.format(filename))

    stats = Counter()
    with warc.open(input_file) as it:
        it = each_doc(it, stats)
        if drop_urls:
            # Get the right list: from the Manager or the local one
            url_list = drop_urls if drop_urls.__class__.__name__ == 'DictProxy' \
                                 else urls_to_drop  # noqa
            it = filter_urls(it, url_list, stats)
        if keep_urls:
            # Get the right list: from the Manager or the local one
            url_list = keep_urls if keep_urls.__class__.__name__ == 'DictProxy' \
                                 else urls_to_keep  # noqa
            it = retain_urls(it, url_list, stats)
        try:
            with notempty(openall(output_file, 'wb')) as outf:
                # MIME-based filtering. Why isn't it a separate function?
                # Because the warc library interace sucks, and once read(),
                # the payload of the WARC record cannot really be reused
                # (offset = 0 works sometimes, but not in this script...)
                doc_no, kept, mimes = 0, 0, Counter()
                for doc_no, doc in enumerate(it, 1):
                    response, payload = doc.payload.read().split(b'\r\n\r\n', 1)
                    mt, mime = check_mime(payload[:2048])
                    mimes[mime] += 1
                    if mt:
                        kept += 1
                        doc.header.write_to(outf)
                        outf.write(response)
                        outf.write(b'\r\n\r\n')
                        outf.write(payload)
                        outf.write(b'\r\n\r\n')
                if doc_no:
                    logging.info(f'Filtered {doc_no} documents based on mime: '
                                 f'kept {kept}')
                stats['mimes'] = dict(mimes)
        except:  # noqa
            logging.exception('Got an error.')
    logging.info('Finished processing file {}...'.format(filename))
    return stats


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    files = os.listdir(args.input_dir)
    logging.info('Scheduled {} files for filtering.'.format(len(files)))

    with Manager() as manager:
        if args.use_manager:
            initialize_url_filters(args.drop_urls, args.keep_urls)
            drop_urls = manager.dict({url: None for url in urls_to_drop or {}})
            keep_urls = manager.dict({url: None for url in urls_to_keep or {}})
            p = Pool(args.processes)
        else:
            p = Pool(args.processes, initializer=initialize_url_filters,
                     initargs=[args.drop_urls, args.keep_urls])
            drop_urls = args.drop_urls
            keep_urls = args.keep_urls
        f = partial(process_file,
                    input_dir=args.input_dir, output_dir=args.output_dir,
                    keep_urls=keep_urls, drop_urls=drop_urls)
        # Note: + / sum() do not keep keys with 0 values here, hence update()
        stats = Counter()
        mimes = Counter()
        for sub_stats in otqdm(p.imap_unordered(f, files),
                               'Filtering WARC files...', total=len(files)):
            logging.info(f'SS {sub_stats}')
            sub_mimes = sub_stats.pop('mimes')
            stats.update(sub_stats)
            mimes.update(sub_mimes)
        stats['mimes'] = dict(mimes)
        logging.info('Statistics: {}'.format(stats))
        p.close()
        p.join()
        logging.info('Done.')


if __name__ == '__main__':
    main()
