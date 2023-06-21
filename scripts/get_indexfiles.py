#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Downloads a selected top level domain from Common Crawl. Uses the new(er, it's
from 2015) S3-based method, completely circumventing the index server, which
can become overloaded.

References:
- https://groups.google.com/g/common-crawl/c/vBeLAbfH1wY
- https://groups.google.com/g/common-crawl/c/qRo-gqviaTM/m/40FDmXwRBwAJ
"""

from argparse import ArgumentParser
from contextlib import closing, nullcontext
import logging
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import time
import urllib.request

from cc_corpus.download import DownloadError, download_index_range
from cc_corpus.index import (
    BatchWriter, CLUSTER_SIZE, SurtDomain,
    filter_json, process_index_range,
    ranges_from_clusters, collect_clusters_from_index
)
from cc_corpus.utils import num_digits, openall


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    pattern_group = parser.add_mutually_exclusive_group(required=True)
    pattern_group.add_argument('--patterns', '-p', nargs='+',
                               help='the url pattern to download, '
                                    'e.g. "elte.hu".')
    pattern_group.add_argument('--pattern-file', '-pf', type=Path,
                               help='the file containing the patterns '
                                    'to download.')
    parser.add_argument('--collection', '-c', required=True,
                        help='the collection to download.')
    parser.add_argument('--output-dir', '-o', type=Path, required=True,
                        help='the output directory.')
    parser.add_argument('--lines-per-file', '-l', type=int, default=15000,
                        help='the number of index lines per output file. The '
                             'default is 15000.')
    parser.add_argument('--field-list', '--fl', type=str,
                        default='url,filename,offset,length,status,mime',
                        help='the list of fields to keep from the index '
                             'JSONs. A comma-separated list of field names. '
                             'The default is '
                             '"url,filename,offset,length,status,mime"')
    parser.add_argument('--clusters-dir', type=Path,
                        help='the directory to where the index-of-index file '
                             'cluster.idx is downloaded to. The file is '
                             'renamed to include the collection (see above). '
                             'If not specified, the file is downloaded to a '
                             'temporary directory and is deleted afterwards.')
    parser.add_argument('--file-prefix', '-f',
                        help='the output file name prefix. If not specified, '
                             'it will be based on the name of the pattern '
                             'file (if specified) or the first domain pattern.')
    parser.add_argument('--delay', '-d', type=float, default=1,
                        help='the number of seconds to wait between requests '
                             'to prevent DDoS\'ing the server.')
    parser.add_argument('--batch-size', '-b', type=int, default=100,
                        help='how many clusters (of 3,000 URLs each) to ask '
                             'for per request. The default is 100. Should be '
                             'balanced with --delay.')
    parser.add_argument('--max-retry', '-m', type=int, default=5,
                        help='maximum number of attempts to redownload a '
                             'specific page.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error',
                                 'critical'],
                        help='the logging level.')
    args = parser.parse_args()

    args.field_list = {field.strip() for field in args.field_list.split(',')}
    return args


def get_file_prefix(
    pattern_file: Path, patterns: list[str], file_prefix: str
) -> str:
    if file_prefix:
        return file_prefix
    elif pattern_file:
        file_name = pattern_file.name
        return file_name[
            :len(file_name) - len(''.join(pattern_file.suffixes))
        ]
    else:
        return f'pattern-{patterns[0]}'.replace('*.', '')


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )

    base_url = f'https://data.commoncrawl.org/cc-index/collections/' \
               f'{args.collection}/indexes/'

    if args.patterns:
        raw_patterns = args.patterns
    else:
        with openall(args.pattern_file, 'rt') as pf:
            raw_patterns = [line.strip() for line in pf]

    patterns = [SurtDomain.from_string(p) for p in raw_patterns]
    logging.debug(f'The patterns we look for: {patterns}')

    if args.clusters_dir:
        clusters_context = nullcontext(args.clusters_dir)
        logging.info(f'Using clusters directory {args.clusters_dir}.')
        args.clusters_dir.mkdir(parents=True, exist_ok=True)
    else:
        clusters_context = TemporaryDirectory()
        logging.info('Using temporary clusters directory '
                     f'{clusters_context.name}.')

    args.output_dir.mkdir(parents=True, exist_ok=True)

    with clusters_context as clusters_dir:
        # First, let's download the cluster.idx file.
        cluster_idx = Path(clusters_dir) / f'{args.collection}_cluster.idx'
        if not cluster_idx.is_file():
            logging.info(f'Downloading cluster index for {args.collection}...')
            logging.debug(base_url + 'cluster.idx')
            urllib.request.urlretrieve(base_url + 'cluster.idx', cluster_idx)

        # Then, get the files and byte ranges that correspond to the query:
        clusters = collect_clusters_from_index(patterns, cluster_idx)
        logging.info(f'Found {len(clusters)} clusters to download.')

        # Assemble a regexp that matches the patterns:
        regexp_string = '^('
        regexp_string += '|'.join(','.join(pe) for pe in patterns)
        regexp_string += ')[,)]'
        pattern_matcher = re.compile(regexp_string)

        file_prefix = get_file_prefix(args.pattern_file, raw_patterns,
                                      args.file_prefix)
        with closing(BatchWriter(
            args.lines_per_file, args.output_dir,
            num_digits(len(clusters) * CLUSTER_SIZE // args.lines_per_file + 1),
            f'{file_prefix}-{args.collection}-'
        )) as bw:
            for frange in ranges_from_clusters(clusters, args.batch_size):
                time.sleep(args.delay)
                logging.debug(f'Downloading {frange}...')
                try:
                    index_range = download_index_range(
                        base_url + frange.file_name,
                        frange.offset, frange.length,
                        args.max_retry
                    )
                    for line in process_index_range(index_range):
                        if pattern_matcher.match(line):
                            try:
                                bw.write(filter_json(line, args.field_list))
                            except ValueError:
                                logging.exception('Could not parse line {line}')
                                raise
                except DownloadError as de:
                    logging.error(f'Could not download range: {de}.')

    logging.info('Done.')


if __name__ == '__main__':
    main()
