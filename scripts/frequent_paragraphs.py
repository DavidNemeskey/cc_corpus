#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Writes the positions of all documents in each file."""

from argparse import ArgumentParser
from functools import partial
from itertools import accumulate, groupby
import logging
from multiprocessing import Pool
import os
import os.path as op
from urllib.parse import urlsplit

from datasketch import MinHashLSH
from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import parse_file, parse
from cc_corpus.deduplication import MinHasher
from cc_corpus.utils import host_to_path, host_weight, openall


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--index', required=True,
                        help='the output index file.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1). Note that in order '
                             'to deduplicate documents, much memory might be '
                             'needed, so it is a good idea to be conservative '
                             'with the number of processes.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='the logging level.')
    subparsers = parser.add_subparsers(
        help='The steps of frequent paragraph detection.')

    parser_index = subparsers.add_parser(
        'index_docs', aliases=['index'],
        help='Indexes the documents in the corpus and sorts the index by '
             'domain and corpus location.'
    )
    parser_index.set_defaults(command='index_docs')
    parser_index.add_argument('--input-dir', '-i', required=True,
                              help='the corpus directory.')

    parser_distribute = subparsers.add_parser(
        'distribute_index', aliases=['distribute', 'dist'],
        help='Distributes the index file into separate files for running on'
             'separate machines. Each host can have their own weight.'
    )
    parser_distribute.set_defaults(command='distribute')
    parser_distribute.add_argument('--host', '-H', action='append',
                                   type=host_weight, dest='hosts',
                                   help='a host:weight pair.')

    parser_filter = subparsers.add_parser(
        'filter_paragraphs', aliases=['filter'],
        help='Filters frequent paragraphs within a domain.'
    )
    parser_filter.set_defaults(command='filter')
    parser_filter.add_argument('--input-dir', '-i', required=True,
                               help='the corpus directory.')
    parser_filter.add_argument(
        '--permutations', '-p', type=int, default=256,
        help='the number of permutations per paragraph (256).'
    )
    parser_filter.add_argument(
        '--n', '-n', type=int, default=5,
        help='the number of permutations per paragraph (5).'
    )
    parser_filter.add_argument('--threshold', '-t', type=float, default=0.9,
                               help='the Jaccard similarity threshold (0.9).')

    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    return args


# --------------------------------- Indexing -----------------------------------


def index_file(input_file, input_dir):
    """
    Indexes an input file. Returns two items:
    - the input file: since this function is called (kind of) asynchronously,
      we need to keep track of it
    - a list of tuples for each document: its url, position and length in the
      file.
    """
    input_path = op.join(input_dir, input_file)
    urls, lens = [], []
    for doc in parse_file(input_path):
        urls.append(doc.attrs['url'])
        lens.append(doc.stream_size())
    return input_file, list(zip(urls, accumulate([0] + lens[:-1]), lens))


def index_key(url_file_pos_len):
    """The key function for index list sorting."""
    url, input_file, input_pos, _ = url_file_pos_len
    return (urlsplit(url).netloc.split('.')[::-1], input_file, input_pos)


def main_index_documents(args):
    """The main function for indexing documents."""
    input_files = os.listdir(args.input_dir)

    logging.info('Found a total of {} input files.'.format(len(input_files)))
    index = []
    with Pool(args.processes) as pool:
        f = partial(index_file, input_dir=args.input_dir)
        for input_file, urls_poss_lens in pool.imap(f, input_files):
            for doc_url, doc_pos, doc_len in urls_poss_lens:
                index.append((doc_url, input_file, doc_pos, doc_len))
    pool.close()
    pool.join()

    index.sort(key=index_key)
    with openall(args.index, 'wt') as outf:
        for doc_url, doc_file, doc_pos, doc_len in index:
            print(doc_url, doc_file, doc_pos, doc_len, sep='\t', file=outf)


# ------------------------------- Distribution ---------------------------------


def read_grouped_index(index_file):
    """Reads the index file domain group by group."""
    with openall(index_file) as inf:
        for _, group in groupby(map(str.strip, inf),
                                key=lambda l: urlsplit(l[0:l.find('\t')]).netloc):
            yield list(group)


def main_distribute(args):
    """The main function for distributing the index file."""
    weights = [weight for _, weight in args.hosts]
    hosts = [openall(host_to_path(args.index, host), 'wt') for host, _ in args.hosts]
    lens = [0 for _ in weights]
    try:
        for group in read_grouped_index(args.index):
            i = lens.index(min(lens))  # argmin
            logging.debug('Adding {} items to host {} ({}).'.format(
                len(group), i, hosts[i].name))
            for line in group:
                print(line, file=hosts[i])
            # Higher weight means "I need more documents"
            lens[i] += len(group) / weights[i]
    finally:
        for i, host in enumerate(hosts):
            logging.info('Wrote {} lines to {}.'.format(
                round(lens[i] * weights[i]), host.name))
            host.close()


# -------------------------------- Filtering ----------------------------------


def read_group_documents(group, input_dir):
    """Returns an iterator of the documents in a group."""
    last_file = None
    f = None
    try:
        for line in group:
            _, doc_file, doc_pos, doc_len = line.split('\t')
            if doc_file != last_file:
                if f:
                    f.close()
                f = openall(op.join(input_dir, doc_file), 'rb')
                last_file = doc_file
            f.seek(int(doc_pos))
            yield from parse(f.read(int(doc_len)).decode('utf-8').split('\n'))

    finally:
        if f:
            f.close()


def main_filter(args):
    """The main function for filtering the documents."""
    minhasher = MinHasher(args.permutations, args.n)
    for group in read_grouped_index(args.index):
        domain = urlsplit(group[0][0:group[0].find('\t')]).netloc
        lsh = MinHashLSH(threshold=args.threshold, num_perm=args.permutations)
        ps = {}  # key -> [score, num, text]
        for doc in read_group_documents(group, args.input_dir):
            # Step 1: decrease score of all paragraphs
            for p_data in ps.values():
                p_data[0] *= 0.99
            # Step 2: add new paragraphs to the roster
            already_increased = set()  # See below
            for p, text in enumerate(doc.paragraphs, start=1):
                mh = minhasher.minhash(text)
                if mh in lsh:
                    for duplicate in lsh.query(mh):
                        # Ensure that the paragraph counter is increased by
                        # at most one per document
                        if duplicate not in already_increased:
                            ps[duplicate][0] += 1
                            ps[duplicate][1] += 1
                            already_increased.add(duplicate)
                else:
                    key = '_'.join(doc.attrs['url'], p)
                    lsh.insert(key, mh)
                    ps[key] = [1, 1, text]
                    already_increased.add(key)
            # Step 3: drop paragraphs with low score
            to_drop = [key for key, p_data in ps.items() if p_data[0] < 0.5]
            for key in to_drop:
                ps.pop(key)
                lsh.remove(key)

        # Get rid of paragraphs that only occured once
        ps = {key: p_data for key, p_data in ps.items() if p_data[1] > 1}
        logging.info('Found {} frequent paragraphs in domain {}'.format(
            len(ps), domain))
        for key, p_data in ps.items():
            logging.debug('{}: {} {} {}'.format(key, p_data[0], p_data[1], p_data[2]))


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    install_mp_handler()

    os.nice(20)

    if args.command == 'index_docs':
        main_index_documents(args)
    elif args.command == 'distribute':
        main_distribute(args)
    elif args.command == 'filter':
        main_filter(args)

    logging.info('Done.')


if __name__ == '__main__':
    main()
