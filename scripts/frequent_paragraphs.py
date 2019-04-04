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
                              dest='input_dirs', action='append',
                              help='the corpus directory. Can be specified '
                                   'more than once.')

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


def index_file(input_file):
    """
    Indexes an input file. Returns two items:
    - the input file: since this function is called (kind of) asynchronously,
      we need to keep track of it
    - a list of tuples for each document: its url, position and length in the
      file.
    """
    urls, lens = [], []
    for doc in parse_file(input_file):
        urls.append(doc.attrs['url'])
        lens.append(doc.stream_size())
    return input_file, list(zip(urls, accumulate([0] + lens[:-1]), lens))


def index_key(url_file_pos_len):
    """The key function for index list sorting."""
    url, input_file, input_pos, _ = url_file_pos_len
    # Protocolless URL, so that http:// and https:// variants are put next to
    # each other. This allows us to uniq' them in main_index or during filtering
    return (urlsplit(url).netloc.split('.')[::-1],
            url[url.find('://') + 3:], input_file, input_pos)


def main_index_documents(args):
    """The main function for indexing documents."""
    input_files = [op.join(input_dir, f) for input_dir in args.input_dirs
                                         for f in os.listdir(input_dir)]

    logging.info('Found a total of {} input files.'.format(len(input_files)))
    index = []
    with Pool(args.processes) as pool:
        f = partial(index_file)
        for input_file, urls_poss_lens in pool.imap(f, input_files):
            for doc_url, doc_pos, doc_len in urls_poss_lens:
                index.append((doc_url, input_file, doc_pos, doc_len))
    pool.close()
    pool.join()

    index.sort(key=index_key)
    with openall(args.index, 'wt') as outf:
        for domain, group in groupby(index, lambda record: urlsplit(record[0]).netloc):
            urls_written = set()
            for doc_url, doc_file, doc_pos, doc_len in group:
                # This also filters http:// + https:// variants
                try:
                    pure_url = doc_url[doc_url.find('://') + 3:]
                    if pure_url not in urls_written:
                        urls_written.add(pure_url)
                        print(doc_url, doc_file, doc_pos, doc_len, sep='\t', file=outf)
                        logging.debug('Printed URL {}.'.format(doc_url))
                    else:
                        logging.debug('Skipped duplicate URL {}.'.format(doc_url))
                except:
                    logging.exception('Error somewhere!!!')


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


def read_group_documents(group):
    """Returns an iterator of the documents in a group."""
    last_file = None
    f = None
    try:
        for line in group:
            _, doc_file, doc_pos, doc_len = line.split('\t')
            if doc_file != last_file:
                if f:
                    f.close()
                f = openall(doc_file, 'rb')
                last_file = doc_file
            f.seek(int(doc_pos))
            yield from parse(f.read(int(doc_len)).decode('utf-8').split('\n'))

    finally:
        if f:
            f.close()


def collect_frequent(group):
    """Collects the frequent paragraphs in a domain."""
    minhasher = MinHasher(args.permutations, args.n)
    lsh = MinHashLSH(threshold=args.threshold, num_perm=args.permutations)
    ps = {}  # key -> [score, num, text]
    num_dup = 0

    for doc_no, doc in enumerate(read_group_documents(group)):
        # Step 1: decrease score of all paragraphs
        for p_data in ps.values():
            p_data[0] *= 0.99

        # Step 2: add new paragraphs to the roster
        already_increased = set()  # See below
        for p, text in enumerate(doc.paragraphs, start=1):
            mh = minhasher.minhash(text)
            found_dup = False
            for duplicate in lsh.query(mh):
                # Ensure that the paragraph counter is increased by
                # at most one per document
                if duplicate not in already_increased:
                    ps[duplicate][0] += 1
                    ps[duplicate][1] += 1
                    already_increased.add(duplicate)
                    if not found_dup:
                        found_dup = True
                        num_dup += 1
            if not found_dup:
                # OK, this is a new paragraph
                key = doc.attrs['url'] + '_' + str(p)
                lsh.insert(key, mh)
                ps[key] = [1, 1, text]
                already_increased.add(key)

        # Step 3: drop paragraphs with low score
        to_drop = [key for key, p_data in ps.items() if p_data[0] < 0.5]
        for key in to_drop:
            ps.pop(key)
            lsh.remove(key)
    logging.debug('Ending domain {}...'.format(domain))

    # Get rid of paragraphs that only occured once
    ps = {key: p_data for key, p_data in ps.items() if p_data[1] > 1}
    if ps:
        logging.info(
            'Found {} frequent paragraphs (duplicates: {}) '
            'in domain {} ({} documents).'.format(
                len(ps), num_dup, domain, doc_no))
    return ps


def main_filter(args):
    """The main function for filtering the documents."""
    install_mp_handler()

    minhasher = MinHasher(args.permutations, args.n)
    for group in read_grouped_index(args.index):
        domain = urlsplit(group[0][0:group[0].find('\t')]).netloc
        logging.debug('Starting domain {}...'.format(domain))

        lsh = MinHashLSH(threshold=args.threshold, num_perm=args.permutations)
        ps = {}  # key -> [score, num, text, text_hash]
        text_ps = {}  # text hash -> key
        dup_by_text, dup_by_hash = 0, 0
        for doc_no, doc in enumerate(read_group_documents(group)):
            # Step 1: decrease score of all paragraphs
            for p_data in ps.values():
                p_data[0] *= 0.99

            # Step 2: add new paragraphs to the roster
            already_increased = set()  # See below
            for p, text in enumerate(doc.paragraphs, start=1):
                text_hash = hash(text)
                dup_key = text_ps.get(text_hash)

                # First try with text agreement (need to re-check the
                # approximate hash equality), because it's much faster
                if dup_key and ps[dup_key][2] == text:
                    # Ensure that the paragraph counter is increased by
                    # at most one per document
                    if dup_key not in already_increased:
                        ps[dup_key][0] += 1
                        ps[dup_key][1] += 1
                        already_increased.add(dup_key)
                        dup_by_text += 1
                    continue

                # Then on a minhash basis
                mh = minhasher.minhash(text)
                for duplicate in lsh.query(mh):
                    # Ensure that the paragraph counter is increased by
                    # at most one per document
                    if duplicate not in already_increased:
                        ps[duplicate][0] += 1
                        ps[duplicate][1] += 1
                        already_increased.add(duplicate)
                        dup_by_hash += 1
                    # There will be at most one matching paragraph
                    continue

                # OK, this is a new paragraph
                key = doc.attrs['url'] + '_' + str(p)
                lsh.insert(key, mh)
                text_ps[text_hash] = key
                ps[key] = [1, 1, text, text_hash]
                already_increased.add(key)

            # Step 3: drop paragraphs with low score
            to_drop = [key for key, p_data in ps.items() if p_data[0] < 0.5]
            for key in to_drop:
                _, _, _, text_hash = ps.pop(key)
                lsh.remove(key)
                del text_ps[text_hash]
        logging.debug('Ending domain {}...'.format(domain))

        # Get rid of paragraphs that only occured once
        ps = {key: p_data for key, p_data in ps.items() if p_data[1] > 1}
        if ps:
            logging.info(
                'Found {} frequent paragraphs (duplicates: {} by text / '
                '{} by lsh) in domain {} ({} documents).'.format(
                    len(ps), dup_by_text, dup_by_hash, domain, doc_no))
        # for key, p_data in sorted(ps.items(), key=lambda kv: -kv[1][1]):
        #     logging.debug('{}: {} {} {}'.format(key, p_data[0], p_data[1], p_data[2]))


def main():
    args = parse_arguments()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(process)s - %(levelname)s - %(message)s'
    )
    # install_mp_handler()

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
