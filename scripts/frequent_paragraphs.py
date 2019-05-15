#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Writes the positions of all documents in each file."""

from argparse import ArgumentParser
from contextlib import closing
from functools import partial, reduce
from itertools import accumulate, chain, groupby
import logging
from multiprocessing import Manager, Pool
import os
import os.path as op
import time
from urllib.parse import urlsplit

from datasketch import MinHashLSH
from multiprocessing_logging import install_mp_handler

from cc_corpus.corpus import BatchWriter, Document, parse_file, parse
from cc_corpus.deduplication import MinHasher
from cc_corpus.frequent import PData
from typing import Any, Dict, Generator, Iterator, List, Set, Tuple
from cc_corpus.utils import grouper, host_to_path, host_weight, openall, Stats


def parse_arguments():
    parser = ArgumentParser(__doc__)
    parser.add_argument('--index', required=True,
                        help='the output index file.')
    parser.add_argument('--processes', '-P', type=int, default=1,
                        help='number of worker processes to use (max is the '
                             'num of cores, default: 1). Note that in order '
                             'to deduplicate documents, much memory might be '
                             'needed, so it is a good idea to be conservative '
                             'with the number of processes. Also, for the '
                             'collect task, if too high a number of '
                             'processes is created, the main processes will '
                             'be unable to keep up with the data they '
                             'produce, and the system might run out of memory.')
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

    parser_collect = subparsers.add_parser(
        'collect_frequent', aliases=['collect', 'frequent'],
        help='Collects the frequent paragraphs within domains.'
    )
    parser_collect.set_defaults(command='collect')
    parser_collect.add_argument(
        '--output-prefix', '-o', required=True,
        help='the prefix of the output files. Two files will be written. '
             'The first is the .pdata file for each frequent paragraph, which '
             'contains three fields per file: the minhash, the score and the '
             'frequency of the paragraph (the latter two can be used to '
             'continue frequent document collection when more data becomes '
             'available). The second is the .pdi file, which contains the '
             'index of the former: for each domain, it specifies the offset '
             'and length of the paragraph data.')
    parser_collect.add_argument(
        '--permutations', '-p', type=int, default=256,
        help='the number of permutations per paragraph (256).'
    )
    parser_collect.add_argument('--n', '-n', type=int, default=5,
                                help='the size of the n-grams (5).')
    parser_collect.add_argument('--threshold', '-t', type=float, default=0.9,
                                help='the Jaccard similarity threshold (0.9).')
    parser_collect.add_argument('--min-freq', '-m', type=int, default=2,
                                help='the minimum number of occurrence from '
                                     'which a paragraph is deemed frequent (2).')
    parser_collect.add_argument('--docs-per-batch', type=int, default=100,
                                help='the number of documents to send to '
                                     'workers at a time (100).')
    decay_group = parser_collect.add_mutually_exclusive_group()
    decay_group.add_argument('--c', '-c', type=float, default=0.01,
                             help='the decay (multiplication) constant used '
                                  'for scoring paraphraphs (0.99).')
    decay_group.add_argument('--keep-for', '-k', type=int,
                             help='keep frequent paragraph candidates for this '
                                  'many iterations. This argument is '
                                  'another way to specify -c and is mutually '
                                  'exclusive with it.')

    parser_filter = subparsers.add_parser(
        'filter_paragraphs', aliases=['filter'],
        help='Filters frequent paragraphs within a domain.'
    )
    parser_filter.set_defaults(command='filter')
    parser_filter.add_argument(
        '--output-dir', '-o', required=True,
        help='the output directory. The *last directory* of the input path '
             'is replaced with the output directory; not all of it. This is '
             'because we expect that all corpus directories are next to each '
             'other; also, if the year is the path element before that, it '
             'will be kept intact.')
    parser.add_argument('--documents', '-d', type=int, default=1000,
                        help='the number of documents an output file should '
                        'contain (1000).')
    parser.add_argument('--zeroes', '-z', type=int, default=4,
                        help='the number of zeroes in the output files\' name.')
    parser_filter.add_argument(
        '--permutations', '-p', type=int, default=256,
        help='the number of permutations per paragraph (256).'
    )
    parser_filter.add_argument('--n', '-n', type=int, default=5,
                               help='the size of the n-grams (5).')
    parser_filter.add_argument('--threshold', '-t', type=float, default=0.9,
                               help='the Jaccard similarity threshold (0.9).')
    parser_filter.add_argument('--min-freq', '-m', type=int, default=2,
                               help='the minimum number of occurrence from '
                                    'which a paragraph is deemed frequent (2).')
    decay_group = parser_filter.add_mutually_exclusive_group()
    decay_group.add_argument('--c', '-c', type=float, default=0.01,
                             help='the decay (multiplication) constant used '
                                  'for scoring paraphraphs (0.99).')
    decay_group.add_argument('--keep-for', '-k', type=int,
                             help='keep frequent paragraph candidates for this '
                                  'many iterations. This argument is '
                                  'another way to specify -c and is mutually '
                                  'exclusive with it.')

    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    # Compute c from keep_for
    if args.command == 'filter' and args.keep_for:
        args.c = 1 - pow(0.5, 1 / (args.keep_for - 0.5))
    return args


# ----------------------------------- Types -----------------------------------


DocURL = str
DocFile = str
DocPos = int
DocLen = int
DocTuple = Tuple[DocURL, DocPos, DocLen]
DocFileTuple = Tuple[DocURL, DocFile, DocPos, DocLen]

Group = List[str]
DomainGroup = Tuple[str, Group]
PDict = Dict[str, PData]


# --------------------------------- Indexing -----------------------------------


def index_file(input_file: str) -> Tuple[str, List[DocTuple]]:
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


def index_key(url_file_pos_len: DocFileTuple) -> Tuple[List[str], str,
                                                       DocFile, DocPos]:
    """
    The key function for index list sorting. Sorts by domain, the
    (protocol-less) url, input file and position in the input file, in that
    order. The latter two fields were added to reduce seeking, but with the
    url added later, they kind of irrelevant now.
    """
    url, input_file, input_pos, _ = url_file_pos_len
    # Protocolless URL, so that http:// and https:// variants are put next to
    # each other. This allows us to uniq' them in main_index or during filtering
    return (urlsplit(url).netloc.split('.')[::-1],
            url[url.find('://') + 3:], input_file, input_pos)


def main_index_documents(args):
    """The main function for indexing documents."""
    input_files = [op.join(input_dir, f)
                   for input_dir in args.input_dirs
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

def read_index(index_file: str) -> Iterator[str]:
    """Reads the index file. Not really necessary, but oh well."""
    with openall(index_file) as inf:
        yield from map(str.strip, inf)


def read_grouped_index(index_file: str) -> Iterator[DomainGroup]:
    """Reads the index file domain group (of lines) by group."""
    for domain, group in groupby(
        read_index(index_file),
        key=lambda l: urlsplit(l[0:l.find('\t')]).netloc
    ):
        yield domain, list(group)


def main_distribute(args):
    """The main function for distributing the index file."""
    weights = [weight for _, weight in args.hosts]
    hosts = [openall(host_to_path(args.index, host), 'wt') for host, _ in args.hosts]
    lens = [0 for _ in weights]
    try:
        for _, group in read_grouped_index(args.index):
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


# -------------------------------- Collection ----------------------------------


CollectStats = Stats.create('docs', 'ps', 'frequents', 'domains')  # type: Any


def read_group_documents(group: Group) -> Iterator[Document]:
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


def minhash_group(group: Group, minhasher: MinHasher) -> List[Tuple[str, List[Any]]]:
    """
    Minhashes all paragraphs in a group of documents.

    In the whole fingerprinting / deduplication process, minhashing takes up
    most of the time. Hence, it has been extracted into a separate function so
    that it can be run parallelly. We pass a group of documents (instead of
    a single one and using `chunksize` in :func:`multiprocessing.imap`), because
    in this way, :func:`read_group_documents` might be able to run faster,
    using the fact that consecutive documents might belong to the same file.

    :param group: the lines that describe the documents; as read by
                  :func:`read_grouped_index`.
    :param minhasher: TODO delete
    :returns: a list of tuples of
        - the URL of the document (so that the caller knows what it gets back)
        - a list of paragraph fingerprints.
    """
    logging.debug('minhash_group({}) -> {}'.format(len(group), group[0]))
    return [(doc.attrs['url'], [minhasher.minhash(text)
                                for p, text in enumerate(doc.paragraphs, start=1)])
            for doc in read_group_documents(filter(bool, group))]


class FrequentCollector:
    def __init__(self, threshold, permutations, decay, min_freq):
        self.threshold = threshold
        self.permutations = permutations
        self.decay = decay
        self.min_freq = min_freq

    def reset(self):
        """Resets the bookkeeping and statistics objects."""
        self.stats = CollectStats(domains=1)
        self.lsh = MinHashLSH(threshold=self.threshold,
                              num_perm=self.permutations)
        self.freq_ps = {}  # type: Dict[str, PData]
        self.num_dup = 0

    def collect_from_doc(self, url, paragraphs):
        """
        Runs the algorithm in MMDS (TOOD) on a document, does the bookkeeping
        and updates the statistcs in the object.

        :param url: the URL of the document (used as key in LSH).
        :param paragraphs: the minhashes of the paragraphs of the document.
        """
        # Step 1: decrease score of all paragraphs
        for p_data in self.freq_ps.values():
            p_data *= self.decay

        # Step 2: add new paragraphs to the roster
        already_increased = set()  # type: Set[str]
        for p, mh in enumerate(paragraphs, start=1):
            found_dup = False
            for duplicate in self.lsh.query(mh):
                # Ensure that the paragraph counter is increased by
                # at most one per document
                if duplicate not in already_increased:
                    self.freq_ps[duplicate] += 1
                    already_increased.add(duplicate)
                    if not found_dup:
                        found_dup = True
                        self.num_dup += 1
            if not found_dup:
                # OK, this is a new paragraph
                key = url + '_' + str(p)
                self.lsh.insert(key, mh)
                self.freq_ps[key] = PData(mh)
                already_increased.add(key)
        self.stats.docs += 1
        self.stats.ps += p

        # Step 3: drop paragraphs with low score
        to_drop = [key for key, p_data in self.freq_ps.items()
                   if p_data.score < 0.5]
        for key in to_drop:
            self.freq_ps.pop(key)
            self.lsh.remove(key)

    def wrap_up_domain(self):
        """
        Drops all frequent candidates that are below the minimum frequency and
        updates the statistics.
        """
        # Get rid of paragraphs that only occured once
        self.freq_ps = {key: p_data for key, p_data in self.freq_ps.items()
                        if p_data.count > self.min_freq}
        self.stats.frequents = len(self.freq_ps)


def collect_frequent2(
    it: Iterator[List[Tuple[str, List[Any]]]], threshold: float,
    permutations: int, decay: float, min_freq: int
 ) -> Generator[Tuple[str, PDict], None, None]:  # noqa
    """
    Reads all the documents (as returned by :func:`minhash_group`) and
    collects the frequent paragraphs from them on a per-domain basis.

    TODO: reference to MMDS

    Yields (domain, `PDict`) tuples per domain.
    """
    curr_domain = None
    fc = FrequentCollector(threshold, permutations, decay, min_freq)
    # I don't want to write all the domain != curr_domain stuff twice, so
    # let's add a sentinel record to the end.
    for url, mhs in chain(chain.from_iterable(it), [('', [])]):
        domain = urlsplit(url).netloc

        # A new domain: yield results and re-initialize everything
        if domain != curr_domain:
            # Filtering and yielding results
            if curr_domain is not None:
                fc.wrap_up_domain()

                logging.debug(
                    'Finished collecting frequent paragraphs from {}...'.format(
                        curr_domain))
                if fc.freq_ps:
                    logging.debug('Found {} frequent paragraphs (duplicates: '
                                  '{}) in domain {} ({} documents).'.format(
                                      len(fc.freq_ps), fc.num_dup,
                                      curr_domain, fc.stats.docs))

                # The domain is returned as well, so that we know what the input was
                yield curr_domain, fc.freq_ps, fc.stats

            # Check for the sentinel
            if not domain:
                break

            # Re-initialization
            logging.debug(
                'Collecting frequent paragraphs from {}...'.format(domain))
            curr_domain = domain
            fc.reset()

        fc.collect_from_doc(url, mhs)


def main_collect2(args):
    """
    The main function for collecting frequent paragraphs (and saving the
    results to file).
    """
    install_mp_handler()

    logging.info('Collecting frequent paragraphs from index {}...'.format(
        args.index))

    with closing(open('{}.pdata'.format(args.output_prefix), 'wb')) as dataf:
        index = []
        sum_stats = CollectStats()

        minhasher = MinHasher(args.permutations, args.n)
        with Pool(args.processes) as pool, index:
            # TODO: grouper parameter to argument
            it = pool.imap(partial(minhash_group, minhasher=minhasher),
                           grouper(read_index(args.index), args.docs_per_batch))
            for domain, freq_ps, stats in collect_frequent2(
                it, args.threshold, args.permutations, 1 - args.c, args.min_freq
            ):
                if freq_ps:
                    offset = dataf.tell()
                    for pdata in sorted(freq_ps.values(),
                                        key=lambda pd: -pd.count):
                        pdata.write_to(dataf)
                    length = dataf.tell() - offset
                    index.append((domain, offset, length, len(freq_ps), stats.docs))
                sum_stats += stats

        index.sort()
        with closing(open('{}.pdi'.format(args.output_prefix), 'wt')) as indexf:
            for domain, offset, length, num, docs in index:
                print('{}\t{}\t{}\t{}\t{}'.format(
                    domain, offset, length, num, docs), file=indexf)

        pool.close()
        pool.join()

        logging.info('Collected frequent paragraphs from index {} '
                     'with statistics {}.'.format(args.index, sum_stats))


def collect_frequent(domain_group: DomainGroup, minhasher: MinHasher,
                     threshold: float, decay: float, min_freq: int) -> Tuple[str, PDict]:
    """Collects the frequent paragraphs in a domain."""
    domain, group = domain_group
    stats = CollectStats(domains=1)
    logging.debug('Collecting frequent paragraphs from {}...'.format(domain))

    lsh = MinHashLSH(threshold=threshold, num_perm=minhasher.permutations)
    ps = {}  # type: Dict[str, PData]
    num_dup = 0

    for doc_no, doc in enumerate(read_group_documents(group), start=1):
        # Step 1: decrease score of all paragraphs
        for p_data in ps.values():
            p_data *= decay

        # Step 2: add new paragraphs to the roster
        already_increased = set()  # type: Set[str]
        for p, text in enumerate(doc.paragraphs, start=1):
            mh = minhasher.minhash(text)
            found_dup = False
            for duplicate in lsh.query(mh):
                # Ensure that the paragraph counter is increased by
                # at most one per document
                if duplicate not in already_increased:
                    ps[duplicate] += 1
                    already_increased.add(duplicate)
                    if not found_dup:
                        found_dup = True
                        num_dup += 1
            if not found_dup:
                # OK, this is a new paragraph
                key = doc.attrs['url'] + '_' + str(p)
                lsh.insert(key, mh)
                ps[key] = PData(mh)
                already_increased.add(key)
        stats.ps += p

        # Step 3: drop paragraphs with low score
        to_drop = [key for key, p_data in ps.items() if p_data.score < 0.5]
        for key in to_drop:
            ps.pop(key)
            lsh.remove(key)
    stats.docs = doc_no
    logging.debug('Finished collecting frequent paragraphs from {}...'.format(
        domain))

    # Get rid of paragraphs that only occured once
    ps = {key: p_data for key, p_data in ps.items() if p_data.count > min_freq}
    if ps:
        logging.debug('Found {} frequent paragraphs (duplicates: {}) '
                      'in domain {} ({} documents).'.format(
                          len(ps), num_dup, domain, doc_no))
    stats.frequents = len(ps)
    # The domain is returned as well, so that we know what the input was
    return domain, ps, stats


def main_collect(args):
    """
    The main function for collecting frequent paragraphs (and saving the
    results to file).
    """
    install_mp_handler()

    logging.info('Collecting frequent paragraphs from index {}...'.format(
        args.index))

    minhasher = MinHasher(args.permutations, args.n)
    with Pool(args.processes) as pool:
        f = partial(collect_frequent, minhasher=minhasher,
                    threshold=args.threshold, decay=1 - args.c,
                    min_freq=args.min_freq)
        with closing(open('{}.pdata'.format(args.output_prefix), 'wb')) as dataf:
            index = []
            sum_stats = CollectStats()
            for domain, freq_ps, stats in pool.imap(f, read_grouped_index(args.index)):
                if freq_ps:
                    offset = dataf.tell()
                    for pdata in sorted(freq_ps.values(),
                                        key=lambda pd: -pd.count):
                        pdata.write_to(dataf)
                    length = dataf.tell() - offset
                    index.append((domain, offset, length, len(freq_ps)))
                sum_stats += stats

        index.sort()
        with closing(open('{}.pdi'.format(args.output_prefix), 'wt')) as indexf:
            for domain, offset, length, num in index:
                print('{}\t{}\t{}\t{}\t{}'.format(
                    domain, offset, length, num, sum_stats.docs), file=indexf)

        pool.close()
        pool.join()

        logging.info('Collected frequent paragraphs from index {} '
                     'with statistics {}.'.format(args.index, sum_stats))


# -------------------------------- Filtering ----------------------------------


FilterStats = Stats.create(
    'frequent', 'old_ps', 'new_ps', 'old_docs', 'new_docs')  # type: Any


def write_through(group: Group, domain: str,
                  stats: FilterStats) -> Iterator[Document]:
    """
    Just enumerates all documents in the group / domain. Called by
    filter_paragraphs() when there are no frequent paragraphs in the domain
    and so no filtering is required.
    """
    domain = urlsplit(group[0][0:group[0].find('\t')]).netloc
    logging.debug(
        'Domain {} does not require filtering, copying documents...'.format(
            domain))

    for doc_no, doc in enumerate(read_group_documents(group), start=1):
        stats.old_ps += len(doc.paragraphs)
        stats.new_ps += len(doc.paragraphs)
        yield doc
    stats.old_docs += doc_no
    stats.new_docs += doc_no

    logging.debug('Copied {} documents from {}.'.format(doc_no, domain))


def filter_paragraphs(group: Group, domain: str, freq_ps: PDict,
                      minhasher: MinHasher, threshold: float,
                      stats: FilterStats) -> Iterator[Document]:
    """Filters the frequent paragraphs from documents."""
    # Handle the case where no filtering is needed first
    if len(freq_ps) == 0:
        yield from write_through(group, domain, stats)
        return

    logging.debug('Filtering frequent paragraphs from {}...'.format(domain))

    lsh = MinHashLSH(threshold=threshold, num_perm=minhasher.permutations)
    for key, p_data in freq_ps.items():
        lsh.insert(key, p_data.minhash)
    # The LSH is, by itself, not enough for frequent p filtering, as we have to
    # keep frequent ps the first time we see them. Hence, seen_frequents, which
    # consists of frequent paragraphs we have already seen and thus can
    # (should) be omitted.
    seen_frequents = set()  # type: Set[str]

    for doc_no, doc in enumerate(read_group_documents(group), start=1):
        stats.old_ps += len(doc.paragraphs)
        new_paragraphs = []
        new_seen_frequents = set()
        for p, text in enumerate(doc.paragraphs):
            mh = minhasher.minhash(text)
            # sorted(), because I don't know if the order is fixed and
            # it would be a bummer to "lose" two similar frequent paragraphs
            # in the loop below when a document contains the same p multiple
            # times
            duplicates = sorted(lsh.query(mh))
            if duplicates:
                # There are (frequent) duplicates: this p is frequent
                for dup in duplicates:
                    # But this is the first time seeing it: keep it.
                    if dup not in seen_frequents:
                        new_seen_frequents.add(dup)
                        new_paragraphs.append(text)
                        break
            else:
                # No duplicates: this p is not frequent
                new_paragraphs.append(text)
        # Update seen_frequents. We are doing it here because a document
        # can contain the same p multiple times, and (as in collect_frequent)
        # we aim to keep all of them.
        seen_frequents |= new_seen_frequents

        # Keep only documents with at least 1 non-frequent paragraph
        if new_paragraphs:
            stats.new_ps += len(new_paragraphs)
            stats.new_docs += 1
            doc.paragraphs = new_paragraphs
            yield doc

    stats.old_docs += doc_no
    logging.debug('Filtered frequent paragraphs from {}.'.format(domain))


def full_filter(group, args, queue):
    """Groups collect_frequent() and filter_paragraphs()."""
    minhasher = MinHasher(args.permutations, args.n)
    domain = urlsplit(group[0][0:group[0].find('\t')]).netloc
    freq_ps = collect_frequent(group, domain, minhasher, args.threshold,
                               1 - args.c, args.min_freq)
    stats = FilterStats(len(freq_ps))
    for doc in filter_paragraphs(group, domain, freq_ps,
                                 minhasher, args.threshold, stats):
        queue.put(doc)
    logging.debug('Found {} frequent paragraphs in domain {}, resulting in '
                  'documents {} -> {}, paragraphs {} -> {}.'.format(
                      stats.frequent, domain, stats.old_docs, stats.new_docs,
                      stats.old_ps, stats.new_ps))
    return stats


def main_filter(args):
    """The main function for filtering the documents."""
    install_mp_handler()

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    with Pool(args.processes) as pool:
        m = Manager()
        queue = m.Queue()
        f = partial(full_filter, args=args, queue=queue)
        res = pool.map_async(f, read_grouped_index(args.index))

        with closing(BatchWriter(args.documents,
                                 args.output_dir, args.zeroes)) as bw:
            while True:
                if queue.empty():
                    if res.ready():
                        break
                    time.sleep(1)  # I don't like Empty exceptions
                else:
                    doc = queue.get()
                    bw.write(doc)
                    queue.task_done()

        pool.close()
        pool.join()

        try:
            stats = reduce(lambda ss, s: ss + s, res.get())
            logging.info(
                'Done filtering; in total, found {} frequent paragraphs, '
                'resulting in documents {} -> {}, paragraphs {} -> {}.'.format(
                    stats.frequent, stats.old_docs, stats.new_docs,
                    stats.old_ps, stats.new_ps))
        except:
            logging.exception('Error while filtering')


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
    elif args.command == 'collect':
        main_collect2(args)
    elif args.command == 'filter':
        main_filter(args)

    logging.info('Done.')


if __name__ == '__main__':
    main()
