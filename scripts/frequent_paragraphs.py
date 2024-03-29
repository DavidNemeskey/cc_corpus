#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Writes the positions of all documents in each file."""

from argparse import ArgumentParser
from collections import Counter, deque
from contextlib import closing
from functools import partial
from itertools import accumulate, chain, groupby, islice
import logging
from multiprocessing import Manager, Pool, RLock
import os
import os.path as op
import sys
from typing import (
    Any, Callable, Dict, Generator, Iterable, Iterator, List, Set, Tuple, Union
)
from urllib.parse import urlsplit

from datasketch import MinHashLSH
from multiprocessing_logging import install_mp_handler

from cc_corpus.code import Filter
from cc_corpus.corpus import BatchWriter, Document, parse_file, parse_docs
from cc_corpus.deduplication import MinHasher
from cc_corpus.frequent import PData, RandomPDataReader
from cc_corpus.frequent import open as pdata_open
from cc_corpus.utils import grouper2, host_to_path, host_weight, openall, Stats


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--index', required=True,
                        help='the index file (output of the index task and '
                             'input to the rest.')
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
        help='Distributes the index file into separate files for running on '
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
                                help='the Jaccard similarity threshold for '
                                     'paragraph identity (0.9).')
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
    parser_collect.add_argument('--decay-filter', default='score < 0.5',
                                help='decay expression that is used to filter '
                                     'paragraphs after each step. The default '
                                     'purges those whose score < 0.5. The '
                                     'available variables are score and count.')
    parser_collect.add_argument('--wrap-filter', default='count >= min_freq',
                                help='expression that is used to filter '
                                     'paragraphs after all documents have been '
                                     'processed. Default is count >= min_freq; '
                                     'available variables are score, count, '
                                     'min_freq and docs.')
    parser_collect.add_argument('--bootstrap', '-b', default=None,
                                help='instead of starting from scratch, use an '
                                     'existing .pdata/.pdi pair to bootstrap '
                                     'the paragraph statistics. The original '
                                     'files are not modified, and the output '
                                     'will contain only the domains met in the '
                                     'data.')

    parser_filter = subparsers.add_parser(
        'filter_paragraphs', aliases=['filter'],
        help='Filters frequent paragraphs within a domain.'
    )
    parser_filter.set_defaults(command='filter')
    parser_filter.add_argument('--frequents', required=True,
                               help='the prefix to the frequent paragraph files '
                                    '(written by the `collect` task).')
    parser_filter.add_argument('--old-frequents', default=None,
                               help='the prefix to the "old" frequent paragraph '
                                    'files (written by the `collect` task). '
                                    'What "old" means in this case is that '
                                    'one instance of each paragraph is already '
                                    'part of the corpus, so all occurrences '
                                    'will be removed (as opposed to '
                                    '--frequents, where the first is kept).')
    parser_filter.add_argument(
        '--output-dir', '-o', required=True,
        help='the output directory. The *last directory* of the input path '
             'is replaced with the output directory; not all of it. This is '
             'because we expect that all corpus directories are next to each '
             'other; also, if the year is the path element before that, it '
             'will be kept intact.')
    parser_filter.add_argument('--documents', '-d', type=int, default=1000,
                               help='the number of documents an output file '
                                    'should contain (1000).')
    parser_filter.add_argument('--digits', '-z', type=int, default=4,
                               help='the number of digits in the output '
                                    'files\' names.')
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

    args = parser.parse_args()
    num_procs = len(os.sched_getaffinity(0))
    if args.processes < 1 or args.processes > num_procs:
        parser.error('Number of processes must be between 1 and {}'.format(
            num_procs))
    # Compute c from keep_for
    if args.command == 'collect' and args.keep_for:
        args.c = 1 - pow(0.5, 1 / (args.keep_for - 0.5))
    return args


# ----------------------------------- Types -----------------------------------


DocURL = str
DocFile = str
DocPos = int
DocLen = int
DocTuple = Tuple[DocURL, DocPos, DocLen]
DocFileTuple = Tuple[DocURL, DocFile, DocPos, DocLen]

IndexLine = str
DomainGroup = Tuple[str, List[IndexLine]]
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


def group_index(index_it: Iterable[str]) -> Iterator[DomainGroup]:
    """Reads the index file domain-group (of lines) by group."""
    for domain, group in groupby(
        index_it,
        key=lambda l: urlsplit(l[0:l.find('\t')]).netloc
    ):
        yield domain, list(group)


def main_distribute(args):
    """The main function for distributing the index file."""
    weights = [weight for _, weight in args.hosts]
    hosts = [openall(host_to_path(args.index, host), 'wt') for host, _ in args.hosts]
    lens = [0 for _ in weights]
    try:
        for _, group in group_index(read_index(args.index)):
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


def read_group_documents(group: Iterator[str]) -> Iterator[Document]:
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
            yield from parse_docs(f.read(int(doc_len)).decode('utf-8').split('\n'))
    finally:
        if f:
            f.close()


def minhash_group(group: List[IndexLine],
                  minhasher: MinHasher) -> List[Tuple[str, List[Any]]]:
    """
    Minhashes all paragraphs in a group of documents.

    In the whole fingerprinting / deduplication process, minhashing takes up
    most of the time. Hence, it has been extracted into a separate function so
    that it can be run parallelly. We pass a group of documents (instead of
    a single one and using `chunksize` in :func:`multiprocessing.imap`), because
    in this way, :func:`read_group_documents` might be able to run faster,
    using the fact that consecutive documents might belong to the same file.

    :param group: the lines that describe the documents; as read by
                  :func:`read_index`.
    :returns: a list of tuples of
        - the URL of the document (so that the caller knows what it gets back)
        - a list of paragraph fingerprints.
    """
    logging.debug('minhash_group({}) -> {}'.format(len(group), group[0]))
    return [(doc.attrs['url'], [minhasher.minhash(text)
                                for text in doc.paragraphs])
            for doc in read_group_documents(group)]


class LazyPool:
    """
    A version of :class:`multiprocessing.Pool` that limits the number of inputs
    being queued at the same time.
    At the beginning, ``processes * item_per_process`` inputs are sent
    to the pool. Further inputs are only sent if the previous ones have
    already been processed. This prevents outputs from being overgenerated
    and filling up the memory when the main processes cannot keep up.

    Note: ATM only :meth:`imap` is implemented.

    Note also that :class:`multiprocessing.Pool` cannot (easily?) be subclassed.
    So :class:`LazyPool` is not a subclass of :class:`multiprocessing.Pool`, just
    uses the latter. After Python 3.5, :class:`multiprocessing.pool.Pool` is
    exposed (or simply exists?), so it's a bit easier.
    """
    def __init__(self, processes=None, item_per_process=5,
                 initializer=None, initargs=(), maxtasksperchild=None):
        self.item_per_process = item_per_process
        self.max_items = item_per_process * processes
        self.pool = Pool(processes, initializer, initargs, maxtasksperchild)

    def __enter__(self):
        self.pool.__enter__()
        return self

    def __exit__(self, *args):
        return self.pool.__exit__(*args)

    def imap(self, func: Callable, iterable: Iterable) -> Generator[Tuple[Any, Any], None, None]:
        outputs = deque(maxlen=self.max_items)
        it = iter(iterable)
        # Add the first batch of inputs at the same time
        for inp in islice(it, self.max_items):
            outputs.append(self.pool.apply_async(func, (inp,)))
        # After that, add new input whenever one has been processed. We
        # wait for the first result to keep the order of the input and the
        # output corpus the same.
        for inp in it:
            outputs[0].wait()
            yield outputs.popleft().get()
            outputs.append(self.pool.apply_async(func, (inp,)))
        # Just consume the rest
        while outputs:
            outputs[0].wait()
            yield outputs.popleft().get()

    def __getattr__(self, name):
        return getattr(self.pool, name)


class FrequentCollector:
    """
    Parts of the frequent paragraph collection algorithm in
    :func:`collect_frequent` have been moved here to make the code more
    readable.
    """
    # The default (dummy) bootstrap tuple used when there is no bootstrap data
    BOOTSTRAP_TUPLE = (None, 0, [])

    def __init__(self, threshold: float, permutations: int, decay: float,
                 min_freq: int,
                 bootstrap: Union[RandomPDataReader, None] = None,
                 decay_filter: str = 'score < 0.5',
                 wrap_filter: int = 'count >= min_freq'):
        self.threshold = threshold
        self.permutations = permutations
        self.decay = decay
        self.min_freq = min_freq
        self.bootstrap = bootstrap or {}
        self.decay_filter = Filter(decay_filter)
        self.wrap_filter = Filter(wrap_filter)
        logging.debug('Decay filter: {}'.format(decay_filter))
        logging.debug('Wrap filter: {}'.format(wrap_filter))

    def reset(self, domain):
        """Resets the bookkeeping and statistics objects."""
        self.lsh = MinHashLSH(threshold=self.threshold,
                              num_perm=self.permutations)
        self.freq_ps = {}  # type: Dict[str, PData]
        self.num_dup = 0
        # Bootstrap the domain frequency counts if previous data is available
        _, docs, pdatas = self.bootstrap.get(domain, self.BOOTSTRAP_TUPLE)
        self.stats = CollectStats(domains=1, docs=docs)
        for pdata_id, pdata in enumerate(pdatas, start=1):
            self.lsh.insert(str(pdata_id), pdata.minhash)
            self.freq_ps[str(pdata_id)] = pdata

    def collect_from_doc(self, url: str, paragraphs: List[Any]):
        """
        Runs the algorithm in MMDS (TOOD) on a document, does the bookkeeping
        and updates the statistics in the object.

        :param url: the URL of the document (used as key in LSH).
        :param paragraphs: the minhashes of the paragraphs of the document.
        """
        # Step 1: decrease score of all paragraphs
        for pdata in self.freq_ps.values():
            pdata *= self.decay

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
        to_drop = [key for key, pdata in self.freq_ps.items()
                   if self.decay_filter(score=pdata.score, count=pdata.count)]
        for key in to_drop:
            self.freq_ps.pop(key)
            self.lsh.remove(key)

    def wrap_up_domain(self):
        """
        Drops all frequent candidates that are below the minimum frequency and
        updates the statistics.
        """
        # Get rid of paragraphs that only occured once
        self.freq_ps = {key: pdata for key, pdata in self.freq_ps.items()
                        if self.wrap_filter(score=pdata.score,
                                            count=pdata.count,
                                            min_freq=self.min_freq,
                                            docs=self.stats.docs)}
        self.stats.frequents = len(self.freq_ps)


def collect_frequent(
    it: Iterator[List[Tuple[str, List[Any]]]], threshold: float,
    permutations: int, decay: float, min_freq: int,
    decay_filter: str, wrap_filter: str, bootstrap_prefix: str = None
) -> Generator[Tuple[str, PDict], None, None]:  # noqa
    """
    Reads all the documents (as returned by :func:`minhash_group`) and
    collects the frequent paragraphs from them on a per-domain basis.

    TODO: reference to MMDS

    Yields (domain, `PDict`) tuples per domain.

    :param it: an iterator that yields documents as in :func:`minhash_group`;
               i.e. URL -- paragraph minhash list tuples.
    :param threshold: the Jaccard similarity threshold for paragraph identity.
    :param permutations: the number of permutations per paragraph.
    :param decay: the decay (multiplication) constant used for scoring
                  paraphraphs.
    :param min_freq: the minimum number of occurrence from which a paragraph
                     is deemed frequent.
    :param decay_filter: decay expression that is used to filter paragraphs
                         after each step.
    :param wrap_filter: expression that is used to filter paragraphs after all
                        documents have been processed.
    :param bootstrap_prefix: prefix of an existing .pdata/.pdi file pair to
                             bootstrap the domain frequency counts with.
    """
    curr_domain = None

    if bootstrap_prefix:
        bootstrap = RandomPDataReader(bootstrap_prefix)
        logging.debug('Bootstrap file prefix: {}'.format(bootstrap_prefix))
    else:
        bootstrap = None

    try:
        fc = FrequentCollector(threshold, permutations, decay, min_freq,
                               bootstrap, decay_filter, wrap_filter)
        # I don't want to write all the domain != curr_domain stuff twice, so
        # let's add a sentinel record to the end.
        for url, mhs in chain(chain.from_iterable(it), [('', [])]):
            domain = urlsplit(url).netloc

            # A new domain: yield results and re-initialize everything
            if domain != curr_domain:
                # Filtering and yielding results
                if curr_domain is not None:
                    fc.wrap_up_domain()

                    logging.debug('Finished collecting frequent paragraphs '
                                  'from {}...'.format(curr_domain))
                    if fc.freq_ps:
                        logging.debug('Found {} frequent paragraphs (duplicates: '
                                      '{}) in domain {} ({} documents).'.format(
                                          len(fc.freq_ps), fc.num_dup,
                                          curr_domain, fc.stats.docs))

                    # The domain is returned as well, so that we know what the
                    # input was
                    yield curr_domain, fc.freq_ps, fc.stats

                # Check for the sentinel
                if not domain:
                    break

                # Re-initialization
                logging.debug(
                    'Collecting frequent paragraphs from {}...'.format(domain))
                curr_domain = domain
                fc.reset(curr_domain)

            fc.collect_from_doc(url, mhs)
    finally:
        if bootstrap:
            bootstrap.close()


def main_collect(args):
    """
    The main function for collecting frequent paragraphs (and saving the
    results to file).
    """
    install_mp_handler()

    logging.info('Collecting frequent paragraphs from index {}...'.format(
        args.index))

    with pdata_open(args.output_prefix, 'w', sorting=True) as dataf:
        sum_stats = CollectStats()

        minhasher = MinHasher(args.permutations, args.n)
        with LazyPool(args.processes) as pool:
            it = pool.imap(partial(minhash_group, minhasher=minhasher),
                           grouper2(read_index(args.index), args.docs_per_batch))
            for domain, freq_ps, stats in collect_frequent(
                it, args.threshold, args.permutations, 1 - args.c, args.min_freq,
                args.decay_filter, args.wrap_filter, args.bootstrap
            ):
                if freq_ps:
                    dataf.write(domain, stats.docs,
                                *sorted(freq_ps.values(),
                                        key=lambda pd: -pd.count))
                sum_stats += stats

        pool.close()
        pool.join()

        logging.info('Collected frequent paragraphs from index {} '
                     'with statistics {}.'.format(args.index, sum_stats))


# -------------------------------- Filtering ----------------------------------


FilterStats = Stats.create(
    'old_ps', 'new_ps', 'old_docs', 'new_docs')  # type: Any


# RandomPDataReaders for each process
filter_frequents = None
filter_old_frequents = None


def init_filter(frequents: str, old_frequents: str):
    """
    Opens :data:`filter_frequents` and :data:`filter_old_frequents` if
    ``old_frequents`` is not ``None``.

    """
    global filter_frequents, filter_old_frequents
    filter_frequents = RandomPDataReader(frequents)
    if old_frequents:
        filter_old_frequents = RandomPDataReader(old_frequents)


def filter_file(file_id: int, index_lines: List[IndexLine], args: Any,
                frequents_seen: Dict[str, Any], lock: RLock):
    def seen_enough_of(domain: str, ps: List[int]) -> Set[int]:
        # https://stackoverflow.com/questions/9436757/how-does-multiprocessing-manager-work-in-python
        with lock:
            freq_counter = frequents_seen.setdefault(domain, Counter())
            freq_counter.update(ps)
            frequents_seen[domain] = freq_counter
            return set(p for p in ps if freq_counter[p] >= args.min_freq)

    sum_stats = FilterStats()
    minhasher = MinHasher(args.permutations, args.n)
    with closing(BatchWriter(sys.maxsize, args.output_dir,
                             args.digits, first_batch=file_id)) as bw:
        for domain, group in group_index(index_lines):
            logging.debug('Filtering domain {}...'.format(domain))
            stats = FilterStats()
            # Build the LSHs
            lsh = MinHashLSH(threshold=args.threshold,
                             num_perm=args.permutations)
            _, _, pdatas = filter_frequents.get(domain)
            for pdata_id, pdata in enumerate(pdatas, start=1):
                lsh.insert(str(pdata_id), pdata.minhash)
            old_lsh = MinHashLSH(threshold=args.threshold,
                                 num_perm=args.permutations)
            if (filter_old_frequents):
                _, _, pdatas = filter_old_frequents.get(domain)
                for pdata_id, pdata in enumerate(pdatas, start=1):
                    old_lsh.insert(str(pdata_id), pdata.minhash)

            doc_it = read_group_documents(group)
            if not lsh.keys and not old_lsh.keys:
                # There are no frequent paragraphs in the domain: just write
                # documents to file
                for doc_no, doc in enumerate(doc_it, start=1):
                    stats.old_ps += len(doc.paragraphs)
                    stats.new_ps += len(doc.paragraphs)
                    bw.write(doc)
                stats.old_docs += doc_no
                stats.new_docs += doc_no
                logging.debug('Domain {} did not require filtering; copied '
                              '{} documents and {} paragraphs.'.format(
                                  domain, doc_no, stats.old_ps))
            else:
                for doc_no, doc in enumerate(doc_it, start=1):
                    stats.old_docs += 1
                    stats.old_ps += len(doc.paragraphs)

                    minhashes = {p_id: minhasher.minhash(p) for p_id, p in
                                 enumerate(doc.paragraphs, start=1)}
                    # Just get rid of everything in old_frequents
                    old_frequents_found = {p_id for p_id, mh in minhashes.items()
                                           if old_lsh.query(mh)}
                    minhashes = {p_id: mh for p_id, mh in minhashes.items()
                                 if p_id not in old_frequents_found}
                    # And now deal with the "new" frequents. Remember, we have
                    # to keep each first occurrence
                    frequents_found = {}
                    for p_id, mh in minhashes.items():
                        for freq_id in lsh.query(mh):
                            frequents_found[p_id] = int(freq_id)
                            break
                    if frequents_found:
                        seen_enough = seen_enough_of(
                            domain, set(frequents_found.values()))
                        frequents_set = set(
                            p_id for p_id, freq_id in frequents_found.items()
                            if freq_id in seen_enough
                        ) | old_frequents_found
                        doc.paragraphs = [
                            p for p_id, p in enumerate(doc.paragraphs, start=1)
                            if p_id not in frequents_set
                        ]
                    if doc.paragraphs:
                        stats.new_docs += 1
                        stats.new_ps += len(doc.paragraphs)
                        bw.write(doc)
                logging.debug('Filtered domain {}, resulting in documents '
                              '{} -> {}, paragraphs {} -> {}.'.format(
                                  domain, stats.old_docs, stats.new_docs,
                                  stats.old_ps, stats.new_ps))
            sum_stats += stats
    return sum_stats


def main_filter(args):
    """The main function for filtering the documents."""
    install_mp_handler()

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    logging.info('Filtering frequent paragraphs from index {}...'.format(
        args.index))

    with Pool(args.processes, initializer=init_filter,
              initargs=[args.frequents, args.old_frequents]) as pool:
        m = Manager()
        frequents_seen = m.dict()
        lock = m.RLock()
        group_it = enumerate(grouper2(read_index(args.index), args.documents),
                             start=1)
        f = partial(filter_file, args=args,
                    frequents_seen=frequents_seen, lock=lock)

        sum_stats = FilterStats()
        for stats in pool.starmap(f, group_it):
            sum_stats += stats

        pool.close()
        pool.join()

        logging.info(
            'Done filtering: documents {} -> {}, paragraphs {} -> {}.'.format(
                sum_stats.old_docs, sum_stats.new_docs,
                sum_stats.old_ps, sum_stats.new_ps)
        )


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
        main_collect(args)
    elif args.command == 'filter':
        main_filter(args)

    logging.info('Done.')


if __name__ == '__main__':
    main()
