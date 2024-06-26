#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Functions (and classes) used in processing the index."""

from bisect import bisect_left
from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass
from itertools import groupby
import json
import logging
from operator import attrgetter
from pathlib import Path
import re
import zlib

from more_itertools import batched, peekable

from cc_corpus.io import BatchWriterBase
from cc_corpus.utils import openall


CLUSTER_SIZE = 3000


class SurtDomain(tuple[str]):
    WWWP = re.compile('www[1-9]?')

    @staticmethod
    def from_string(domain: str) -> 'SurtDomain':
        surt_domain = domain.split('.')[::-1]
        if surt_domain[-1] == '*':
            surt_domain.pop()
        elif SurtDomain.WWWP.fullmatch(surt_domain[-1]):
            surt_domain.pop()
        return SurtDomain(surt_domain)


@dataclass(frozen=True)
class Cluster:
    """Represents a cluster in the cluster.idx file."""
    domain: SurtDomain
    path: str
    file_name: str
    offset: int
    length: int

    def surt(self):
        return f'{",".join(self.domain)})/{self.path}'

    def end(self):
        return self.offset + self.length

    @classmethod
    def from_line(cls, line):
        surt, file_name, offset, length = line.split('\t')[:4]
        domain, _, path = surt.partition(')/')
        return Cluster(SurtDomain(domain.split(',')), path,
                       file_name, int(offset), int(length))


@dataclass
class FileRange:
    """
    Represents a byte range that should be downloaded from a particular file.
    """
    file_name: str
    offset: int
    length: int


def read_cluster_idx(cluster_idx: Path) -> Generator[Cluster]:
    """Enumerates the clusters (lines) in a cluster.idx file."""
    with openall(cluster_idx, 'rt') as inf:
        for line in map(str.strip, inf):
            yield Cluster.from_line(line)


def compare_surt_domains(query: SurtDomain, other: SurtDomain) -> int:
    """
    This is a three-way comparison operator, that operates on urls,  given as
    inverted lists (e.g. ['hu','elte'] for elte.hu).
    The basis of the comparison is alphabetical sorting per domain components.
    It also returns 0 when the other_url is a subdomain of the query_url.
    """
    for query_element, other_element in zip(query, other):
        if query_element > other_element:
            return 1
        if query_element < other_element:
            return -1
    # If the iteration over the common sections was inconclusive, then
    # one of them is the subdomain of the other, or they are identical
    if len(query) > len(other):
        return 1
    return 0


def find_pattern_in_index_iterator(
    pattern: list[str], cluster_it: Iterator[Cluster]
) -> list[Cluster]:
    """
    Finds all clusters that match a SURT domain pattern in an iterator of
    cluster.idx :class:`Cluster`s.

    .. note::

        The cluster that comes before the first one with _tld_ is included in
        the list. This is because the URL in the index file is the first in
        a block of 3,000, so it is possible that the previous cluster already
        has URLs with the TLD in question.

    .. note::

        This function does a linear search in the iterator, and can be slow if
        the iterator is long (e.g. the whole cluster index file). For a faster
        version, see :func:`find_pattern_in_index`.
    """
    last_cluster = None
    clusters = []
    logging.debug(f'Searching clusters for the pattern {pattern}...')
    for cluster in cluster_it:
        comparison = compare_surt_domains(pattern, cluster.domain)
        if comparison > 0:
            last_cluster = cluster
        elif comparison <= 0:
            if last_cluster is not None:
                clusters.append(last_cluster)
                last_cluster = None
            if comparison == 0:
                clusters.append(cluster)
            else:
                break
    return clusters


def find_pattern_in_index(
    pattern: list[str], cluster_index: list[Cluster]
) -> list[Cluster]:
    """
    Finds all clusters that match a SURT domain pattern in a list of
    cluster.idx :class:`Cluster`s.

    .. note::

        The cluster that comes before the first one with _tld_ is included in
        the list. This is because the URL in the index file is the first in
        a block of 3,000, so it is possible that the previous cluster already
        has URLs with the TLD in question.

    .. note::

        This function requires that the whole cluster.idx be loaded into memory
        (into the _cluster_idx_ argument). This allows the use of binary search
        for optimal performance; however, if memory usage is a concern, the
        function :func:`find_pattern_in_index_iterator` should be used instead.
    """
    logging.debug(f'Searching clusters for the pattern {pattern}...')
    cluster_index = list(cluster_index)
    idx = bisect_left(cluster_index, pattern, key=attrgetter('domain'))
    # The domain might start in the middle of the previous cluster
    if idx != 0:
        idx = idx - 1
    clusters = []
    for cluster in cluster_index[idx:]:
        if compare_surt_domains(pattern, cluster.domain) < 0:
            break
        clusters.append(cluster)
    return clusters


def collect_clusters_from_index(
    patterns: list[SurtDomain], cluster_idx: Path
) -> set[Cluster]:
    """Collects the index clusters that match the specified patterns."""
    if len(patterns) == 1:
        return find_pattern_in_index(patterns[0],
                                     read_cluster_idx(cluster_idx))
    else:
        cluster_index = list(read_cluster_idx(cluster_idx))
        clusters = set()
        for pattern in patterns:
            clusters.update(find_pattern_in_index(pattern, cluster_index))
        return sorted(clusters, key=lambda cluster: (cluster.file_name,
                                                     cluster.offset))


def ranges_from_clusters(
    clusters: list[Cluster], max_clusters: int = 0
) -> Generator[list[FileRange]]:
    """
    Creates :class:`FileRange`s from the clusters acquired via
    :func:`find_pattern_in_index` or :func:`find_pattern_in_index_iterator`.

    :param clusters: a list of clusters, ordered by SURT and offset.
    :param max_clusters: the maximum number of clusters in a batch. The
                         default is ``0``, meaning no limit (i.e. all
                         clusters are returned in a single batch).
    :return: yields lists of :class:`FileRange`s.
    """
    def group_continuous_file_clusters(
        file_clusters: Iterable[Cluster]
    ) -> Generator[list[Cluster]]:
        """
        Groups continuous clusters that belong to the same index file (not
        checked). Since an index file may contain clusters corresponding to
        more than one domains, there is no guarantee that the byte ranges
        corresponding to these clusters are continuous. This function groups
        the clusters in continuous chunks so that they can be converted into
        byte ranges for download.
        """
        continuous_clusters = []
        for cluster in (it := peekable(file_clusters)):
            if not continuous_clusters:
                continuous_clusters.append(cluster)
            else:
                if cluster.offset != continuous_clusters[-1].end():
                    yield continuous_clusters
                    continuous_clusters = []
                    it.prepend(cluster)
                else:
                    continuous_clusters.append(cluster)
        if continuous_clusters:
            yield continuous_clusters

    def range_from_clusters(
        file_name: str, file_clusters: Iterable[Cluster]
    ) -> FileRange:
        """
        Returns a single range for an :class:`Iterable` of :class:`Cluster`s.
        """
        return FileRange(file_name, file_clusters[0].offset,
                         file_clusters[-1].end() - file_clusters[0].offset)

    for file_name, file_clusters in groupby(clusters,
                                            key=attrgetter('file_name')):
        for range_clusters in group_continuous_file_clusters(file_clusters):
            if max_clusters > 0:
                for batch in batched(range_clusters, max_clusters):
                    yield range_from_clusters(file_name, batch)
            else:
                yield range_from_clusters(file_name, range_clusters)


class BatchWriter(BatchWriterBase):
    """Writes index lines into a batch of files with consecutive numbering."""
    def __init__(self, batch_size, out_dir, digits=4, name_prefix=''):
        super().__init__(batch_size, out_dir, digits, name_prefix, first_batch=0)


def process_index_range(index_range: bytes) -> Generator[str]:
    """
    Processes the index range downloaded by
    :func:`cc_corpus.download.download_index_range` and yields the index lines
    it contains.

    Correctly parsing the index range is not trivial: it is a multi-stream
    zlib data, with each cluster (3,000 lines) in a separate stream.
    """
    while True:
        dobj = zlib.decompressobj(zlib.MAX_WBITS | 32)
        decompressed = dobj.decompress(index_range)
        for line in map(bytes.strip, decompressed.split(b'\n')):
            if line:
                yield line.decode('utf-8')
        if len(dobj.unused_data) > 0:
            index_range = dobj.unused_data
        else:
            break


def filter_json(index_line: str, field_list: set[str]) -> str:
    """Filters the JSON fields from _index_line_ not listed in _field_list_."""
    surt, timestamp, json_str = index_line.split(' ', 2)
    json_obj = {field: value for field, value in json.loads(json_str).items()
                if field in field_list}
    return f'{surt} {timestamp} {json.dumps(json_obj, ensure_ascii=False)}'
