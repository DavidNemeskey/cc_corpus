#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Functions (and classes) used in processing the index."""

from collections.abc import Generator, Iterable
from itertools import groupby
import json
import logging
from operator import attrgetter
from pathlib import Path
from typing import NamedTuple
import zlib

from more_itertools import batched

from cc_corpus.utils import openall


CLUSTER_SIZE = 3000


class Cluster(NamedTuple):
    """Represents a cluster in the cluster.idx file."""
    surt: str
    file_name: str
    offset: int
    length: int

    @classmethod
    def from_line(cls, line):
        fields = line.split('\t')[:4]
        params = [typ(data) for typ, data in
                  zip(cls.__annotations__.values(), fields)]
        return Cluster(*params)


class FileRange(NamedTuple):
    """
    Named tuple that represents a byte range that should be downloaded from
    a particular file.
    """
    file_name: str
    offset: int
    length: int


def read_cluster_idx(cluster_idx: Path) -> Generator[Cluster]:
    """Enumerates the clusters (lines) in a cluster.idx file."""
    with openall(cluster_idx, 'rt') as inf:
        for line in map(str.strip, inf):
            yield Cluster.from_line(line)


def compare_inverse_surt_lists(query_url, other_url) -> int:
    """
    This is a three-way comparison operator, that operates on urls,  given as
    inverted lists (e.g. ['hu','elte'] for elte.hu).
    The basis of the comparison is alphabetical sorting per domain components.
    It also returns 0 when the other_url is a subdomain of the query_url.
    """
    for query_element, other_element in zip(query_url, other_url):
        if query_element > other_element:
            return 1
        if query_element < other_element:
            return -1
    # If the iteration over the common sections was inconclusive, then
    # one of them is the subdomain of the other, or they are identical
    if len(query_url) > len(other_url):
        return 1
    return 0


def find_pattern_in_index(pattern: list[str],
                          cluster_idx: Path) -> list[Cluster]:
    """
    Finds all clusters that end in a particular TLD in cluster.idx.

    .. note::

        The cluster that comes before the first one with _tld_ is included in
        the list. This is because the URL in the index file is the first in
        a block of 3,000, so it is possible that the previous cluster already
        has URLs with the TLD in question.
    """
    last_cluster = None
    clusters = []
    logging.debug(f'Searching clusters for the pattern {pattern}...')
    for cluster in read_cluster_idx(cluster_idx):
        cl_ptn = cluster.surt[:cluster.surt.find(')')]
        comparison = compare_inverse_surt_lists(pattern, cl_ptn.split(','))
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


def collect_clusters_from_index(patterns_as_lists: list[list[str]],
                                cluster_idx: Path):
    clusters = set()
    for pattern_list in patterns_as_lists:
        clusters.update(find_pattern_in_index(pattern_list, cluster_idx))
    return sorted(clusters, key=lambda cluster: (cluster.file_name,
                                                 cluster.offset))


def ranges_from_clusters(
    clusters: list[Cluster], max_clusters: int = 0
) -> Generator[list[FileRange]]:
    """
    Creates :class:`FileRange`s from the clusters acquired via
    :func:`find_tld_in_index`.

    :param clusters: a list of clusters, ordered by SURT and offset.
    :param max_clusters: the maximum number of clusters in a batch. The
                         default is ``0``, meaning no limit (i.e. all
                         clusters are returned in a single batch).
    :return: yields lists of :class:`FileRange`s.
    """
    def range_from_clusters(file_name: str, file_clusters: Iterable[Cluster]):
        """
        Returns a single range for an :class:`Iterable` of :class:`Cluster`s.
        """
        start, end = None, None
        for cluster in file_clusters:
            if start is None:
                start = cluster.offset
                end = start + cluster.length
            else:
                if cluster.offset != end:
                    raise ValueError(f'Discontinuous cluster {cluster.surt}: '
                                     f'{cluster.offset=} instead of {end=}!')
                else:
                    end += cluster.length
        return FileRange(file_name, start, end - start)

    for file_name, file_clusters in groupby(clusters,
                                            key=attrgetter('file_name')):
        if max_clusters > 0:
            for batch in batched(file_clusters, max_clusters):
                yield range_from_clusters(file_name, batch)
        else:
            yield range_from_clusters(file_name, file_clusters)


class BatchWriter:
    """Writes index lines into a batch of files with consecutive numbering."""
    def __init__(self, batch_size, out_dir, digits=4, name_prefix=''):
        self.batch_size = batch_size
        self.out_dir = Path(out_dir)
        self.digits = digits
        self.name_prefix = name_prefix
        self.batch = 0
        self.outf = None
        self.lines_written = self.batch_size + 1  # so that we invoke new_file
        self.total_written = 0

    def write(self, index_line):
        """
        Writes a single index line to the currently open file. Opens a new file
        when the current one is full.
        """
        if self.lines_written >= self.batch_size:
            self.new_file()
        print(index_line, file=self.outf)
        self.lines_written += 1

    def new_file(self):
        """Closes the old file and opens a new one."""
        self.close()

        self.batch += 1
        new_file_name = f'{self.name_prefix}{{:0{self.digits}}}.gz'.format(
            self.batch
        )
        new_file = (self.out_dir / new_file_name).with_suffix('.gz')
        logging.debug('Opening file {}...'.format(new_file))
        self.outf = openall(new_file, 'wt')

    def close(self):
        """
        Closes the currently written file handle. Called automatically when
        the batch counter increases, but should also be called when processing
        ends to close the files of the last batch.
        """
        if self.outf is not None:
            self.outf.close()
            self.outf = None

            self.total_written += self.lines_written
        self.lines_written = 0

    def __del__(self):
        """Just calls close()."""
        self.close()


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
