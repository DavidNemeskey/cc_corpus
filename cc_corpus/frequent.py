#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Stuff common to all scripts that handle frequent paragraphs
(frequent_paragraphs.py, merge_files.py).
"""

import io
from itertools import islice
import math
import pickle
import struct
from typing import Any, BinaryIO, Dict, List, Tuple, Union

from datasketch import LeanMinHash


class PData:
    """
    Data class that represents a paragraph, encapsulating all data needed by
    the algorithm that identifies frequent paragraphs.

    It defines two methods that the algorithm uses: *= to decay the score and
    += to increase both it and the count.
    """
    # The maximum value of count, so that it fits into 4 bytes
    MAX_COUNT = pow(2, 32) - 1

    def __init__(self, minhash: LeanMinHash, score: float = 1, count: int = 1):
        """
        The three fields are the minhash of the paragraph, its ever-decaying
        score and the total count (frequency).
        """
        if minhash is None:
            raise ValueError('PData: minhash cannot be None')
        self.minhash = minhash
        self.score = score
        self.count = count

    def __imul__(self, decay: float):
        """Decays the score."""
        self.score *= decay
        return self

    def __iadd__(self, count: int):
        """Increases both the score and count."""
        self.score += count
        self.count += count
        return self

    def __eq__(self, other):
        """Equates two minhash objects."""
        return (self.minhash == other.minhash and
                math.isclose(self.score, other.score, rel_tol=1e-6) and
                self.count == other.count)

    def __str__(self):
        return 'PData({}, {})'.format(self.score, self.count)

    def write_to(self, outs: BinaryIO):
        """
        Writes the object data to stream. This is used to save space compared
        to pickle.
        """
        outs.write(pickle.dumps(self.minhash))
        outs.write(struct.pack('!f', self.score))
        outs.write(struct.pack('!I', min(self.count, PData.MAX_COUNT)))
        # outs.write(min(self.count, PData.MAX_COUNT).to_bytes(4, byteorder='big'))

    @staticmethod
    def read_from(ins: BinaryIO) -> 'PData':
        """Reads PData instance data from a binary stream."""
        pd = PData(pickle.load(ins))
        pd.score = struct.unpack('!f', ins.read(4))[0]
        pd.count = struct.unpack('!I', ins.read(4))[0]
        return pd


class PDataIO:
    """
    Base class for PData I/O classes. Defines :meth:`~PDataIO.__init__`,
    :meth:`~PDataIO.close`, :meth:`~PDataIO.__enter__` and
    :meth:`~PDataIO.__exit__` (so subclasses can be used in a :keyword:`with`
    statement).
    """
    def __init__(self, prefix: str, mode: str):
        """
        Opens the file handles.

        :param prefix: the file name prefix. The two files opened are thus
                       ``prefix.pdi`` and ``prefix.pdata``.
        :param mode: the mode the files will be opened in: ``r``, ``w`` or
                     ``a``.
        """
        self.prefix = prefix
        self.pdi = io.open(self.prefix + '.pdi', mode + 't')
        self.pdata = io.open(self.prefix + '.pdata', mode + 'b')

    def close(self):
        """Closes both files."""
        if self.pdi:
            self.pdi.close()
            self.pdata.close()
            self.pdi = self.pdata = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        """Calls :meth:`~PDataIO.close`."""
        self.close()


class PDataReader(PDataIO):
    """Reads paragraph data from file(s), by paragraph or by domain."""
    def __init__(self, prefix: str):
        super().__init__(prefix, 'r')

    def read_index(self):
        """Iterates through the index file."""
        for line in self.pdi:
            domain, *tail = line.strip().split('\t')
            offset, length, num, docs = map(int, tail)
            yield domain, offset, length, num, docs

    def _inner_iter(self):
        for domain, offset, length, num, docs in self.read_index():
            yield domain, docs, num
            self.pdata.seek(offset)
            for _ in range(num):
                pdata = PData.read_from(self.pdata)
                yield pdata

    def iterate_p(self):
        """Returns the paragraph records one-by-one."""
        it = self._inner_iter()
        for domain, docs, num in it:
            for _ in range(num):
                yield (domain, docs, next(it))

    def iterate_domains(self):
        """Returns the paragraph records domain-by-domain."""
        it = self._inner_iter()
        for domain, docs, num in it:
            yield (domain, docs, list(islice(it, num)))

    __iter__ = iterate_domains  # Not sure about this


class RandomPDataReader(PDataIO):
    """
    Allows random indexed access to the :class:`PData` (and associated) values
    in the data.

    This class is not instantiable by :func:`open`.
    """
    def __init__(self, prefix: str):
        super().__init__(prefix, 'r')
        self.index = self._read_index()

    def _read_index(self) -> Dict[str, Tuple[int, int, int, int]]:
        """Reads the index into a dictionary."""
        ret = {}
        for line in self.pdi:
            domain, *tail = line.strip().split('\t')
            offset, length, num, docs = map(int, tail)
            ret[domain] = (offset, length, num, docs)
        return ret

    def get(self, key: str, default: List[PData] = []):
        try:
            return self[key]
        except KeyError:
            return []

    def __getitem__(self, key: str) -> List[PData]:
        offset, length, num, docs = self.index[key]
        self.pdata.seek(offset)
        return [PData.read_from(self.pdata) for _ in range(num)]

    def __setitem__(self, key: str, value: Any):
        raise NotImplementedError('Index values are not settable.')

    def __delitem__(self, key: str):
        del self.index[key]

    def __contains__(self, key: str) -> bool:
        return key in self.index


class PDataWriter(PDataIO):
    """
    Writes paragraph data to file(s), by paragraph or by domain.

    .. note::

       It is the caller's responsibility to present the records of a domain
       in after one another. Failing to do so will result in multiple index
       lines for the same domain and faulty behavior.
    """
    def __init__(self, prefix: str, append: bool = False, sorting: bool = False):
        """
        Opens the files in ``w`` or ``a`` mode, depending on ``append``.

        :param sorting: if ``True``, instead of writing the index to file right
                        away, it is collected in a list. Once
                        :meth:`~PDataWriter.close` is called, the list is sorted
                        and its contents written to the *.pdi* file.
        """
        super().__init__(prefix, 'a' if append else 'w')
        self.domain = None
        self.num = 0
        self.sorting = sorting
        self.offset = self.pdata.tell()
        if self.sorting:
            self._index = []

    def _finalize_domain(self):
        """"Finalizes" data from a domain: writes the index record."""
        if self.domain is not None:
            new_offset = self.pdata.tell()
            index_tuple = (self.domain, self.offset, new_offset - self.offset,
                           self.num, self.docs)
            if self.sorting:
                self._index.append(index_tuple)
            else:
                print('{}\t{}\t{}\t{}\t{}'.format(index_tuple), file=self.pdi)
            self.offset = new_offset

    def _check_new_domain(self, domain, docs):
        """Handles the case when we receive a new domain."""
        if domain != self.domain:
            self._finalize_domain()
            self.domain = domain
            self.docs = docs
            self.num = 0

    def write(self, domain: str, docs: int, *pdatas: PData):
        """Writes any number of single :class:`PData`s."""
        self._check_new_domain(domain, docs)
        for pdata in pdatas:
            pdata.write_to(self.pdata)
        self.num += len(pdatas)

    def close(self):
        self._finalize_domain()
        if self.sorting:
            self._index.sort()
            for index_tuple in self._index:
                print('{}\t{}\t{}\t{}\t{}'.format(index_tuple), file=self.pdi)
            del self._index
        super().close()


def open(prefix: str, mode: str = 'r') -> Union[PDataReader, PDataWriter]:
    """
    Opens a .pdi--.pdata pair for reading or writing, depending on `mode`.

    :param prefix: the file prefix of the `.pdi--.pdata` pair, without any of
                   the extensions.
    :param mode: Reading (*r*), writing (*w*) or appending(*a*).
    """
    if mode == 'r':
        return PDataReader(prefix)
    else:
        return PDataWriter(prefix, mode == 'a')
