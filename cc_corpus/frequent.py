#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Stuff common to all scripts that handle frequent paragraphs
(frequent_paragraphs.py, merge_files.py).
"""

import io
import math
import pickle
import struct
from typing import BinaryIO, Union

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
    def __init__(self, prefix: str):
        super().__init__(prefix, 'r')

    def __iter__(self):
        """Returns the paragraph records one-by-one."""
        for line in self.pdi:
            domain, *tail = line.strip().split('\t')
            offset, length, num, docs = map(int, tail)
            self.pdata.seek(offset)
            for _ in range(num):
                pdata = PData.read_from(self.pdata)
                yield (domain, docs, pdata)


class PDataWriter(PDataIO):
    """
    Writes paragraph data to file(s), by paragraph or by domain.

    .. note::

       It is the caller's responsibility to present the records of a domain
       in after one another. Failing to do so will result in multiple index
       lines for the same domain and faulty behavior.
    """
    def __init__(self, prefix: str, append: bool = False):
        """Opens the files in ``w`` or ``a`` mode, depending on ``append``."""
        super().__init__(prefix, 'a' if append else 'w')
        self.domain = None
        self.num = 0
        self.offset = self.pdata.tell()

    def _finalize_domain(self):
        """"Finalizes" data from a domain: writes the index record."""
        if self.domain is not None:
            new_offset = self.pdata.tell()
            print('{}\t{}\t{}\t{}\t{}'.format(self.domain, self.offset,
                                              new_offset - self.offset,
                                              self.num, self.docs),
                  file=self.pdi)
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
