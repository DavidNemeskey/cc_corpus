#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Stuff common to all scripts that handle frequent paragraphs
(frequent_paragraphs.py, merge_files.py).
"""

import math
import pickle
import struct
from cc_corpus.typing import BinaryIO

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
