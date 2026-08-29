"""
Space-Efficient Bloom Filter Implementation.
Zero-dependency probabilistic set membership tester for avoiding disk SSTable reads.
Optimal bit size m = -(n * ln(p)) / (ln(2)^2), optimal hashes k = (m/n) * ln(2).
"""

import math
from typing import List, Union

from aegis.common.crypto import HashAlgorithms


class BloomFilter:
    """
    Bit array-based Bloom Filter for fast negative key existence checks.
    False positive rate p, guaranteed zero false negatives.
    """

    def __init__(self, expected_elements: int = 10000, false_positive_rate: float = 0.01):
        self.expected_elements = max(10, expected_elements)
        self.fpr = false_positive_rate

        # Calculate optimal size (m) and hash functions count (k)
        # m = - (n * ln(p)) / (ln(2)^2)
        ln2_sq = (math.log(2) ** 2)
        self.bit_size = int(-1.0 * (self.expected_elements * math.log(self.fpr)) / ln2_sq)
        self.bit_size = max(64, (self.bit_size + 7) & ~7)  # Round up to whole bytes

        # k = (m / n) * ln(2)
        self.num_hashes = max(1, int((self.bit_size / self.expected_elements) * math.log(2)))

        self.num_bytes = self.bit_size // 8
        self.bitset = bytearray(self.num_bytes)
        self.count = 0

    def add(self, key: Union[str, bytes]):
        """Adds an element to the filter."""
        hashes = HashAlgorithms.generate_hash_family(key, self.num_hashes)
        for h in hashes:
            bit_idx = h % self.bit_size
            byte_idx = bit_idx // 8
            bit_offset = bit_idx % 8
            self.bitset[byte_idx] |= (1 << bit_offset)
        self.count += 1

    def contains(self, key: Union[str, bytes]) -> bool:
        """
        Tests whether an element is possibly in the set or definitely not.
        Returns False -> Key is 100% NOT in the SSTable (Saves expensive disk seek).
        Returns True  -> Key might be in the SSTable (Check sparse index and read).
        """
        hashes = HashAlgorithms.generate_hash_family(key, self.num_hashes)
        for h in hashes:
            bit_idx = h % self.bit_size
            byte_idx = bit_idx // 8
            bit_offset = bit_idx % 8
            if not (self.bitset[byte_idx] & (1 << bit_offset)):
                return False
        return True

    def serialize(self) -> bytes:
        """Serializes the filter configuration and bitset to bytes."""
        import struct
        header = struct.pack("!IIIf", self.bit_size, self.num_hashes, self.count, self.fpr)
        return header + bytes(self.bitset)

    @classmethod
    def deserialize(cls, data: bytes) -> "BloomFilter":
        """Reconstructs BloomFilter from byte stream."""
        import struct
        header_len = struct.calcsize("!IIIf")
        bit_size, num_hashes, count, fpr = struct.unpack("!IIIf", data[:header_len])

        bf = cls.__new__(cls)
        bf.bit_size = bit_size
        bf.num_hashes = num_hashes
        bf.count = count
        bf.fpr = fpr
        bf.num_bytes = bit_size // 8
        bf.expected_elements = count
        bf.bitset = bytearray(data[header_len:header_len + bf.num_bytes])
        return bf
