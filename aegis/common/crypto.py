"""
Cryptographic and Hashing Primitives for Distributed Systems.
Zero-dependency implementation of CRC32, FNV-1a, Murmur3-variant, and Bloom filter hash families.
"""

import struct
from typing import List, Union


class HashAlgorithms:
    """
    Standard and high-performance non-cryptographic hashing algorithms
    optimized for consistent hashing, partitioning, and checksum verification.
    """

    @staticmethod
    def fnv1a_64(data: Union[str, bytes]) -> int:
        """
        Computes 64-bit FNV-1a hash.
        Fast, uniform distribution over 64-bit integer space.
        """
        if isinstance(data, str):
            data = data.encode('utf-8')

        fnv_prime = 0x100000001B3
        hash_val = 0xCBF29CE484222325

        for byte in data:
            hash_val = (hash_val ^ byte) * fnv_prime
            hash_val &= 0xFFFFFFFFFFFFFFFF

        return hash_val

    @staticmethod
    def murmur3_32(data: Union[str, bytes], seed: int = 0) -> int:
        """
        32-bit MurmurHash3 implementation from scratch.
        High avalanche effect, exceptional key dispersion for Bloom Filters and Hash Rings.
        """
        if isinstance(data, str):
            data = data.encode('utf-8')

        length = len(data)
        n_blocks = length // 4
        h1 = seed

        c1 = 0xCC9E2D51
        c2 = 0x1B873593

        # Body
        for i in range(0, n_blocks * 4, 4):
            k1 = struct.unpack('<I', data[i:i+4])[0]

            k1 = (k1 * c1) & 0xFFFFFFFF
            k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
            k1 = (k1 * c2) & 0xFFFFFFFF

            h1 ^= k1
            h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
            h1 = ((h1 * 5) + 0xE6546B64) & 0xFFFFFFFF

        # Tail
        tail_idx = n_blocks * 4
        remaining = length & 3
        k1 = 0

        if remaining == 3:
            k1 ^= data[tail_idx + 2] << 16
        if remaining >= 2:
            k1 ^= data[tail_idx + 1] << 8
        if remaining >= 1:
            k1 ^= data[tail_idx]
            k1 = (k1 * c1) & 0xFFFFFFFF
            k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
            k1 = (k1 * c2) & 0xFFFFFFFF
            h1 ^= k1

        # Finalization
        h1 ^= length
        h1 ^= (h1 >> 16)
        h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
        h1 ^= (h1 >> 13)
        h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
        h1 ^= (h1 >> 16)

        return h1

    @staticmethod
    def crc32_custom(data: Union[str, bytes]) -> int:
        """
        CRC-32 checksum calculation with IEEE 802.3 polynomial 0xEDB88320.
        Detects burst errors and transmission bit flips in WAL and network packets.
        """
        if isinstance(data, str):
            data = data.encode('utf-8')

        # Precompute table on first use
        if not hasattr(HashAlgorithms, '_crc32_table'):
            table = []
            for i in range(256):
                curr = i
                for _ in range(8):
                    if curr & 1:
                        curr = 0xEDB88320 ^ (curr >> 1)
                    else:
                        curr >>= 1
                table.append(curr)
            HashAlgorithms._crc32_table = table

        crc = 0xFFFFFFFF
        table = HashAlgorithms._crc32_table
        for b in data:
            crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)

        return (crc ^ 0xFFFFFFFF) & 0xFFFFFFFF

    @staticmethod
    def generate_hash_family(data: Union[str, bytes], num_hashes: int) -> List[int]:
        """
        Generates 'k' independent hash values using double hashing technique:
        gi(x) = h1(x) + i * h2(x) (mod m)
        Ideal for space-efficient, zero-collision Bloom filter lookups.
        """
        if isinstance(data, str):
            data = data.encode('utf-8')

        h1 = HashAlgorithms.murmur3_32(data, seed=0x12345678)
        h2 = HashAlgorithms.fnv1a_64(data) & 0xFFFFFFFF

        hashes = []
        for i in range(num_hashes):
            combined = (h1 + i * h2 + i * i) & 0xFFFFFFFF
            hashes.append(combined)
        return hashes
