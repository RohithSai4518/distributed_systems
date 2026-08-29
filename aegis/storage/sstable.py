"""
Immutable Sorted String Table (SSTable) File Format.
Structure:
+-------------------------------------------------------------+
| DATA SECTION: Sequentially sorted Key-Value Record Blocks   |
+-------------------------------------------------------------+
| INDEX SECTION: Sparse Key Index (Key -> Data Offset)        |
+-------------------------------------------------------------+
| FILTER SECTION: Serialized Bloom Filter                     |
+-------------------------------------------------------------+
| FIXED TRAILER (40 Bytes):                                   |
|   Index Offset (8B) + Index Len (4B) + Filter Offset (8B)   |
|   + Filter Len (4B) + Entry Count (4B) + CRC (4B) + Magic(8B)|
+-------------------------------------------------------------+
"""

from dataclasses import dataclass
import os
import struct
import threading
from typing import Any, Dict, Iterator, List, Optional, Tuple

from aegis.common.crypto import HashAlgorithms
from aegis.storage.bloom_filter import BloomFilter


@dataclass
class IndexEntry:
    key: str
    offset: int
    length: int


class SSTableWriter:
    """Writes sorted key-value pairs into an immutable SSTable file."""

    MAGIC = b"AEGSSST1"
    TRAILER_FORMAT = "!QIQIII8s"  # 8 + 4 + 8 + 4 + 4 + 4 + 8 = 40 bytes
    TRAILER_SIZE = struct.calcsize(TRAILER_FORMAT)

    def __init__(self, file_path: str, index_interval: int = 16):
        self.file_path = file_path
        self.index_interval = index_interval
        self.entries_written = 0
        self.min_key = ""
        self.max_key = ""
        self.index_entries: List[IndexEntry] = []
        self.bloom_filter = BloomFilter(expected_elements=10000, false_positive_rate=0.01)

    def write_from_iter(self, entries_iter: Iterator[Any]):
        """
        Consumes an iterator of (key, value, is_tombstone, timestamp_ns) or SkipNodes.
        Requires entries to be sorted in strictly ascending key order.
        """
        os.makedirs(os.path.dirname(os.path.abspath(self.file_path)), exist_ok=True)
        temp_path = self.file_path + ".tmp"

        with open(temp_path, "wb") as f:
            # 1. Write Data Blocks
            for node in entries_iter:
                key = node.key if hasattr(node, 'key') else node[0]
                value = node.value if hasattr(node, 'value') else node[1]
                is_tombstone = node.is_tombstone if hasattr(node, 'is_tombstone') else (node[2] if len(node) > 2 else False)
                ts = node.timestamp_ns if hasattr(node, 'timestamp_ns') else (node[3] if len(node) > 3 else 0)

                if self.entries_written == 0:
                    self.min_key = key
                self.max_key = key

                # Update Bloom filter
                self.bloom_filter.add(key)

                # Record sparse index
                cur_offset = f.tell()
                if self.entries_written % self.index_interval == 0:
                    self.index_entries.append(IndexEntry(key=key, offset=cur_offset, length=0))

                key_bytes = key.encode('utf-8')
                val_str = "" if value is None else str(value)
                val_bytes = val_str.encode('utf-8')
                tombstone_flag = 1 if is_tombstone else 0

                # Entry format: KeyLen (2B) + ValLen (4B) + Tombstone (1B) + TS (8B) + KeyBytes + ValBytes
                rec_header = struct.pack("!HIBQ", len(key_bytes), len(val_bytes), tombstone_flag, ts)
                f.write(rec_header)
                f.write(key_bytes)
                f.write(val_bytes)

                self.entries_written += 1

            # 2. Write Sparse Index
            index_offset = f.tell()
            index_bytes_list = []
            for idx_entry in self.index_entries:
                k_bytes = idx_entry.key.encode('utf-8')
                index_bytes_list.append(struct.pack("!HQ", len(k_bytes), idx_entry.offset) + k_bytes)
            raw_index = b"".join(index_bytes_list)
            f.write(raw_index)
            index_len = len(raw_index)

            # 3. Write Bloom Filter
            filter_offset = f.tell()
            filter_bytes = self.bloom_filter.serialize()
            f.write(filter_bytes)
            filter_len = len(filter_bytes)

            # 4. Write 40-byte Fixed Trailer
            trailer_body = struct.pack("!QIQII", index_offset, index_len, filter_offset, filter_len, self.entries_written)
            crc = HashAlgorithms.crc32_custom(trailer_body)
            trailer = struct.pack(self.TRAILER_FORMAT, index_offset, index_len, filter_offset, filter_len, self.entries_written, crc, self.MAGIC)
            f.write(trailer)
            f.flush()

        # Atomic rename
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        os.rename(temp_path, self.file_path)


class SSTableReader:
    """Reads and performs binary searches on an SSTable file."""

    MAGIC = b"AEGSSST1"
    TRAILER_FORMAT = "!QIQIII8s"
    TRAILER_SIZE = struct.calcsize(TRAILER_FORMAT)  # 40 bytes

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.min_key = ""
        self.max_key = ""
        self.entry_count = 0
        self.index: List[Tuple[str, int]] = []
        self.bloom_filter: Optional[BloomFilter] = None
        self._lock = threading.RLock()
        self._load_metadata()

    def _load_metadata(self):
        with open(self.file_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            if file_size < self.TRAILER_SIZE:
                raise ValueError("Corrupt SSTable file: too small")

            # Seek to fixed trailer at the end
            f.seek(file_size - self.TRAILER_SIZE)
            trailer_bytes = f.read(self.TRAILER_SIZE)

            idx_off, idx_len, filt_off, filt_len, entries, crc, magic = struct.unpack(
                self.TRAILER_FORMAT, trailer_bytes
            )

            if magic != self.MAGIC:
                raise ValueError(f"Invalid SSTable magic header: {magic}")

            self.entry_count = entries
            self.index_offset = idx_off

            # Read Sparse Index
            f.seek(idx_off)
            raw_index = f.read(idx_len)
            self._parse_index(raw_index)

            # Read Bloom Filter
            f.seek(filt_off)
            raw_filter = f.read(filt_len)
            self.bloom_filter = BloomFilter.deserialize(raw_filter)

    def _parse_index(self, raw_index: bytes):
        offset = 0
        total_len = len(raw_index)
        while offset < total_len:
            k_len, data_off = struct.unpack("!HQ", raw_index[offset:offset+10])
            offset += 10
            k_bytes = raw_index[offset:offset+k_len]
            offset += k_len
            key = k_bytes.decode('utf-8', errors='replace')
            self.index.append((key, data_off))

        if self.index:
            self.min_key = self.index[0][0]
            self.max_key = self.index[-1][0]

    def get(self, key: str) -> Tuple[bool, Optional[Any], bool]:
        """
        Queries the SSTable for a key.
        1. Fast Bloom filter check
        2. Sparse index binary search to locate candidate data block
        3. Block scan to find exact key
        Returns: (found, value, is_tombstone)
        """
        # 1. Bloom filter check
        if self.bloom_filter and not self.bloom_filter.contains(key):
            return False, None, False

        # 2. Binary search index
        with self._lock:
            if not self.index:
                return False, None, False

            # If key is outside min_key
            if key < self.index[0][0]:
                return False, None, False

            low = 0
            high = len(self.index) - 1
            best_idx = 0

            while low <= high:
                mid = (low + high) // 2
                if self.index[mid][0] <= key:
                    best_idx = mid
                    low = mid + 1
                else:
                    high = mid - 1

            start_offset = self.index[best_idx][1]
            end_offset = self.index[best_idx + 1][1] if best_idx + 1 < len(self.index) else self.index_offset

            # 3. Read data block
            try:
                with open(self.file_path, "rb") as f:
                    f.seek(start_offset)
                    while True:
                        cur_pos = f.tell()
                        if end_offset and cur_pos >= end_offset:
                            break

                        header = f.read(15)  # H(2) + I(4) + B(1) + Q(8) = 15 bytes
                        if len(header) < 15:
                            break

                        k_len, v_len, tombstone, ts = struct.unpack("!HIBQ", header)
                        k_bytes = f.read(k_len)
                        v_bytes = f.read(v_len) if v_len > 0 else b""

                        cur_key = k_bytes.decode('utf-8', errors='replace')
                        if cur_key == key:
                            val = v_bytes.decode('utf-8', errors='replace') if v_len > 0 else None
                            return True, val, bool(tombstone)

                        if cur_key > key:
                            break  # Passed the target key in sorted sequence
            except (FileNotFoundError, OSError):
                return False, None, False

        return False, None, False

    def scan_all(self) -> Iterator[Tuple[str, Optional[str], bool, int]]:
        """Iterates sequentially through all entries in the SSTable."""
        try:
            with open(self.file_path, "rb") as f:
                entries_read = 0
                while entries_read < self.entry_count:
                    cur_pos = f.tell()
                    if cur_pos >= self.index_offset:
                        break

                    header = f.read(15)
                    if len(header) < 15:
                        break

                    k_len, v_len, tombstone, ts = struct.unpack("!HIBQ", header)
                    k_bytes = f.read(k_len)
                    v_bytes = f.read(v_len) if v_len > 0 else b""

                    key = k_bytes.decode('utf-8', errors='replace')
                    val = v_bytes.decode('utf-8', errors='replace') if v_len > 0 else None
                    entries_read += 1
                    yield (key, val, bool(tombstone), ts)
        except (FileNotFoundError, OSError):
            return
