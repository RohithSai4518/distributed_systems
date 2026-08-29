"""
MemTable Implementation backed by a Concurrent SkipList.
Provides logarithmic O(log N) lookups, insertions, deletions, range scans,
and memory size tracking for high-throughput in-memory LSM ingestion.
"""

from dataclasses import dataclass
import random
import threading
from typing import Any, Iterator, List, Optional, Tuple


@dataclass
class SkipNode:
    key: str
    value: Optional[Any]  # None indicates a deleted key (tombstone)
    is_tombstone: bool = False
    timestamp_ns: int = 0
    forward: List[Optional['SkipNode']] = None

    def __post_init__(self):
        if self.forward is None:
            self.forward = []


class SkipList:
    """
    Probabilistic alternative to balanced trees (Pugh, 1990).
    Level assignment uses geometric distribution with p=0.5.
    """

    MAX_LEVEL = 16
    P = 0.5

    def __init__(self):
        self.head = SkipNode(key="", value=None, forward=[None] * self.MAX_LEVEL)
        self.level = 1
        self.size = 0
        self.byte_size = 0
        self._lock = threading.RLock()

    def _random_level(self) -> int:
        lvl = 1
        while random.random() < self.P and lvl < self.MAX_LEVEL:
            lvl += 1
        return lvl

    def put(self, key: str, value: Optional[Any], is_tombstone: bool = False, timestamp_ns: int = 0) -> int:
        """
        Inserts or updates a key-value pair in the skip list.
        Returns the delta in memory bytes added.
        """
        with self._lock:
            update = [None] * self.MAX_LEVEL
            current = self.head

            for i in range(self.level - 1, -1, -1):
                while current.forward[i] and current.forward[i].key < key:
                    current = current.forward[i]
                update[i] = current

            current = current.forward[0]

            # Key already exists -> update in place
            if current and current.key == key:
                old_bytes = len(str(current.value or ""))
                current.value = value
                current.is_tombstone = is_tombstone
                current.timestamp_ns = timestamp_ns
                new_bytes = len(str(value or ""))
                delta = new_bytes - old_bytes
                self.byte_size += delta
                return delta

            # New key insertion
            new_level = self._random_level()
            if new_level > self.level:
                for i in range(self.level, new_level):
                    update[i] = self.head
                self.level = new_level

            new_node = SkipNode(
                key=key,
                value=value,
                is_tombstone=is_tombstone,
                timestamp_ns=timestamp_ns,
                forward=[None] * new_level
            )

            for i in range(new_level):
                new_node.forward[i] = update[i].forward[i]
                update[i].forward[i] = new_node

            self.size += 1
            node_bytes = len(key.encode('utf-8')) + len(str(value or "").encode('utf-8')) + 64
            self.byte_size += node_bytes
            return node_bytes

    def get(self, key: str) -> Tuple[bool, Optional[Any], bool]:
        """
        Searches for a key.
        Returns: (found, value, is_tombstone)
        """
        with self._lock:
            current = self.head
            for i in range(self.level - 1, -1, -1):
                while current.forward[i] and current.forward[i].key < key:
                    current = current.forward[i]

            current = current.forward[0]
            if current and current.key == key:
                return True, current.value, current.is_tombstone
            return False, None, False

    def scan(self, start_key: str, end_key: Optional[str] = None, limit: int = 1000) -> List[Tuple[str, Any]]:
        """
        Ordered range scan from start_key (inclusive) to end_key (exclusive).
        Excludes tombstones.
        """
        results = []
        with self._lock:
            current = self.head
            for i in range(self.level - 1, -1, -1):
                while current.forward[i] and current.forward[i].key < start_key:
                    current = current.forward[i]

            current = current.forward[0]
            while current and len(results) < limit:
                if end_key and current.key >= end_key:
                    break
                if not current.is_tombstone:
                    results.append((current.key, current.value))
                current = current.forward[0]

        return results

    def iter_all(self) -> Iterator[SkipNode]:
        """Iterates over all nodes including tombstones in ascending key order."""
        with self._lock:
            current = self.head.forward[0]
            while current:
                yield current
                current = current.forward[0]


class MemTable:
    """
    In-Memory LSM Table.
    Wraps SkipList with immutable freezing support when capacity threshold is reached.
    """

    def __init__(self, capacity_bytes: int = 4 * 1024 * 1024):
        self.capacity_bytes = capacity_bytes
        self.skip_list = SkipList()
        self.is_frozen = False
        self._lock = threading.RLock()

    def put(self, key: str, value: Any, timestamp_ns: int = 0) -> bool:
        with self._lock:
            if self.is_frozen:
                return False
            self.skip_list.put(key, value, is_tombstone=False, timestamp_ns=timestamp_ns)
            return True

    def delete(self, key: str, timestamp_ns: int = 0) -> bool:
        with self._lock:
            if self.is_frozen:
                return False
            self.skip_list.put(key, None, is_tombstone=True, timestamp_ns=timestamp_ns)
            return True

    def get(self, key: str) -> Tuple[bool, Optional[Any], bool]:
        return self.skip_list.get(key)

    def scan(self, start_key: str, end_key: Optional[str] = None, limit: int = 1000) -> List[Tuple[str, Any]]:
        return self.skip_list.scan(start_key, end_key, limit)

    def is_full(self) -> bool:
        with self._lock:
            return self.skip_list.byte_size >= self.capacity_bytes

    def freeze(self):
        with self._lock:
            self.is_frozen = True

    def size_bytes(self) -> int:
        return self.skip_list.byte_size

    def count(self) -> int:
        return self.skip_list.size

    def iter_entries(self) -> Iterator[SkipNode]:
        return self.skip_list.iter_all()
