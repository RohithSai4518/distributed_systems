"""
Adaptive Replacement Cache (ARC) & Multi-Policy Block Cache
Combines LRU and LFU dynamics with real-time target tuning (Megiddo & Modha, 2003).
Features:
- Ghost cache tracking for frequency vs recency balancing
- Lock-sharded concurrency for multi-threaded cache hits
- Memory footprint budgeting
"""

import threading
import time
from collections import OrderedDict
from typing import Any, Optional, Tuple, Dict

class ShardedBlockCache:
    """Lock-sharded multi-tier cache to prevent mutex contention on hot paths."""
    def __init__(self, capacity_bytes: int = 64 * 1024 * 1024, num_shards: int = 16):
        self.num_shards = num_shards
        self.shard_capacity = capacity_bytes // num_shards
        self.shards = [ARCCache(self.shard_capacity) for _ in range(num_shards)]

    def _get_shard(self, key: str) -> 'ARCCache':
        h = hash(key)
        return self.shards[h % self.num_shards]

    def get(self, key: str) -> Optional[Any]:
        return self._get_shard(key).get(key)

    def put(self, key: str, value: Any, cost: int = 1):
        self._get_shard(key).put(key, value, cost)

    def remove(self, key: str):
        self._get_shard(key).remove(key)

    def clear(self):
        for s in self.shards:
            s.clear()

    def stats(self) -> Dict[str, Any]:
        hits = sum(s.hits for s in self.shards)
        misses = sum(s.misses for s in self.shards)
        entries = sum(len(s) for s in self.shards)
        ratio = (hits / (hits + misses)) if (hits + misses) > 0 else 0.0
        return {
            "hits": hits,
            "misses": misses,
            "hit_ratio": ratio,
            "entries": entries,
            "num_shards": self.num_shards
        }


class ARCCache:
    """
    Adaptive Replacement Cache (ARC).
    Maintains 4 LRU lists:
    - T1: Recent cache entries
    - T2: Frequent cache entries
    - B1: Ghost cache of evicted recent entries (metadata only)
    - B2: Ghost cache of evicted frequent entries (metadata only)
    """
    def __init__(self, capacity: int = 1000):
        self.c = capacity
        self.p = 0.0  # Target size of T1
        self.t1: OrderedDict[str, Tuple[Any, int]] = OrderedDict()
        self.t2: OrderedDict[str, Tuple[Any, int]] = OrderedDict()
        self.b1: OrderedDict[str, int] = OrderedDict()
        self.b2: OrderedDict[str, int] = OrderedDict()
        self.lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def __len__(self) -> int:
        return len(self.t1) + len(self.t2)

    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if key in self.t1:
                val, cost = self.t1.pop(key)
                self.t2[key] = (val, cost)
                self.hits += 1
                return val
            elif key in self.t2:
                val, cost = self.t2.pop(key)
                self.t2[key] = (val, cost)
                self.hits += 1
                return val
            self.misses += 1
            return None

    def put(self, key: str, value: Any, cost: int = 1):
        with self.lock:
            # Case 1: Key already in T1 or T2
            if key in self.t1:
                self.t1.pop(key)
                self.t2[key] = (value, cost)
                return
            if key in self.t2:
                self.t2.pop(key)
                self.t2[key] = (value, cost)
                return

            # Case 2: Key is in ghost cache B1 (Recency hit)
            if key in self.b1:
                delta = 1.0 if len(self.b1) >= len(self.b2) else float(len(self.b2)) / float(len(self.b1))
                self.p = min(float(self.c), self.p + delta)
                self._replace(key)
                self.b1.pop(key)
                self.t2[key] = (value, cost)
                return

            # Case 3: Key is in ghost cache B2 (Frequency hit)
            if key in self.b2:
                delta = 1.0 if len(self.b2) >= len(self.b1) else float(len(self.b1)) / float(len(self.b2))
                self.p = max(0.0, self.p - delta)
                self._replace(key)
                self.b2.pop(key)
                self.t2[key] = (value, cost)
                return

            # Case 4: Complete cache miss
            if len(self.t1) + len(self.b1) == self.c:
                if len(self.t1) < self.c:
                    if self.b1:
                        self.b1.popitem(last=False)
                    self._replace(key)
                else:
                    if self.t1:
                        self.t1.popitem(last=False)
            elif len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) >= 2 * self.c:
                if len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) >= 2 * self.c:
                    if self.b2:
                        self.b2.popitem(last=False)
                self._replace(key)

            self.t1[key] = (value, cost)

    def _replace(self, key: str):
        if self.t1 and ((key in self.b2 and len(self.t1) == int(self.p)) or (len(self.t1) > self.p)):
            old_k, (old_v, c) = self.t1.popitem(last=False)
            self.b1[old_k] = c
        else:
            if self.t2:
                old_k, (old_v, c) = self.t2.popitem(last=False)
                self.b2[old_k] = c

    def remove(self, key: str):
        with self.lock:
            self.t1.pop(key, None)
            self.t2.pop(key, None)
            self.b1.pop(key, None)
            self.b2.pop(key, None)

    def clear(self):
        with self.lock:
            self.t1.clear()
            self.t2.clear()
            self.b1.clear()
            self.b2.clear()
            self.p = 0.0
