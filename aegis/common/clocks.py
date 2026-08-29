"""
Distributed Logical Clocks & Causality Tracking.
Provides pure implementations of Lamport Timestamps, Vector Clocks,
and Hybrid Logical Clocks (HLC) for causal ordering and conflict detection.
"""

from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from typing import Any, Dict, Optional, Tuple


class CausalityRelation(Enum):
    HAPPENS_BEFORE = "HAPPENS_BEFORE"      # A -> B
    HAPPENS_AFTER = "HAPPENS_AFTER"        # B -> A
    CONCURRENT = "CONCURRENT"              # A || B (Conflict/divergence)
    EQUAL = "EQUAL"                        # A == B


@dataclass
class LamportClock:
    """
    Lamport Logical Clock: Maintains total ordering of events in a distributed system.
    Condition: If e1 -> e2 then L(e1) < L(e2).
    """
    node_id: str
    _counter: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def tick(self) -> int:
        """Increments clock on local event generation."""
        with self._lock:
            self._counter += 1
            return self._counter

    def update(self, received_time: int) -> int:
        """Updates clock upon receiving a remote event: L = max(L, received_L) + 1."""
        with self._lock:
            self._counter = max(self._counter, received_time) + 1
            return self._counter

    def read(self) -> int:
        with self._lock:
            return self._counter


@dataclass
class VectorClock:
    """
    Vector Clock: Tracks causal dependencies across N nodes in a distributed system.
    Enables exact detection of concurrent updates and partial ordering.
    """
    node_id: str
    clock_map: Dict[str, int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        if self.node_id not in self.clock_map:
            self.clock_map[self.node_id] = 0

    def tick(self) -> Dict[str, int]:
        """Increments current node's entry in the vector."""
        with self._lock:
            self.clock_map[self.node_id] = self.clock_map.get(self.node_id, 0) + 1
            return dict(self.clock_map)

    def update(self, remote_vector: Dict[str, int]) -> Dict[str, int]:
        """
        Merges remote vector into local:
        V_local[k] = max(V_local[k], V_remote[k]) for all k in union of keys.
        Then increments V_local[node_id].
        """
        with self._lock:
            all_nodes = set(self.clock_map.keys()).union(remote_vector.keys())
            for node in all_nodes:
                self.clock_map[node] = max(
                    self.clock_map.get(node, 0),
                    remote_vector.get(node, 0)
                )
            self.clock_map[self.node_id] = self.clock_map.get(self.node_id, 0) + 1
            return dict(self.clock_map)

    def clone_map(self) -> Dict[str, int]:
        with self._lock:
            return dict(self.clock_map)

    @staticmethod
    def compare(v1: Dict[str, int], v2: Dict[str, int]) -> CausalityRelation:
        """
        Compares two vector clocks to determine their causal relationship:
        - v1 < v2 if for all k, v1[k] <= v2[k] and exists k such that v1[k] < v2[k]
        - v1 > v2 if for all k, v1[k] >= v2[k] and exists k such that v1[k] > v2[k]
        - v1 == v2 if for all k, v1[k] == v2[k]
        - Otherwise v1 || v2 (concurrent)
        """
        all_keys = set(v1.keys()).union(v2.keys())
        v1_has_greater = False
        v2_has_greater = False

        for k in all_keys:
            val1 = v1.get(k, 0)
            val2 = v2.get(k, 0)

            if val1 > val2:
                v1_has_greater = True
            elif val2 > val1:
                v2_has_greater = True

        if v1_has_greater and not v2_has_greater:
            return CausalityRelation.HAPPENS_AFTER
        elif v2_has_greater and not v1_has_greater:
            return CausalityRelation.HAPPENS_BEFORE
        elif not v1_has_greater and not v2_has_greater:
            return CausalityRelation.EQUAL
        else:
            return CausalityRelation.CONCURRENT


@dataclass
class HybridLogicalClock:
    """
    Hybrid Logical Clock (HLC) (Kulkarni et al., 2014).
    Combines physical wall-clock time with logical counters to achieve:
    1. e -> e' => hlc(e) < hlc(e')
    2. Space overhead is O(1)
    3. Bound on drift from physical clock: |hlc.l - pt| <= max_drift
    """
    node_id: str
    l: int = 0  # Highest physical time observed
    c: int = 0  # Logical counter for events within same physical tick
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        self.l = time.time_ns()
        self.c = 0

    def now(self) -> Tuple[int, int]:
        """Generates next HLC timestamp for a local event."""
        with self._lock:
            pt = time.time_ns()
            if pt > self.l:
                self.l = pt
                self.c = 0
            else:
                self.c += 1
            return (self.l, self.c)

    def update(self, remote_l: int, remote_c: int) -> Tuple[int, int]:
        """Updates HLC upon receiving a remote timestamp."""
        with self._lock:
            pt = time.time_ns()
            prev_l = self.l
            self.l = max(prev_l, pt, remote_l)

            if self.l == prev_l == remote_l:
                self.c = max(self.c, remote_c) + 1
            elif self.l == prev_l:
                self.c += 1
            elif self.l == remote_l:
                self.c = remote_c + 1
            else:
                self.c = 0

            return (self.l, self.c)

    @staticmethod
    def compare(hlc1: Tuple[int, int], hlc2: Tuple[int, int]) -> int:
        """
        Total ordering comparison between two HLC timestamps:
        Returns: -1 if hlc1 < hlc2, 0 if hlc1 == hlc2, 1 if hlc1 > hlc2
        """
        if hlc1[0] < hlc2[0]:
            return -1
        elif hlc1[0] > hlc2[0]:
            return 1
        else:
            if hlc1[1] < hlc2[1]:
                return -1
            elif hlc1[1] > hlc2[1]:
                return 1
            else:
                return 0
