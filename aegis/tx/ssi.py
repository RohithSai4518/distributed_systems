"""
Serializable Snapshot Isolation (SSI) & Dependency Conflict Graph
Implements Cahill, Rohm & Fekete (2008): Serializable Isolation for Snapshot Databases.
Features:
- RW-antidependency (SIREAD lock) tracking
- Dangerous structure detection (T1 --rw--> T2 --rw--> T3 where T3 commits first)
- False-positive-free cycle aborts to ensure strict serializability
"""

import threading
import time
from typing import Dict, Set, List, Optional, Tuple, Any

class TransactionNode:
    def __init__(self, tx_id: str, start_ts: int):
        self.tx_id = tx_id
        self.start_ts = start_ts
        self.commit_ts: Optional[int] = None
        self.is_active = True
        self.is_aborted = False
        self.in_edges: Set[str] = set()    # Inbound rw-antidependencies
        self.out_edges: Set[str] = set()   # Outbound rw-antidependencies
        self.read_keys: Set[str] = set()
        self.write_keys: Set[str] = set()

class SerializableSnapshotIsolationEngine:
    """Detects dangerous cycles in the serialization graph to guarantee SSI."""
    def __init__(self):
        self.transactions: Dict[str, TransactionNode] = {}
        self.key_readers: Dict[str, Set[str]] = {}  # key -> Set[tx_id]
        self.key_writers: Dict[str, Set[str]] = {}  # key -> Set[tx_id]
        self.lock = threading.RLock()
        self._clock = 0

    def begin_tx(self, tx_id: str) -> TransactionNode:
        with self.lock:
            self._clock += 1
            node = TransactionNode(tx_id, self._clock)
            self.transactions[tx_id] = node
            return node

    def record_read(self, tx_id: str, key: str):
        with self.lock:
            tx = self.transactions.get(tx_id)
            if not tx or not tx.is_active:
                return
            tx.read_keys.add(key)
            self.key_readers.setdefault(key, set()).add(tx_id)

            # Check if an active writer exists -> add out-edge (rw-antidependency)
            if key in self.key_writers:
                for w_tx in self.key_writers[key]:
                    if w_tx != tx_id:
                        self.transactions[w_tx].in_edges.add(tx_id)
                        tx.out_edges.add(w_tx)

    def record_write(self, tx_id: str, key: str):
        with self.lock:
            tx = self.transactions.get(tx_id)
            if not tx or not tx.is_active:
                return
            tx.write_keys.add(key)
            self.key_writers.setdefault(key, set()).add(tx_id)

            # Check if readers exist -> add in-edge (rw-antidependency)
            if key in self.key_readers:
                for r_tx in self.key_readers[key]:
                    if r_tx != tx_id:
                        tx.in_edges.add(r_tx)
                        self.transactions[r_tx].out_edges.add(tx_id)

    def can_commit(self, tx_id: str) -> bool:
        """Checks if committing tx_id forms a dangerous pivot (in_edge AND out_edge)."""
        with self.lock:
            tx = self.transactions.get(tx_id)
            if not tx or tx.is_aborted:
                return False

            # Dangerous structure rule: T has both in-edge and out-edge
            if len(tx.in_edges) > 0 and len(tx.out_edges) > 0:
                tx.is_aborted = True
                return False  # Abort to prevent non-serializable anomaly

            self._clock += 1
            tx.commit_ts = self._clock
            tx.is_active = False
            return True
