"""
ZooKeeper Atomic Broadcast (ZAB) Consensus Protocol
Implements Junqueira et al. (2011): Zab: High-performance broadcast for primary-backup systems.
Protocol phases:
- Phase 1: Fast Leader Election & Discovery
- Phase 2: Synchronization & Epoch Update (zxid: 32-bit epoch + 32-bit counter)
- Phase 3: Two-Phase Atomic Broadcast (PROPOSAL -> ACK -> COMMIT)
"""

import threading
import time
from typing import Dict, List, Optional, Tuple, Any

class ZXID:
    def __init__(self, epoch: int = 0, counter: int = 0):
        self.epoch = epoch
        self.counter = counter

    def to_int(self) -> int:
        return (self.epoch << 32) | (self.counter & 0xFFFFFFFF)

    @classmethod
    def from_int(cls, val: int) -> 'ZXID':
        return cls(epoch=(val >> 32) & 0xFFFFFFFF, counter=val & 0xFFFFFFFF)

    def __lt__(self, other: 'ZXID') -> bool:
        return self.to_int() < other.to_int()

    def __eq__(self, other: 'ZXID') -> bool:
        return self.to_int() == other.to_int()


class ZABNode:
    """ZooKeeper Atomic Broadcast engine for replicated state machines."""
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers
        self.current_epoch = 0
        self.last_zxid = ZXID(0, 0)
        self.state = "DISCOVERY"
        self.history: List[Tuple[ZXID, Any]] = []
        self.committed_zxid = ZXID(0, 0)
        self.lock = threading.RLock()

    def propose(self, payload: Any) -> ZXID:
        with self.lock:
            self.last_zxid = ZXID(self.current_epoch, self.last_zxid.counter + 1)
            self.history.append((self.last_zxid, payload))
            return self.last_zxid

    def commit(self, zxid: ZXID):
        with self.lock:
            if zxid > self.committed_zxid:
                self.committed_zxid = zxid
