"""
Practical Byzantine Fault Tolerance (PBFT) Consensus Engine
Based on Castro & Liskov (1999): Practical Byzantine Fault Tolerance.
Resilient against up to f Byzantine (arbitrary/malicious) failures in a 3f + 1 system.
Three-Phase Consensus:
1. Pre-Prepare: Primary broadcasts request with sequence number and view
2. Prepare: 2f + 1 matching prepare messages collected from peers
3. Commit: 2f + 1 matching commit messages collected from peers -> Execute
"""

import hashlib
import threading
import time
from typing import Dict, List, Optional, Tuple, Any, Set

class PBFTMessage:
    def __init__(self, msg_type: str, view: int, seq: int, digest: str, node_id: str):
        self.msg_type = msg_type
        self.view = view
        self.seq = seq
        self.digest = digest
        self.node_id = node_id

class PBFTNode:
    """PBFT replica state machine node."""
    def __init__(self, node_id: str, total_nodes: int = 4):
        self.node_id = node_id
        self.n = total_nodes
        self.f = (total_nodes - 1) // 3
        self.view = 0
        self.sequence_number = 0
        self.preprepares: Dict[Tuple[int, int], str] = {}
        self.prepares: Dict[Tuple[int, int], Set[str]] = {}
        self.commits: Dict[Tuple[int, int], Set[str]] = {}
        self.executed_sequence = 0
        self.lock = threading.RLock()

    def digest(self, message: Any) -> str:
        return hashlib.sha256(str(message).encode('utf-8')).hexdigest()

    def handle_preprepare(self, view: int, seq: int, digest: str) -> bool:
        with self.lock:
            if view != self.view or seq <= self.executed_sequence:
                return False
            self.preprepares[(view, seq)] = digest
            self.prepares.setdefault((view, seq), set()).add(self.node_id)
            return True

    def handle_prepare(self, view: int, seq: int, digest: str, sender: str) -> bool:
        with self.lock:
            if (view, seq) in self.preprepares and self.preprepares[(view, seq)] == digest:
                self.prepares.setdefault((view, seq), set()).add(sender)
                # Check prepared state: 2f + 1 prepares
                return len(self.prepares[(view, seq)]) >= (2 * self.f + 1)
            return False

    def handle_commit(self, view: int, seq: int, sender: str) -> bool:
        with self.lock:
            self.commits.setdefault((view, seq), set()).add(sender)
            return len(self.commits[(view, seq)]) >= (2 * self.f + 1)
