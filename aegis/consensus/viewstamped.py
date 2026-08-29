"""
Viewstamped Replication (VR Revisited) Protocol Implementation
Based on Oki & Liskov (1988) and Liskov & Cowling (2012).
Features:
- Primary-Backup Replication with View Numbers & Op Numbers
- Quorum-driven View Change Protocol
- State Transfer and Recovery Protocol
"""

import threading
import time
from typing import Dict, List, Optional, Tuple, Any

class VRNode:
    def __init__(self, node_id: str, configuration: List[str]):
        self.node_id = node_id
        self.config = configuration
        self.view_number = 0
        self.status = "NORMAL"
        self.op_number = 0
        self.commit_number = 0
        self.log: List[Tuple[int, int, Any]] = []  # (view, op_num, req)
        self.lock = threading.RLock()

    def is_primary(self) -> bool:
        return self.config[self.view_number % len(self.config)] == self.node_id

    def handle_prepare(self, view: int, op: int, req: Any) -> bool:
        with self.lock:
            if view != self.view_number or self.status != "NORMAL":
                return False
            self.op_number = op
            self.log.append((view, op, req))
            return True

    def handle_commit(self, view: int, commit_num: int):
        with self.lock:
            if view == self.view_number:
                self.commit_number = min(commit_num, self.op_number)
