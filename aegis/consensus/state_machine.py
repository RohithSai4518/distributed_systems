"""
Replicated State Machine Interface.
Applies committed Raft log entries to the underlying LSM Storage Engine,
guaranteeing deterministic linearizable state across all replica nodes.
"""

from dataclasses import dataclass
import threading
from typing import Any, Callable, Dict, Optional, Tuple

from aegis.common.logger import Logger
from aegis.common.types import LogEntry, OpType
from aegis.storage.engine import LSMEngine


class ReplicatedStateMachine:
    """
    Applies committed entries in strict index order to LSMEngine.
    """

    def __init__(self, node_id: str, storage_engine: LSMEngine):
        self.node_id = node_id
        self.storage = storage_engine
        self.last_applied_index = 0
        self.logger = Logger(node_id=f"{node_id}:SM")
        self._lock = threading.RLock()
        self._client_pending_responses: Dict[int, Any] = {}

    def apply_entry(self, entry: LogEntry) -> Any:
        """
        Executes a committed log entry on the storage engine.
        Returns result of operation (e.g. previous value or success status).
        """
        with self._lock:
            if entry.index <= self.last_applied_index:
                return None  # Already applied

            result = None
            if entry.op_type == OpType.PUT:
                self.storage.put(entry.key, entry.value)
                result = {"status": "OK", "key": entry.key, "value": entry.value}
            elif entry.op_type == OpType.DELETE:
                self.storage.delete(entry.key)
                result = {"status": "OK", "deleted_key": entry.key}
            elif entry.op_type == OpType.CAS:
                found, current_val = self.storage.get(entry.key)
                if (not found and entry.prev_value is None) or (found and str(current_val) == str(entry.prev_value)):
                    self.storage.put(entry.key, entry.value)
                    result = {"status": "OK", "cas_success": True, "new_value": entry.value}
                else:
                    result = {"status": "CAS_FAILED", "cas_success": False, "current_value": current_val}
            elif entry.op_type == OpType.NOOP:
                result = {"status": "NOOP"}

            self.last_applied_index = entry.index
            return result

    def get_applied_index(self) -> int:
        with self._lock:
            return self.last_applied_index
