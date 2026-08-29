"""
Durable Raft Log Store.
Maintains continuous indexed log entries, supports log truncation on conflict,
and tracks base snapshot index and term.
"""

from dataclasses import dataclass
import json
import os
import threading
from typing import Any, Dict, List, Optional

from aegis.common.types import LogEntry, OpType


class RaftLogStore:
    """
    1-indexed Raft log store with durable persistence.
    Log entries: index 1, 2, ..., N.
    Index 0 is the dummy base entry.
    """

    def __init__(self, log_dir: str, node_id: str):
        self.log_dir = log_dir
        self.node_id = node_id
        self.log_file = os.path.join(log_dir, f"raft_log_{node_id}.jsonl")
        self.state_file = os.path.join(log_dir, f"raft_state_{node_id}.json")

        os.makedirs(log_dir, exist_ok=True)

        # Base index/term before snapshot
        self.snapshot_index = 0
        self.snapshot_term = 0

        # Entries array (index 0 is dummy)
        self.entries: List[LogEntry] = [
            LogEntry(term=0, index=0, op_type=OpType.NOOP, key="")
        ]

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self._lock = threading.RLock()

        self._load_state()

    def _load_state(self):
        with self._lock:
            # 1. Load term & voted_for
            if os.path.exists(self.state_file):
                try:
                    with open(self.state_file, "r") as f:
                        data = json.load(f)
                        self.current_term = data.get("current_term", 0)
                        self.voted_for = data.get("voted_for")
                        self.snapshot_index = data.get("snapshot_index", 0)
                        self.snapshot_term = data.get("snapshot_term", 0)
                except Exception:
                    pass

            # 2. Load log entries
            if os.path.exists(self.log_file):
                try:
                    loaded_entries = [self.entries[0]]
                    with open(self.log_file, "r") as f:
                        for line in f:
                            if line.strip():
                                entry_dict = json.loads(line)
                                loaded_entries.append(LogEntry.from_dict(entry_dict))
                    if len(loaded_entries) > 1:
                        self.entries = loaded_entries
                except Exception:
                    pass

    def save_state(self, current_term: int, voted_for: Optional[str]):
        """Persists currentTerm and votedFor to disk before responding to RPCs."""
        with self._lock:
            self.current_term = current_term
            self.voted_for = voted_for
            data = {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "snapshot_index": self.snapshot_index,
                "snapshot_term": self.snapshot_term
            }
            temp_file = self.state_file + ".tmp"
            with open(temp_file, "w") as f:
                json.dump(data, f)
            if os.path.exists(self.state_file):
                os.remove(self.state_file)
            os.rename(temp_file, self.state_file)

    def _rewrite_log_file(self):
        temp_file = self.log_file + ".tmp"
        with open(temp_file, "w") as f:
            for entry in self.entries[1:]:
                f.write(json.dumps(entry.to_dict()) + "\n")
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        os.rename(temp_file, self.log_file)

    def append_entry(self, entry: LogEntry):
        with self._lock:
            self.entries.append(entry)
            with open(self.log_file, "a") as f:
                f.write(json.dumps(entry.to_dict()) + "\n")

    def append_entries(self, new_entries: List[LogEntry]):
        with self._lock:
            for e in new_entries:
                self.append_entry(e)

    def truncate_from(self, start_index: int):
        """Removes all log entries from start_index to the end (used on log conflict)."""
        with self._lock:
            if start_index < len(self.entries):
                self.entries = self.entries[:start_index]
                self._rewrite_log_file()

    def get_entry(self, index: int) -> Optional[LogEntry]:
        with self._lock:
            if 0 <= index < len(self.entries):
                return self.entries[index]
            return None

    def get_entries_from(self, start_index: int) -> List[LogEntry]:
        with self._lock:
            if start_index < len(self.entries):
                return list(self.entries[start_index:])
            return []

    def last_log_index(self) -> int:
        with self._lock:
            return self.entries[-1].index

    def last_log_term(self) -> int:
        with self._lock:
            return self.entries[-1].term

    def term_at(self, index: int) -> int:
        with self._lock:
            if index == self.snapshot_index:
                return self.snapshot_term
            if 0 <= index < len(self.entries):
                return self.entries[index].term
            return 0
