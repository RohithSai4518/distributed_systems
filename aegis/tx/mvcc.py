"""
Multi-Version Concurrency Control (MVCC) Engine.
Provides Snapshot Isolation and Non-Blocking Reads without read locks.
Maintains version chains for each key: Key -> [V3, V2, V1].
"""

from dataclasses import dataclass, field
import threading
import time
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class VersionRecord:
    value: Optional[Any]  # None indicates deletion tombstone
    created_ts: int       # Transaction commit timestamp (or HLC)
    expired_ts: int = 0   # 0 means currently active / not overwritten
    tx_id: str = ""
    is_tombstone: bool = False


@dataclass
class ReadView:
    """Snapshot read-view defining what transactions are visible to a reader."""
    read_ts: int
    active_tx_ids: set[str] = field(default_factory=set)


class MVCCStorage:
    """
    In-memory MVCC version store with Snapshot Isolation.
    """

    def __init__(self):
        self._versions: Dict[str, List[VersionRecord]] = {}
        self._lock = threading.RLock()
        self._active_write_locks: Dict[str, str] = {}  # key -> tx_id

    def create_read_view(self, read_ts: int, active_tx_ids: Optional[set[str]] = None) -> ReadView:
        return ReadView(read_ts=read_ts, active_tx_ids=active_tx_ids or set())

    def get(self, key: str, read_view: ReadView) -> Tuple[bool, Optional[Any]]:
        """
        Snapshot Read: Returns the version where created_ts <= read_view.read_ts,
        and (expired_ts == 0 or expired_ts > read_view.read_ts),
        and tx_id was not active when the read view was captured.
        """
        with self._lock:
            chain = self._versions.get(key)
            if not chain:
                return False, None

            # Scan version chain from newest to oldest
            for ver in chain:
                if ver.tx_id in read_view.active_tx_ids:
                    continue  # Uncommitted transaction when view was created

                if ver.created_ts <= read_view.read_ts:
                    if ver.expired_ts == 0 or ver.expired_ts > read_view.read_ts:
                        if ver.is_tombstone:
                            return False, None
                        return True, ver.value

        return False, None

    def write(self, key: str, value: Optional[Any], commit_ts: int, tx_id: str, is_tombstone: bool = False) -> bool:
        """
        Appends a new version to the version chain and expires previous active version.
        Detects write-write conflicts if another concurrent transaction modified key after our read view.
        """
        with self._lock:
            chain = self._versions.setdefault(key, [])

            # Check write-write conflict: if newest version has created_ts > commit_ts
            if chain:
                newest = chain[0]
                if newest.created_ts > commit_ts:
                    return False  # Write-write conflict

                # Expire previous newest version
                newest.expired_ts = commit_ts

            new_ver = VersionRecord(
                value=value,
                created_ts=commit_ts,
                expired_ts=0,
                tx_id=tx_id,
                is_tombstone=is_tombstone
            )
            # Insert at head (newest first)
            chain.insert(0, new_ver)
            return True

    def garbage_collect(self, min_active_read_ts: int) -> int:
        """
        Prunes versions that are older than the oldest active transaction's read view
        and have already expired.
        """
        pruned_count = 0
        with self._lock:
            for key, chain in list(self._versions.items()):
                new_chain = []
                seen_visible = False
                for ver in chain:
                    if ver.created_ts <= min_active_read_ts:
                        if not seen_visible:
                            new_chain.append(ver)
                            seen_visible = True
                        else:
                            pruned_count += 1
                    else:
                        new_chain.append(ver)
                self._versions[key] = new_chain

        return pruned_count
