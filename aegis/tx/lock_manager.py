"""
Distributed Lock Manager (DLM) & Deadlock Detection.
Implements Strict Two-Phase Locking (SS2PL) with Shared (Read) and Exclusive (Write) lock modes.
Maintains a dynamic Wait-For-Graph (WFG) with automated cycle detection (Tarjan / DFS)
to abort younger transactions and resolve deadlocks.
"""

from collections import defaultdict
from enum import Enum, auto
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

from aegis.common.logger import Logger


class LockMode(Enum):
    SHARED = auto()       # S-Lock (Read)
    EXCLUSIVE = auto()    # X-Lock (Write)


class LockManager:
    """
    Two-Phase Lock Manager with wait queues and deadlock resolution.
    """

    def __init__(self, node_id: str = "DLM"):
        self.node_id = node_id
        self.logger = Logger(node_id=f"{node_id}:LOCKS")

        # key -> set of tx_ids holding shared locks
        self._shared_holders: Dict[str, Set[str]] = defaultdict(set)
        # key -> tx_id holding exclusive lock
        self._exclusive_holder: Dict[str, Optional[str]] = defaultdict(lambda: None)

        # tx_id -> set of keys locked
        self._tx_locks: Dict[str, Set[Tuple[str, LockMode]]] = defaultdict(set)

        # Wait-For-Graph: waiting_tx_id -> set of holding_tx_ids
        self._wait_for_graph: Dict[str, Set[str]] = defaultdict(set)

        self._lock = threading.RLock()

    def acquire_lock(self, tx_id: str, key: str, mode: LockMode, timeout_sec: float = 2.0) -> bool:
        """
        Attempts to acquire a Shared or Exclusive lock on a key.
        Blocks until acquired, timeout elapses, or deadlock is detected.
        """
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            with self._lock:
                if mode == LockMode.SHARED:
                    # Can acquire if no one holds X-lock, or current tx holds X-lock
                    x_holder = self._exclusive_holder[key]
                    if x_holder is None or x_holder == tx_id:
                        self._shared_holders[key].add(tx_id)
                        self._tx_locks[tx_id].add((key, LockMode.SHARED))
                        self._wait_for_graph[tx_id].discard(x_holder)
                        return True
                    else:
                        self._wait_for_graph[tx_id].add(x_holder)

                elif mode == LockMode.EXCLUSIVE:
                    # Can acquire if no X-holder and (no S-holders or only self is S-holder)
                    x_holder = self._exclusive_holder[key]
                    s_holders = self._shared_holders[key]

                    if (x_holder is None or x_holder == tx_id) and (len(s_holders) == 0 or s_holders == {tx_id}):
                        self._exclusive_holder[key] = tx_id
                        self._tx_locks[tx_id].add((key, LockMode.EXCLUSIVE))
                        self._wait_for_graph[tx_id].clear()
                        return True
                    else:
                        if x_holder and x_holder != tx_id:
                            self._wait_for_graph[tx_id].add(x_holder)
                        for sh in s_holders:
                            if sh != tx_id:
                                self._wait_for_graph[tx_id].add(sh)

                # Check for deadlocks in Wait-For-Graph
                cycle = self._find_deadlock_cycle()
                if cycle and tx_id in cycle:
                    self.logger.error("Deadlock detected in cycle: %s! Aborting lock request for %s", cycle, tx_id)
                    return False

            time.sleep(0.02)  # Wait before re-checking lock availability

        return False

    def release_all(self, tx_id: str):
        """Releases all locks held by transaction (Strict 2PL at commit/abort)."""
        with self._lock:
            for key, mode in self._tx_locks.get(tx_id, set()):
                if mode == LockMode.SHARED:
                    self._shared_holders[key].discard(tx_id)
                elif mode == LockMode.EXCLUSIVE:
                    if self._exclusive_holder[key] == tx_id:
                        self._exclusive_holder[key] = None

            self._tx_locks.pop(tx_id, None)
            self._wait_for_graph.pop(tx_id, None)

            # Clear references in other wait lists
            for waiters in self._wait_for_graph.values():
                waiters.discard(tx_id)

    def _find_deadlock_cycle(self) -> Optional[List[str]]:
        """
        Runs Depth-First Search (DFS) on the Wait-For-Graph to detect directed cycles.
        Returns the list of transaction IDs involved in the cycle, or None if DAG.
        """
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        path: List[str] = []

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in self._wait_for_graph.get(node, set()):
                if neighbor not in visited:
                    res = dfs(neighbor)
                    if res:
                        return res
                elif neighbor in rec_stack:
                    # Found cycle
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:]

            rec_stack.remove(node)
            path.pop()
            return None

        for tx in list(self._wait_for_graph.keys()):
            if tx not in visited:
                cycle = dfs(tx)
                if cycle:
                    return cycle

        return None
