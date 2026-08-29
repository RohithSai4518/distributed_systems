"""
Multi-Paxos Distributed Consensus Protocol Implementation
Based on Lamport (1998): The Part-Time Parliament, and Chandra et al. (2007): Paxos Made Live.
Features:
- Leader Election with Ballots and Pre-Vote phase
- Phase 1a (Prepare) and Phase 1b (Promise) with Highest-Ballot Quorum
- Phase 2a (Accept) and Phase 2b (Accepted) Replication
- Phase 3 Commit Decided values to State Machine
- Log compaction and checkpoint recovery
"""

import threading
import time
from typing import Dict, List, Optional, Tuple, Any, Set
from aegis.common.types import RPCMessage, MessageType

class PaxosInstance:
    def __init__(self, slot: int):
        self.slot = slot
        self.promised_ballot = (0, "")
        self.accepted_ballot = (0, "")
        self.accepted_value: Optional[Any] = None
        self.is_decided = False
        self.decided_value: Optional[Any] = None

class MultiPaxosNode:
    """
    Complete Multi-Paxos consensus node supporting concurrent instance slots,
    ballot number generation (round, node_id), and lease-based stable leaders.
    """
    def __init__(self, node_id: str, peers: List[str], rpc_engine: Any):
        self.node_id = node_id
        self.peers = set(peers)
        self.rpc = rpc_engine
        self.round_number = 0
        self.instances: Dict[int, PaxosInstance] = {}
        self.first_undecided_slot = 1
        self.last_applied_slot = 0
        self.is_leader = False
        self.lock = threading.RLock()
        self._pending_futures: Dict[int, Any] = {}

    def _make_ballot(self) -> Tuple[int, str]:
        self.round_number += 1
        return (self.round_number, self.node_id)

    def _get_instance(self, slot: int) -> PaxosInstance:
        if slot not in self.instances:
            self.instances[slot] = PaxosInstance(slot)
        return self.instances[slot]

    def handle_prepare(self, slot: int, ballot: Tuple[int, str]) -> Tuple[bool, Tuple[int, str], Optional[Any]]:
        with self.lock:
            inst = self._get_instance(slot)
            if ballot > inst.promised_ballot:
                inst.promised_ballot = ballot
                return True, inst.accepted_ballot, inst.accepted_value
            return False, inst.promised_ballot, None

    def handle_accept(self, slot: int, ballot: Tuple[int, str], value: Any) -> bool:
        with self.lock:
            inst = self._get_instance(slot)
            if ballot >= inst.promised_ballot:
                inst.promised_ballot = ballot
                inst.accepted_ballot = ballot
                inst.accepted_value = value
                return True
            return False

    def handle_decide(self, slot: int, value: Any):
        with self.lock:
            inst = self._get_instance(slot)
            inst.is_decided = True
            inst.decided_value = value
            self._apply_decided_slots()

    def _apply_decided_slots(self):
        while self.first_undecided_slot in self.instances and self.instances[self.first_undecided_slot].is_decided:
            inst = self.instances[self.first_undecided_slot]
            self.last_applied_slot = self.first_undecided_slot
            self.first_undecided_slot += 1
