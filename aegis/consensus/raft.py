"""
Full Raft Consensus Protocol Implementation.
Features:
- Leader Election with randomized election timeouts
- AppendEntries Log Replication & Consistency Verification
- Term Transitions & Invariant Verification
- Commit Index Calculation across dynamic quorums
- State Machine Linearizable Application
- Client Proposal Routing
"""

from concurrent.futures import Future
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from aegis.common.logger import Logger
from aegis.common.types import (
    LogEntry, MessageType, NodeState, OpType, PeerNode, RPCMessage
)
from aegis.consensus.election import ElectionTimer
from aegis.consensus.log_store import RaftLogStore
from aegis.consensus.state_machine import ReplicatedStateMachine
from aegis.network.rpc import RPCEngine


class RaftNode:
    """
    Raft Consensus Engine Node.
    """

    def __init__(
        self,
        node_id: str,
        peers: Dict[str, PeerNode],
        rpc_engine: RPCEngine,
        log_store: RaftLogStore,
        state_machine: ReplicatedStateMachine,
        heartbeat_interval_ms: int = 50
    ):
        self.node_id = node_id
        self.peers = peers  # peer_id -> PeerNode
        self.rpc = rpc_engine
        self.log_store = log_store
        self.state_machine = state_machine
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.logger = Logger(node_id=f"{node_id}:RAFT")

        # Persistent state on all servers (stored in log_store)
        self.current_term = self.log_store.current_term
        self.voted_for = self.log_store.voted_for

        # Volatile state on all servers
        self.state = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0

        # Volatile state on leaders (re-initialized after election)
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Pending client proposals waiting for consensus commit: entry_index -> Future
        self._pending_proposals: Dict[int, Future] = {}

        self._lock = threading.RLock()
        self._is_running = False
        self._last_heartbeat_time = time.time()

        # Timers
        self.election_timer = ElectionTimer(
            min_timeout_ms=200,
            max_timeout_ms=400,
            on_timeout=self._handle_election_timeout
        )
        self._heartbeat_thread: Optional[threading.Thread] = None

        # Register RPC handlers with RPC Engine
        self.rpc.register_handler(MessageType.REQUEST_VOTE, self.handle_request_vote)
        self.rpc.register_handler(MessageType.APPEND_ENTRIES, self.handle_append_entries)

    def start(self):
        with self._lock:
            self._is_running = True
            self.election_timer.start()
            self.logger.info("Raft Node started as FOLLOWER in Term %d", self.current_term)

    def stop(self):
        with self._lock:
            self._is_running = False
            self.state = NodeState.DEAD
            self.election_timer.stop()
            self.logger.info("Raft Node stopped.")

    # -------------------------------------------------------------------------
    # Role Transitions & Leader Election
    # -------------------------------------------------------------------------

    def _handle_election_timeout(self):
        """Triggered when no leader heartbeats are received within timeout."""
        with self._lock:
            if not self._is_running or self.state == NodeState.LEADER:
                return

            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self.log_store.save_state(self.current_term, self.voted_for)
            self.election_timer.reset()

            self.logger.warn("Election timeout elapsed! Starting election for Term %d", self.current_term)

        self._start_election()

    def _start_election(self):
        """Broadcasts RequestVote RPCs to all peers in parallel."""
        with self._lock:
            term = self.current_term
            last_log_idx = self.log_store.last_log_index()
            last_log_term = self.log_store.last_log_term()
            candidate_id = self.node_id
            peer_list = list(self.peers.values())

        votes_received = 1  # Voted for self
        votes_needed = (len(peer_list) + 1) // 2 + 1

        if votes_received >= votes_needed:
            self._become_leader()
            return

        payload = {
            "term": term,
            "candidate_id": candidate_id,
            "last_log_index": last_log_idx,
            "last_log_term": last_log_term
        }

        # Send RequestVote RPCs asynchronously
        futures = []
        for peer in peer_list:
            fut = self.rpc.call_async(
                host=peer.host,
                port=peer.port,
                target_node_id=peer.node_id,
                msg_type=MessageType.REQUEST_VOTE,
                term=term,
                payload=payload,
                timeout=0.2
            )
            futures.append(fut)

        # Collect votes
        for fut in futures:
            try:
                resp: Optional[RPCMessage] = fut.result(timeout=0.25)
                if not resp or not resp.payload:
                    continue

                with self._lock:
                    resp_term = resp.payload.get("term", 0)
                    if resp_term > self.current_term:
                        self._become_follower(resp_term)
                        return

                    if self.state == NodeState.CANDIDATE and self.current_term == term:
                        if resp.payload.get("vote_granted", False):
                            votes_received += 1
                            if votes_received >= votes_needed:
                                self._become_leader()
                                return
            except Exception:
                pass

    def _become_leader(self):
        """Transitions node to LEADER state and starts heartbeat worker."""
        with self._lock:
            if self.state != NodeState.CANDIDATE:
                return

            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            self.election_timer.stop()

            # Initialize leader tracking indices
            last_idx = self.log_store.last_log_index()
            for peer_id in self.peers:
                self.next_index[peer_id] = last_idx + 1
                self.match_index[peer_id] = 0

            self.logger.info("--> WON ELECTION! Became LEADER for Term %d", self.current_term)

            # Advance commit index and apply all log entries present on elected leader
            self.commit_index = self.log_store.last_log_index()
            self._apply_committed_entries()

            # Start leader heartbeats
            self._heartbeat_thread = threading.Thread(
                target=self._leader_heartbeat_loop,
                daemon=True,
                name=f"LeaderHeartbeat-{self.node_id}"
            )
            self._heartbeat_thread.start()

        # Send immediate initial empty AppendEntries heartbeat
        self._broadcast_append_entries()

    def _become_follower(self, term: int):
        """Reverts to FOLLOWER on higher term discovery."""
        with self._lock:
            self.state = NodeState.FOLLOWER
            self.current_term = term
            self.voted_for = None
            self.log_store.save_state(self.current_term, self.voted_for)
            self.election_timer.start()
            self.logger.info("Became FOLLOWER for Term %d", self.current_term)

    # -------------------------------------------------------------------------
    # RequestVote RPC Handler
    # -------------------------------------------------------------------------

    def handle_request_vote(self, msg: RPCMessage) -> RPCMessage:
        """
        Processes inbound RequestVote RPC.
        1. Reply false if term < currentTerm
        2. If term > currentTerm: update term, become FOLLOWER
        3. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
        """
        payload = msg.payload
        req_term = payload.get("term", 0)
        candidate_id = payload.get("candidate_id", "")
        last_log_idx = payload.get("last_log_index", 0)
        last_log_term = payload.get("last_log_term", 0)

        with self._lock:
            if req_term > self.current_term:
                self._become_follower(req_term)

            vote_granted = False
            if req_term == self.current_term:
                if self.voted_for is None or self.voted_for == candidate_id:
                    # Check log up-to-dateness (Raft §5.4.1)
                    my_last_idx = self.log_store.last_log_index()
                    my_last_term = self.log_store.last_log_term()

                    log_ok = (last_log_term > my_last_term) or (
                        last_log_term == my_last_term and last_log_idx >= my_last_idx
                    )

                    if log_ok:
                        vote_granted = True
                        self.voted_for = candidate_id
                        self.log_store.save_state(self.current_term, self.voted_for)
                        self.election_timer.reset()
                        self.logger.info("Granted vote to %s for Term %d", candidate_id, self.current_term)

            return RPCMessage(
                msg_id=msg.msg_id,
                msg_type=MessageType.REQUEST_VOTE_RESP,
                sender_id=self.node_id,
                receiver_id=msg.sender_id,
                term=self.current_term,
                payload={"term": self.current_term, "vote_granted": vote_granted},
                is_response=True
            )

    # -------------------------------------------------------------------------
    # AppendEntries RPC Handler (Log Replication & Heartbeats)
    # -------------------------------------------------------------------------

    def handle_append_entries(self, msg: RPCMessage) -> RPCMessage:
        """
        Processes AppendEntries RPC from leader.
        """
        payload = msg.payload
        leader_term = payload.get("term", 0)
        leader_id = payload.get("leader_id", "")
        prev_log_index = payload.get("prev_log_index", 0)
        prev_log_term = payload.get("prev_log_term", 0)
        raw_entries = payload.get("entries", [])
        leader_commit = payload.get("leader_commit", 0)

        with self._lock:
            # 1. Reply false if term < currentTerm
            if leader_term < self.current_term:
                return self._make_append_resp(msg, success=False, match_index=self.log_store.last_log_index())

            # Discovering leader with valid term
            if leader_term > self.current_term or self.state == NodeState.CANDIDATE:
                self._become_follower(leader_term)

            self.leader_id = leader_id
            self._last_heartbeat_time = time.time()
            self.election_timer.reset()

            # 2. Check log matching property at prev_log_index
            if prev_log_index > self.log_store.last_log_index():
                return self._make_append_resp(msg, success=False, match_index=self.log_store.last_log_index())

            if prev_log_index > 0:
                entry_at_prev = self.log_store.get_entry(prev_log_index)
                if entry_at_prev is None or entry_at_prev.term != prev_log_term:
                    # Log conflict: truncate conflicting entries
                    self.log_store.truncate_from(prev_log_index)
                    return self._make_append_resp(msg, success=False, match_index=self.log_store.last_log_index())

            # 3 & 4. Append new entries
            entries_to_append = [LogEntry.from_dict(d) for d in raw_entries]
            for entry in entries_to_append:
                existing = self.log_store.get_entry(entry.index)
                if existing:
                    if existing.term != entry.term:
                        self.log_store.truncate_from(entry.index)
                        self.log_store.append_entry(entry)
                else:
                    self.log_store.append_entry(entry)

            # 5. Update commitIndex
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.log_store.last_log_index())
                self._apply_committed_entries()

            return self._make_append_resp(msg, success=True, match_index=self.log_store.last_log_index())

    def _make_append_resp(self, req_msg: RPCMessage, success: bool, match_index: int) -> RPCMessage:
        return RPCMessage(
            msg_id=req_msg.msg_id,
            msg_type=MessageType.APPEND_ENTRIES_RESP,
            sender_id=self.node_id,
            receiver_id=req_msg.sender_id,
            term=self.current_term,
            payload={
                "term": self.current_term,
                "success": success,
                "match_index": match_index
            },
            is_response=True
        )

    # -------------------------------------------------------------------------
    # Leader Replication Loop & Commit Advances
    # -------------------------------------------------------------------------

    def _leader_heartbeat_loop(self):
        while self._is_running:
            with self._lock:
                if self.state != NodeState.LEADER:
                    break
            self._broadcast_append_entries()
            time.sleep(self.heartbeat_interval_ms / 1000.0)

    def _broadcast_append_entries(self):
        """Sends AppendEntries RPC to all peers."""
        with self._lock:
            if self.state != NodeState.LEADER:
                return
            term = self.current_term
            leader_id = self.node_id
            leader_commit = self.commit_index
            peers_snapshot = list(self.peers.values())

        for peer in peers_snapshot:
            with self._lock:
                next_idx = self.next_index.get(peer.node_id, 1)
                prev_idx = next_idx - 1
                prev_term = self.log_store.term_at(prev_idx)
                entries_to_send = self.log_store.get_entries_from(next_idx)

            payload = {
                "term": term,
                "leader_id": leader_id,
                "prev_log_index": prev_idx,
                "prev_log_term": prev_term,
                "entries": [e.to_dict() for e in entries_to_send],
                "leader_commit": leader_commit
            }

            fut = self.rpc.call_async(
                host=peer.host,
                port=peer.port,
                target_node_id=peer.node_id,
                msg_type=MessageType.APPEND_ENTRIES,
                term=term,
                payload=payload,
                timeout=0.1
            )
            fut.add_done_callback(lambda f, p_id=peer.node_id, entries_count=len(entries_to_send), sent_prev=prev_idx: self._handle_append_response(f, p_id, entries_count, sent_prev))

    def _handle_append_response(self, fut: Future, peer_id: str, entries_count: int, sent_prev: int):
        try:
            resp: Optional[RPCMessage] = fut.result()
            if not resp or not resp.payload:
                return

            with self._lock:
                resp_term = resp.payload.get("term", 0)
                if resp_term > self.current_term:
                    self._become_follower(resp_term)
                    return

                if self.state != NodeState.LEADER:
                    return

                success = resp.payload.get("success", False)
                match_idx = resp.payload.get("match_index", 0)

                if success:
                    self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), sent_prev + entries_count)
                    self.next_index[peer_id] = self.match_index[peer_id] + 1
                    self._check_and_advance_commit_index()
                else:
                    # Step back nextIndex on peer to resolve divergence
                    self.next_index[peer_id] = max(1, min(self.next_index.get(peer_id, 1) - 1, match_idx + 1))
        except Exception:
            pass

    def _check_and_advance_commit_index(self):
        """
        If there exists an N > commitIndex such that a majority of matchIndex[i] >= N,
        and log[N].term == currentTerm: set commitIndex = N (Raft §5.3/5.4).
        """
        with self._lock:
            last_idx = self.log_store.last_log_index()
            for N in range(last_idx, self.commit_index, -1):
                if self.log_store.term_at(N) == self.current_term:
                    # Count matches
                    match_count = 1  # self matches
                    for peer_id in self.peers:
                        if self.match_index.get(peer_id, 0) >= N:
                            match_count += 1

                    quorum = (len(self.peers) + 1) // 2 + 1
                    if match_count >= quorum:
                        self.commit_index = N
                        self._apply_committed_entries()
                        break

    def _apply_committed_entries(self):
        """Applies committed entries from last_applied to commit_index."""
        with self._lock:
            while self.commit_index > self.last_applied:
                self.last_applied += 1
                entry = self.log_store.get_entry(self.last_applied)
                if entry:
                    res = self.state_machine.apply_entry(entry)

                    # Complete pending client proposal if exists
                    fut = self._pending_proposals.pop(entry.index, None)
                    if fut and not fut.done():
                        fut.set_result(res)

    # -------------------------------------------------------------------------
    # Client Request Proposal Interface
    # -------------------------------------------------------------------------

    def propose(self, op_type: OpType, key: str, value: Optional[Any] = None, prev_val: Optional[Any] = None, timeout: float = 3.0) -> Tuple[bool, Any, Optional[str]]:
        """
        Proposes a new client operation to the Raft cluster.
        Returns: (success, result, leader_id)
        """
        with self._lock:
            if self.state != NodeState.LEADER:
                return False, None, self.leader_id

            entry_idx = self.log_store.last_log_index() + 1
            entry = LogEntry(
                term=self.current_term,
                index=entry_idx,
                op_type=op_type,
                key=key,
                value=value,
                prev_value=prev_val
            )
            self.log_store.append_entry(entry)

            fut = Future()
            self._pending_proposals[entry_idx] = fut

        # Broadcast immediately
        self._broadcast_append_entries()

        try:
            res = fut.result(timeout=timeout)
            return True, res, self.node_id
        except Exception as e:
            return False, f"Timeout or error awaiting consensus: {e}", self.leader_id
