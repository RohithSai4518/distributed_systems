"""
SWIM-Style Gossip Cluster Membership & Failure Detection Protocol.
Features:
- Periodic direct pinging of random cluster members
- Indirect pinging (PING-REQ) via intermediate nodes on missed heartbeat
- Suspect -> Dead state machine with incarnation refutation
- Anti-Entropy piggybacked membership dissemination
"""

import random
import threading
import time
from typing import Dict, List, Optional, Set

from aegis.common.logger import Logger
from aegis.common.types import MessageType, NodeStatus, PeerNode, RPCMessage
from aegis.network.rpc import RPCEngine


class GossipMemberState:
    def __init__(self, node: PeerNode):
        self.node = node
        self.status = node.status
        self.incarnation = node.incarnation
        self.suspect_since: Optional[float] = None


class GossipManager:
    """
    Decentralized cluster membership and failure detector.
    Zero single-point-of-failure topology discovery.
    """

    def __init__(
        self,
        node_id: str,
        self_node: PeerNode,
        rpc_engine: RPCEngine,
        ping_interval_sec: float = 1.0,
        ping_timeout_sec: float = 0.5,
        suspect_timeout_sec: float = 3.0,
        indirect_ping_count: int = 2
    ):
        self.node_id = node_id
        self.self_node = self_node
        self.rpc = rpc_engine
        self.ping_interval_sec = ping_interval_sec
        self.ping_timeout_sec = ping_timeout_sec
        self.suspect_timeout_sec = suspect_timeout_sec
        self.indirect_ping_count = indirect_ping_count

        self.logger = Logger(node_id=f"{node_id}:GOSSIP")
        self.members: Dict[str, GossipMemberState] = {
            self.node_id: GossipMemberState(self_node)
        }
        self.incarnation = 0

        self._lock = threading.RLock()
        self._is_running = False
        self._gossip_thread: Optional[threading.Thread] = None

        # Register RPC handlers
        self.rpc.register_handler(MessageType.GOSSIP_PING, self._handle_gossip_ping)
        self.rpc.register_handler(MessageType.GOSSIP_PING_REQ, self._handle_gossip_ping_req)

    def add_member(self, peer: PeerNode):
        with self._lock:
            if peer.node_id not in self.members:
                self.members[peer.node_id] = GossipMemberState(peer)
                self.logger.info("Added cluster member: %s (%s:%d)", peer.node_id, peer.host, peer.port)

    def start(self):
        with self._lock:
            self._is_running = True
            self._gossip_thread = threading.Thread(
                target=self._protocol_loop,
                daemon=True,
                name=f"GossipWorker-{self.node_id}"
            )
            self._gossip_thread.start()
            self.logger.info("SWIM Gossip failure detector started.")

    def stop(self):
        with self._lock:
            self._is_running = False

    def _protocol_loop(self):
        while self._is_running:
            time.sleep(self.ping_interval_sec)
            if not self._is_running:
                break
            try:
                self._gossip_step()
                self._check_suspect_timeouts()
            except Exception:
                if not self._is_running:
                    break

    def _gossip_step(self):
        """Selects a random peer and performs direct ping, falling back to indirect ping-req."""
        with self._lock:
            candidates = [m for m in self.members.values() if m.node.node_id != self.node_id and m.status != NodeStatus.DEAD]
            if not candidates:
                return
            target = random.choice(candidates)

        # 1. Direct Ping
        success = self._ping_node(target.node)
        if success or not self._is_running:
            return

        # 2. Indirect Ping via k random intermediaries
        with self._lock:
            intermediaries = [
                m for m in self.members.values()
                if m.node.node_id != self.node_id
                and m.node.node_id != target.node.node_id
                and m.status == NodeStatus.ALIVE
            ]
            selected = random.sample(intermediaries, min(self.indirect_ping_count, len(intermediaries)))

        indirect_success = False
        for helper in selected:
            if not self._is_running:
                return
            if self._ping_req(helper.node, target.node):
                indirect_success = True
                break

        if not indirect_success and self._is_running:
            with self._lock:
                if target.status == NodeStatus.ALIVE:
                    target.status = NodeStatus.SUSPECT
                    target.suspect_since = time.time()
                    self.logger.warn("Marked node %s as SUSPECT (unreachable directly and indirectly)", target.node.node_id)

    def _ping_node(self, target: PeerNode) -> bool:
        """Sends direct GOSSIP_PING with piggybacked membership delta."""
        with self._lock:
            payload = {
                "sender_incarnation": self.incarnation,
                "membership": self._get_membership_snapshot()
            }

        resp = self.rpc.call_sync(
            host=target.host,
            port=target.port,
            target_node_id=target.node_id,
            msg_type=MessageType.GOSSIP_PING,
            payload=payload,
            timeout=self.ping_timeout_sec
        )

        if resp and resp.payload and not resp.error:
            self._merge_membership(resp.payload.get("membership", []))
            return True
        return False

    def _ping_req(self, helper: PeerNode, target: PeerNode) -> bool:
        """Requests helper node to ping target on our behalf."""
        payload = {"target_node": target.to_dict()}
        resp = self.rpc.call_sync(
            host=helper.host,
            port=helper.port,
            target_node_id=helper.node_id,
            msg_type=MessageType.GOSSIP_PING_REQ,
            payload=payload,
            timeout=self.ping_timeout_sec * 1.5
        )
        return resp is not None and resp.payload.get("target_alive", False)

    def _handle_gossip_ping(self, msg: RPCMessage) -> RPCMessage:
        membership_data = msg.payload.get("membership", [])
        self._merge_membership(membership_data)

        return RPCMessage(
            msg_id=msg.msg_id,
            msg_type=MessageType.GOSSIP_ACK,
            sender_id=self.node_id,
            receiver_id=msg.sender_id,
            payload={"membership": self._get_membership_snapshot()},
            is_response=True
        )

    def _handle_gossip_ping_req(self, msg: RPCMessage) -> RPCMessage:
        target_dict = msg.payload.get("target_node", {})
        target_peer = PeerNode.from_dict(target_dict)

        alive = self._ping_node(target_peer)
        return RPCMessage(
            msg_id=msg.msg_id,
            msg_type=MessageType.GOSSIP_ACK,
            sender_id=self.node_id,
            receiver_id=msg.sender_id,
            payload={"target_alive": alive},
            is_response=True
        )

    def _merge_membership(self, remote_membership: List[Dict]):
        """Merges remote membership states using incarnation and status precedence rules."""
        with self._lock:
            for item in remote_membership:
                n_id = item["node_id"]
                inc = item["incarnation"]
                st = NodeStatus(item["status"])

                if n_id == self.node_id:
                    # Refutation: If remote believes we are SUSPECT with our incarnation, increment our incarnation and refute
                    if st == NodeStatus.SUSPECT and inc >= self.incarnation:
                        self.incarnation = inc + 1
                        self.logger.info("Refuting SUSPECT status! Bumped incarnation to %d", self.incarnation)
                    continue

                if n_id not in self.members:
                    node = PeerNode(
                        node_id=n_id,
                        host=item["host"],
                        port=item["port"],
                        http_port=item.get("http_port", item["port"] + 1000),
                        status=st,
                        incarnation=inc
                    )
                    self.members[n_id] = GossipMemberState(node)
                else:
                    curr = self.members[n_id]
                    # Update if remote has higher incarnation, or same incarnation and worse status
                    if inc > curr.incarnation:
                        curr.incarnation = inc
                        curr.status = st
                        curr.suspect_since = time.time() if st == NodeStatus.SUSPECT else None
                    elif inc == curr.incarnation:
                        if curr.status == NodeStatus.ALIVE and st == NodeStatus.SUSPECT:
                            curr.status = NodeStatus.SUSPECT
                            curr.suspect_since = time.time()
                        elif st == NodeStatus.DEAD:
                            curr.status = NodeStatus.DEAD

    def _check_suspect_timeouts(self):
        """Converts long-standing SUSPECT nodes to DEAD."""
        now = time.time()
        with self._lock:
            for m in self.members.values():
                if m.status == NodeStatus.SUSPECT and m.suspect_since:
                    if now - m.suspect_since > self.suspect_timeout_sec:
                        m.status = NodeStatus.DEAD
                        self.logger.error("Node %s suspect timeout expired -> Declared DEAD", m.node.node_id)

    def _get_membership_snapshot(self) -> List[Dict]:
        with self._lock:
            return [
                {
                    "node_id": m.node.node_id,
                    "host": m.node.host,
                    "port": m.node.port,
                    "http_port": m.node.http_port,
                    "status": m.status.value,
                    "incarnation": m.incarnation if m.node.node_id != self.node_id else self.incarnation
                }
                for m in self.members.values()
            ]

    def get_alive_nodes(self) -> List[PeerNode]:
        with self._lock:
            return [m.node for m in self.members.values() if m.status == NodeStatus.ALIVE or m.node.node_id == self.node_id]
