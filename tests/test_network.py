"""
Unit and Integration Tests for Networking, RPC, and Chaos Transport.
"""

import time
import unittest
from aegis.common.types import MessageType, RPCMessage
from aegis.network.client import TCPClient
from aegis.network.rpc import RPCEngine
from aegis.network.server import TCPServer
from aegis.network.transport import ChaosNetworkTransport


class TestNetworkingAndRPC(unittest.TestCase):
    def setUp(self):
        self.server = TCPServer(host="127.0.0.1", port=9555, node_id="N-TEST")
        self.server.register_handler(MessageType.HEARTBEAT, self._handle_heartbeat)
        self.server.start()

        self.client = TCPClient(node_id="CLIENT-TEST")
        self.transport = ChaosNetworkTransport(node_id="CLIENT-TEST", tcp_client=self.client)
        self.rpc = RPCEngine(node_id="CLIENT-TEST", transport=self.transport)

    def tearDown(self):
        self.rpc.shutdown()
        self.client.close()
        self.server.stop()

    def _handle_heartbeat(self, msg: RPCMessage) -> RPCMessage:
        return RPCMessage(
            msg_id=msg.msg_id,
            msg_type=MessageType.HEARTBEAT_RESP,
            sender_id="N-TEST",
            receiver_id=msg.sender_id,
            payload={"status": "ALIVE", "echo": msg.payload.get("data")},
            is_response=True
        )

    def test_sync_rpc_invocation(self):
        resp = self.rpc.call_sync(
            host="127.0.0.1",
            port=9555,
            target_node_id="N-TEST",
            msg_type=MessageType.HEARTBEAT,
            payload={"data": "ping_data"},
            timeout=2.0
        )
        self.assertIsNotNone(resp)
        self.assertEqual(resp.msg_type, MessageType.HEARTBEAT_RESP)
        self.assertEqual(resp.payload.get("echo"), "ping_data")

    def test_chaos_partition_isolation(self):
        # Isolate node in transport
        self.transport.isolate_node("N-TEST")

        resp = self.rpc.call_sync(
            host="127.0.0.1",
            port=9555,
            target_node_id="N-TEST",
            msg_type=MessageType.HEARTBEAT,
            payload={"data": "test"},
            timeout=0.5
        )
        # Should be dropped by chaos transport
        self.assertIsNone(resp)

        # Heal
        self.transport.heal_partitions()
        resp_healed = self.rpc.call_sync(
            host="127.0.0.1",
            port=9555,
            target_node_id="N-TEST",
            msg_type=MessageType.HEARTBEAT,
            payload={"data": "test"},
            timeout=2.0
        )
        self.assertIsNotNone(resp_healed)


if __name__ == "__main__":
    unittest.main()
