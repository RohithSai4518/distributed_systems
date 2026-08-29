"""
Comprehensive Unit Tests for Common Subsystems:
- CRC32, Murmur3, and FNV-1a hashing algorithms
- Binary FrameCodec serialization and checksum verification
- Vector Clocks, Lamport Clocks, and Hybrid Logical Clocks (HLC)
"""

import unittest
from aegis.common.crypto import HashAlgorithms
from aegis.common.serializer import FrameCodec, SerializationError
from aegis.common.clocks import LamportClock, VectorClock, HybridLogicalClock, CausalityRelation
from aegis.common.types import MessageType, RPCMessage


class TestCryptoAndHashing(unittest.TestCase):
    def test_crc32_consistency(self):
        data = b"Distributed Consensus In Action"
        crc1 = HashAlgorithms.crc32_custom(data)
        crc2 = HashAlgorithms.crc32_custom(data)
        self.assertEqual(crc1, crc2)
        self.assertIsInstance(crc1, int)

    def test_fnv1a_and_murmur3(self):
        h1 = HashAlgorithms.fnv1a_64("key-alpha")
        h2 = HashAlgorithms.fnv1a_64("key-beta")
        self.assertNotEqual(h1, h2)

        m1 = HashAlgorithms.murmur3_32("key-alpha")
        m2 = HashAlgorithms.murmur3_32("key-beta")
        self.assertNotEqual(m1, m2)

    def test_hash_family_generation(self):
        hashes = HashAlgorithms.generate_hash_family("test_key", num_hashes=7)
        self.assertEqual(len(hashes), 7)
        # Verify independence
        self.assertEqual(len(set(hashes)), 7)


class TestFrameSerialization(unittest.TestCase):
    def test_frame_codec_roundtrip(self):
        msg = RPCMessage(
            msg_id="req-999",
            msg_type=MessageType.CLIENT_REQUEST,
            sender_id="client-1",
            receiver_id="node-1",
            term=5,
            payload={"op": "PUT", "key": "user:1", "value": "Alice"}
        )

        encoded = FrameCodec.encode_message(msg)
        self.assertTrue(len(encoded) > FrameCodec.HEADER_SIZE)

        decoded = FrameCodec.decode_frame(encoded)
        self.assertEqual(decoded.msg_id, "req-999")
        self.assertEqual(decoded.msg_type, MessageType.CLIENT_REQUEST)
        self.assertEqual(decoded.sender_id, "client-1")
        self.assertEqual(decoded.term, 5)
        self.assertEqual(decoded.payload["key"], "user:1")
        self.assertEqual(decoded.payload["value"], "Alice")

    def test_corrupted_frame_detection(self):
        msg = RPCMessage(
            msg_id="req-test",
            msg_type=MessageType.HEARTBEAT,
            sender_id="node-1",
            receiver_id="node-2"
        )
        encoded = bytearray(FrameCodec.encode_message(msg))
        # Corrupt one payload byte
        encoded[-1] ^= 0xFF

        with self.assertRaises(SerializationError):
            FrameCodec.decode_frame(bytes(encoded))


class TestDistributedClocks(unittest.TestCase):
    def test_lamport_clock_ordering(self):
        c1 = LamportClock(node_id="N1")
        c2 = LamportClock(node_id="N2")

        t1 = c1.tick()
        t2 = c1.tick()
        self.assertTrue(t2 > t1)

        t3 = c2.update(t2)
        self.assertTrue(t3 > t2)

    def test_vector_clock_causality(self):
        v1 = VectorClock(node_id="N1")
        v2 = VectorClock(node_id="N2")

        # N1 generates event
        map1 = v1.tick()  # {N1: 1}

        # N2 generates concurrent event
        map2 = v2.tick()  # {N2: 1}

        # Compare concurrent events
        rel = VectorClock.compare(map1, map2)
        self.assertEqual(rel, CausalityRelation.CONCURRENT)

        # N2 receives message from N1
        merged_map2 = v2.update(map1)  # {N1: 1, N2: 2}

        # Now map1 happens before merged_map2
        rel2 = VectorClock.compare(map1, merged_map2)
        self.assertEqual(rel2, CausalityRelation.HAPPENS_BEFORE)

    def test_hybrid_logical_clock(self):
        hlc1 = HybridLogicalClock(node_id="N1")
        hlc2 = HybridLogicalClock(node_id="N2")

        t1 = hlc1.now()
        t2 = hlc1.now()
        self.assertEqual(HybridLogicalClock.compare(t1, t2), -1)

        t3 = hlc2.update(t2[0], t2[1])
        self.assertEqual(HybridLogicalClock.compare(t2, t3), -1)


if __name__ == "__main__":
    unittest.main()
