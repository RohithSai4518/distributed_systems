"""
Micro-Benchmarks for LSM Engine, SkipList, and Binary Serialization.
"""

import shutil
import tempfile
import time
from typing import Dict

from aegis.common.crypto import HashAlgorithms
from aegis.common.serializer import FrameCodec
from aegis.common.types import MessageType, RPCMessage
from aegis.storage.engine import LSMEngine
from aegis.storage.memtable import SkipList


class MicroBenchmarks:
    """Micro-benchmarks for isolated subsystem performance."""

    @staticmethod
    def benchmark_serialization(iterations: int = 100000) -> Dict[str, float]:
        msg = RPCMessage(
            msg_id="bench-id-12345",
            msg_type=MessageType.APPEND_ENTRIES,
            sender_id="node-1",
            receiver_id="node-2",
            term=42,
            payload={"entries": [{"term": 42, "index": 100, "op_type": "PUT", "key": "k1", "value": "v1"}]}
        )

        t0 = time.time()
        for _ in range(iterations):
            encoded = FrameCodec.encode_message(msg)
        dur_encode = time.time() - t0

        t1 = time.time()
        for _ in range(iterations):
            decoded = FrameCodec.decode_frame(encoded)
        dur_decode = time.time() - t1

        return {
            "Encode (ops/sec)": iterations / dur_encode,
            "Decode (ops/sec)": iterations / dur_decode,
        }

    @staticmethod
    def benchmark_skiplist(iterations: int = 50000) -> Dict[str, float]:
        sl = SkipList()
        t0 = time.time()
        for i in range(iterations):
            sl.put(f"key_{i:06d}", f"value_{i:06d}")
        dur_put = time.time() - t0

        t1 = time.time()
        for i in range(iterations):
            sl.get(f"key_{i:06d}")
        dur_get = time.time() - t1

        return {
            "SkipList Put (ops/sec)": iterations / dur_put,
            "SkipList Get (ops/sec)": iterations / dur_get,
        }

    @staticmethod
    def benchmark_lsm_engine(iterations: int = 10000) -> Dict[str, float]:
        tmp_dir = tempfile.mkdtemp()
        engine = LSMEngine(data_dir=tmp_dir)

        t0 = time.time()
        for i in range(iterations):
            engine.put(f"lsm_key_{i:05d}", f"lsm_val_{i:05d}")
        dur_put = time.time() - t0

        t1 = time.time()
        for i in range(iterations):
            engine.get(f"lsm_key_{i:05d}")
        dur_get = time.time() - t1

        engine.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)

        return {
            "LSM Engine Sequential Put (ops/sec)": iterations / dur_put,
            "LSM Engine Multi-Tier Get (ops/sec)": iterations / dur_get,
        }


if __name__ == "__main__":
    print("\n--- Serialization Benchmark ---")
    print(MicroBenchmarks.benchmark_serialization())
    print("\n--- SkipList Benchmark ---")
    print(MicroBenchmarks.benchmark_skiplist())
    print("\n--- LSM Storage Engine Benchmark ---")
    print(MicroBenchmarks.benchmark_lsm_engine())
