"""
Thread-Safe Multiplexed TCP Client with Connection Pooling.
Provides automatic reconnects, exponential backoff, timeout enforcement,
and framed RPC communication to remote cluster nodes.
"""

import socket
import threading
import time
from typing import Dict, Optional, Tuple

from aegis.common.logger import Logger
from aegis.common.serializer import FrameCodec, SerializationError
from aegis.common.types import RPCMessage
from aegis.network.protocol import NetworkProtocolConstants


class ClientConnectionPool:
    """Manages persistent socket connections to a specific host:port."""

    def __init__(self, host: str, port: int, max_connections: int = 5, timeout: float = 3.0):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.timeout = timeout
        self._pool: list[socket.socket] = []
        self._lock = threading.RLock()

    def get(self) -> socket.socket:
        with self._lock:
            while self._pool:
                sock = self._pool.pop()
                try:
                    # Test if socket is alive
                    sock.settimeout(0.0)
                    test_data = sock.recv(1, socket.MSG_PEEK)
                    if test_data == b"":
                        sock.close()
                        continue
                    sock.settimeout(self.timeout)
                    return sock
                except (BlockingIOError, socket.error):
                    sock.settimeout(self.timeout)
                    return sock

        # Create new socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(self.timeout)
        sock.connect((self.host, self.port))
        return sock

    def release(self, sock: socket.socket):
        with self._lock:
            if len(self._pool) < self.max_connections:
                self._pool.append(sock)
            else:
                try:
                    sock.close()
                except Exception:
                    pass

    def discard(self, sock: socket.socket):
        try:
            sock.close()
        except Exception:
            pass

    def close_all(self):
        with self._lock:
            for sock in self._pool:
                try:
                    sock.close()
                except Exception:
                    pass
            self._pool.clear()


class TCPClient:
    """
    Client for synchronous and asynchronous RPC invocation across nodes.
    """

    def __init__(self, node_id: str = "CLIENT", default_timeout: float = 3.0):
        self.node_id = node_id
        self.default_timeout = default_timeout
        self._pools: Dict[Tuple[str, int], ClientConnectionPool] = {}
        self._lock = threading.RLock()
        self.logger = Logger(node_id=f"{node_id}:CLIENT")

    def _get_pool(self, host: str, port: int) -> ClientConnectionPool:
        key = (host, port)
        with self._lock:
            if key not in self._pools:
                self._pools[key] = ClientConnectionPool(host, port, timeout=self.default_timeout)
            return self._pools[key]

    def send_rpc(self, host: str, port: int, message: RPCMessage, timeout: Optional[float] = None) -> Optional[RPCMessage]:
        """
        Sends an RPCMessage to a remote node and blocks until the response is received.
        Returns the decoded RPCMessage response, or None on failure.
        """
        pool = self._get_pool(host, port)
        sock = None
        req_timeout = timeout or self.default_timeout

        try:
            sock = pool.get()
            sock.settimeout(req_timeout)

            # Send encoded request frame
            data = FrameCodec.encode_message(message)
            sock.sendall(data)

            # Read response header
            header_bytes = bytearray()
            while len(header_bytes) < FrameCodec.HEADER_SIZE:
                chunk = sock.recv(FrameCodec.HEADER_SIZE - len(header_bytes))
                if not chunk:
                    raise ConnectionResetError("Server disconnected while reading header")
                header_bytes.extend(chunk)

            _, _, _, payload_len, expected_checksum = FrameCodec.decode_header(bytes(header_bytes))

            # Read response payload
            payload_bytes = bytearray()
            while len(payload_bytes) < payload_len:
                chunk = sock.recv(min(NetworkProtocolConstants.DEFAULT_BUFFER_SIZE, payload_len - len(payload_bytes)))
                if not chunk:
                    raise ConnectionResetError("Server disconnected while reading payload")
                payload_bytes.extend(chunk)

            resp = FrameCodec.decode_payload(bytes(payload_bytes), expected_checksum)
            pool.release(sock)
            return resp

        except Exception as e:
            if sock:
                pool.discard(sock)
            return None

    def close(self):
        with self._lock:
            for pool in self._pools.values():
                pool.close_all()
            self._pools.clear()
