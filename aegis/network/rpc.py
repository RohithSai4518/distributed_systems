"""
RPC Dispatcher & Request Router.
Maps incoming RPC requests to registered handler functions and dispatches asynchronous RPCs.
"""

from concurrent.futures import Future, ThreadPoolExecutor
import threading
import uuid
from typing import Any, Callable, Dict, Optional

from aegis.common.types import MessageType, RPCMessage
from aegis.network.protocol import RPCHandlerFunc
from aegis.network.transport import ChaosNetworkTransport


class RPCEngine:
    """
    Central RPC routing manager for a distributed node.
    """

    def __init__(self, node_id: str, transport: ChaosNetworkTransport, max_workers: int = 16):
        self.node_id = node_id
        self.transport = transport
        self._handlers: Dict[MessageType, RPCHandlerFunc] = {}
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"RPC-{node_id}")
        self._is_running = True
        self._lock = threading.RLock()

    def register_handler(self, msg_type: MessageType, handler: RPCHandlerFunc):
        with self._lock:
            self._handlers[msg_type] = handler

    def handle_incoming(self, message: RPCMessage) -> Optional[RPCMessage]:
        handler = self._handlers.get(message.msg_type)
        if handler:
            return handler(message)
        return None

    def call_async(
        self,
        host: str,
        port: int,
        target_node_id: str,
        msg_type: MessageType,
        term: int = 0,
        payload: Optional[Dict[str, Any]] = None,
        timeout: float = 3.0
    ) -> Future:
        """
        Dispatches an RPC asynchronously and returns a Future resolving to the RPCMessage response.
        """
        fut = Future()
        if not self._is_running:
            fut.set_result(None)
            return fut

        msg_id = str(uuid.uuid4())
        req = RPCMessage(
            msg_id=msg_id,
            msg_type=msg_type,
            sender_id=self.node_id,
            receiver_id=target_node_id,
            term=term,
            payload=payload or {},
            is_response=False
        )

        def _send():
            if not self._is_running:
                return None
            return self.transport.send_rpc(host, port, target_node_id, req, timeout=timeout)

        try:
            return self._executor.submit(_send)
        except (RuntimeError, Exception):
            fut.set_result(None)
            return fut

    def call_sync(
        self,
        host: str,
        port: int,
        target_node_id: str,
        msg_type: MessageType,
        term: int = 0,
        payload: Optional[Dict[str, Any]] = None,
        timeout: float = 3.0
    ) -> Optional[RPCMessage]:
        """Synchronously invokes remote RPC."""
        fut = self.call_async(host, port, target_node_id, msg_type, term, payload, timeout)
        try:
            return fut.result(timeout=timeout)
        except Exception:
            return None

    def shutdown(self):
        with self._lock:
            self._is_running = False
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
