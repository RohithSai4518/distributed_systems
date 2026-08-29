"""
High-Performance Multi-Threaded TCP Server.
Handles binary wire framing, connection multiplexing, and concurrent message dispatch.
Zero third-party network libraries (pure socket programming).
"""

import socket
import struct
import threading
import time
from typing import Callable, Dict, Optional, Tuple

from aegis.common.logger import Logger
from aegis.common.serializer import FrameCodec, SerializationError
from aegis.common.types import MessageType, RPCMessage
from aegis.network.protocol import NetworkProtocolConstants, RPCHandlerFunc


class TCPServer:
    """
    Threaded TCP Socket Server for high-throughput node-to-node and client communications.
    """

    def __init__(self, host: str, port: int, node_id: str = "SERVER"):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.logger = Logger(node_id=f"{node_id}:TCP")

        self._server_sock: Optional[socket.socket] = None
        self._handlers: Dict[MessageType, RPCHandlerFunc] = {}
        self._is_running = False
        self._active_connections = set()
        self._lock = threading.RLock()
        self._accept_thread: Optional[threading.Thread] = None

    def register_handler(self, msg_type: MessageType, handler: RPCHandlerFunc):
        """Registers a callback for a specific MessageType."""
        with self._lock:
            self._handlers[msg_type] = handler

    def start(self):
        """Binds TCP port and starts listening for connections."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(128)
        self._server_sock.settimeout(1.0)

        self._is_running = True
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            daemon=True,
            name=f"Acceptor-{self.port}"
        )
        self._accept_thread.start()
        self.logger.info("TCP Server listening on %s:%d", self.host, self.port)

    def _accept_loop(self):
        while self._is_running:
            try:
                client_sock, client_addr = self._server_sock.accept()
                client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client_sock.settimeout(NetworkProtocolConstants.DEFAULT_TIMEOUT_SEC)

                with self._lock:
                    self._active_connections.add(client_sock)

                conn_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_sock, client_addr),
                    daemon=True,
                    name=f"ClientWorker-{client_addr[1]}"
                )
                conn_thread.start()
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception as e:
                if self._is_running:
                    self.logger.error("Error in accept loop: %s", e)

    def _handle_client(self, sock: socket.socket, addr: Tuple[str, int]):
        buffer = bytearray()
        try:
            while self._is_running:
                # Read header if we don't have enough bytes
                while len(buffer) < FrameCodec.HEADER_SIZE:
                    chunk = sock.recv(NetworkProtocolConstants.DEFAULT_BUFFER_SIZE)
                    if not chunk:
                        return  # Client disconnected
                    buffer.extend(chunk)

                # Parse header
                header_bytes = bytes(buffer[:FrameCodec.HEADER_SIZE])
                _, _, msg_type, payload_len, expected_checksum = FrameCodec.decode_header(header_bytes)

                if payload_len > NetworkProtocolConstants.MAX_FRAME_SIZE:
                    self.logger.error("Payload too large (%d bytes), disconnecting %s", payload_len, addr)
                    return

                total_frame_size = FrameCodec.HEADER_SIZE + payload_len

                # Read remaining payload
                while len(buffer) < total_frame_size:
                    chunk = sock.recv(min(NetworkProtocolConstants.DEFAULT_BUFFER_SIZE, total_frame_size - len(buffer)))
                    if not chunk:
                        return
                    buffer.extend(chunk)

                # Extract payload bytes and decode message
                payload_bytes = bytes(buffer[FrameCodec.HEADER_SIZE:total_frame_size])
                del buffer[:total_frame_size]

                try:
                    msg = FrameCodec.decode_payload(payload_bytes, expected_checksum)
                    # Dispatch to handler
                    self._dispatch_message(sock, msg)
                except SerializationError as se:
                    self.logger.error("Serialization error from %s: %s", addr, se)

        except (socket.timeout, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            if self._is_running:
                self.logger.debug("Connection error from %s: %s", addr, e)
        finally:
            with self._lock:
                self._active_connections.discard(sock)
            try:
                sock.close()
            except Exception:
                pass

    def _dispatch_message(self, sock: socket.socket, message: RPCMessage):
        """Finds handler for message type, executes it, and writes response if required."""
        handler = self._handlers.get(message.msg_type)
        if handler:
            try:
                resp = handler(message)
                if resp is not None:
                    encoded_resp = FrameCodec.encode_message(resp)
                    sock.sendall(encoded_resp)
            except Exception as e:
                self.logger.error("Handler error for %s: %s", message.msg_type, e)
                err_resp = RPCMessage(
                    msg_id=message.msg_id,
                    msg_type=message.msg_type,
                    sender_id=self.node_id,
                    receiver_id=message.sender_id,
                    term=message.term,
                    is_response=True,
                    error=str(e)
                )
                sock.sendall(FrameCodec.encode_message(err_resp))
        else:
            self.logger.warn("Unhandled message type: %s from %s", message.msg_type, message.sender_id)

    def stop(self):
        """Stops server and closes all active sockets."""
        self._is_running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass

        with self._lock:
            for s in list(self._active_connections):
                try:
                    s.close()
                except Exception:
                    pass
            self._active_connections.clear()

        self.logger.info("TCP Server stopped.")
