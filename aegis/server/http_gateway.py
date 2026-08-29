"""
HTTP REST Gateway & Embedded Dashboard Web Server.
Zero-dependency HTTP server utilizing Python standard library `http.server`.
Exposes cluster diagnostic APIs, KV operations, chaos testing endpoints, and serves the UI.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import threading
import urllib.parse
from typing import Any, Dict, Optional

from aegis.common.logger import Logger
from aegis.common.types import OpType
from aegis.server.cluster import AegisCluster
from aegis.server.node import AegisNode


class GatewayRequestHandler(BaseHTTPRequestHandler):
    """Handles REST and static UI requests."""

    node: Optional[AegisNode] = None
    cluster: Optional[AegisCluster] = None
    web_dir: str = "./web"

    def log_message(self, format, *args):
        # Silence default stderr logging
        pass

    def _send_json(self, status_code: int, data: Any):
        response_body = json.dumps(data).encode('utf-8')
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(response_body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        # 1. Cluster State & Telemetry API
        if path == "/api/cluster":
            if self.cluster:
                telemetry = [n.get_telemetry() for n in self.cluster.nodes.values()]
                self._send_json(200, {"nodes": telemetry, "total": len(telemetry)})
            elif self.node:
                self._send_json(200, {"nodes": [self.node.get_telemetry()], "total": 1})
            else:
                self._send_json(500, {"error": "No active cluster"})
            return

        # 2. KV Read API
        elif path == "/api/kv":
            key = query.get("key", [""])[0]
            if not key:
                self._send_json(400, {"error": "Missing 'key' query parameter"})
                return

            target_node = self.node or (self.cluster.get_leader() if self.cluster else None)
            if not target_node and self.cluster and self.cluster.nodes:
                target_node = list(self.cluster.nodes.values())[0]

            if not target_node:
                self._send_json(503, {"error": "Cluster unavailable"})
                return

            found, val = target_node.storage.get(key)
            self._send_json(200, {"found": found, "key": key, "value": val})
            return

        # 3. KV Scan API
        elif path == "/api/kv/scan":
            start_k = query.get("start", [""])[0]
            limit = int(query.get("limit", [100])[0])
            target_node = self.node or (list(self.cluster.nodes.values())[0] if self.cluster else None)
            items = target_node.storage.scan(start_k, limit=limit) if target_node else []
            self._send_json(200, {"items": items, "count": len(items)})
            return

        # 4. Static UI Files
        self._serve_static(path)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len) if content_len > 0 else b"{}"

        try:
            payload = json.loads(body.decode('utf-8'))
        except Exception:
            payload = {}

        # 1. KV Write (Put)
        if path == "/api/kv":
            key = payload.get("key", "")
            val = payload.get("value")
            if not key:
                self._send_json(400, {"error": "Missing 'key' in body"})
                return

            leader = self.cluster.get_leader() if self.cluster else self.node
            if not leader:
                self._send_json(503, {"error": "No Raft leader elected yet"})
                return

            success, res, l_id = leader.raft.propose(OpType.PUT, key, val)
            self._send_json(200 if success else 500, {"success": success, "result": res, "leader_id": l_id})
            return

        # 2. Chaos: Network Partition
        elif path == "/api/chaos/partition":
            part_a = set(payload.get("partition_a", []))
            part_b = set(payload.get("partition_b", []))

            if self.cluster:
                for node in self.cluster.nodes.values():
                    node.transport.partition_nodes(part_a, part_b)
                self._send_json(200, {"status": "Partition injected", "partition_a": list(part_a), "partition_b": list(part_b)})
            else:
                self._send_json(400, {"error": "Standalone mode"})
            return

        # 3. Chaos: Heal Network
        elif path == "/api/chaos/heal":
            if self.cluster:
                for node in self.cluster.nodes.values():
                    node.transport.heal_partitions()
                self._send_json(200, {"status": "All network partitions healed"})
            else:
                self._send_json(400, {"error": "Standalone mode"})
            return

        # 4. Chaos: Kill / Stop Node
        elif path == "/api/chaos/kill":
            target_id = payload.get("node_id", "")
            if self.cluster and target_id in self.cluster.nodes:
                self.cluster.nodes[target_id].stop()
                self._send_json(200, {"status": f"Node {target_id} killed"})
            else:
                self._send_json(404, {"error": f"Node {target_id} not found"})
            return

        # 5. Chaos: Restart Node
        elif path == "/api/chaos/start":
            target_id = payload.get("node_id", "")
            if self.cluster and target_id in self.cluster.nodes:
                self.cluster.nodes[target_id].start()
                self._send_json(200, {"status": f"Node {target_id} restarted"})
            else:
                self._send_json(404, {"error": f"Node {target_id} not found"})
            return

        self._send_json(404, {"error": "Endpoint not found"})

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/kv":
            query = urllib.parse.parse_qs(parsed.query)
            key = query.get("key", [""])[0]
            if not key:
                self._send_json(400, {"error": "Missing key parameter"})
                return

            leader = self.cluster.get_leader() if self.cluster else self.node
            if not leader:
                self._send_json(503, {"error": "No leader elected"})
                return

            success, res, l_id = leader.raft.propose(OpType.DELETE, key, None)
            self._send_json(200 if success else 500, {"success": success, "result": res, "leader_id": l_id})
            return

        self._send_json(404, {"error": "Endpoint not found"})

    def _serve_static(self, path: str):
        if path == "/" or path == "":
            path = "/index.html"

        rel_path = path.lstrip("/")
        full_path = os.path.join(self.web_dir, rel_path)

        if not os.path.exists(full_path) or os.path.isdir(full_path):
            self._send_json(404, {"error": "File not found"})
            return

        mime_types = {
            ".html": "text/html",
            ".css": "text/css",
            ".js": "application/javascript",
            ".json": "application/json",
            ".png": "image/png",
            ".svg": "image/svg+xml"
        }
        _, ext = os.path.splitext(full_path)
        content_type = mime_types.get(ext.lower(), "application/octet-stream")

        with open(full_path, "rb") as f:
            content = f.read()

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


class HTTPGatewayServer:
    """Zero-dependency HTTP gateway."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8080, cluster: Optional[AegisCluster] = None, node: Optional[AegisNode] = None, web_dir: str = "./web"):
        self.host = host
        self.port = port
        self.cluster = cluster
        self.node = node
        self.web_dir = web_dir
        self.logger = Logger(node_id="HTTP-GATEWAY")
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        GatewayRequestHandler.node = self.node
        GatewayRequestHandler.cluster = self.cluster
        GatewayRequestHandler.web_dir = self.web_dir

        self._server = HTTPServer((self.host, self.port), GatewayRequestHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="HTTP-Server")
        self._thread.start()
        self.logger.info("HTTP Gateway & Dashboard running at http://%s:%d", "127.0.0.1" if self.host == "0.0.0.0" else self.host, self.port)

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()
