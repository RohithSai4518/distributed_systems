# Aegis Distributed Systems Engine (`aegis.dist`)

A ground-up, zero-dependency, fault-tolerant distributed consensus, storage, and transaction engine with an interactive real-time cluster visualizer.

---

## 🏛️ System Architecture

```mermaid
graph TD
    Client[Client SDK / CLI / REST Gateway] -->|Custom Binary RPC| Transport[Chaos-Aware Transport Layer]
    Transport --> Server[Multi-Threaded TCP Server]
    
    subgraph "Aegis Node Architecture"
        Server --> Router[RPC Dispatcher & Router]
        Router --> Raft[Raft Consensus Engine]
        Router --> Gossip[SWIM Gossip Failure Detector]
        Router --> Ring[Consistent Hash Ring & Partitioner]
        Router --> Tx[2PC Transaction Coordinator]
        
        Raft --> SM[Replicated State Machine]
        SM --> LSM[LSM-Tree Storage Engine]
        
        subgraph "LSM Storage Hierarchy"
            LSM --> WAL[Write-Ahead Log (WAL)]
            LSM --> MemTable[Concurrent SkipList MemTable]
            LSM --> Flush[Flushing MemTable Queue]
            LSM --> L0[Level 0 SSTables]
            LSM --> L1[Level 1+ SSTables]
            L0 -.-> Compactor[Leveled Background Compactor]
            L1 -.-> Compactor
        end
    end
```

---

## 📦 Dependencies

Aegis is engineered with **zero external third-party runtime dependencies**. All cryptographic hashing (CRC32, FNV-1a, Murmur3), concurrent data structures (SkipList, B-link Tree, Bloom Filter), networking (TCP socket servers, framing codecs), and consensus algorithms are implemented from first principles using standard runtime libraries.

- **Runtime**: Python 3.9+ (Standard Library only)
- **Frontend**: Pure HTML5, CSS3 Glassmorphic Styling, Vanilla JavaScript ES6+ (No external CDNs or heavy frameworks)

---

## 🔧 Installation

Clone the repository and set up a virtual environment:

```bash
# 1. Create and activate Python virtual environment
python -m venv venv

# Windows PowerShell:
.env\Scripts\Activate.ps1

# Linux / macOS:
source venv/bin/activate

# 2. Install the package in editable mode
pip install -e .
```

---

## 🔨 Build

To compile bytecode, check syntax, and build Docker containers:

```bash
# Compile and verify python bytecode
python -m compileall aegis/ web/

# Or using Makefile:
make build

# Or build via Docker:
docker build -t aegis-dist:latest .
```

---

## 🚀 Run

### 1. Launch Multi-Node Cluster with Web Visualizer Dashboard

Start a 3-node cluster with interactive web dashboard on `http://localhost:8080`:

```bash
python main.py cluster --nodes 3 --http-port 8080
```

Alternatively, launch via Docker or Make:
```bash
# Run with Docker:
docker run -p 8080:8080 -p 9001:9001 -p 9002:9002 -p 9003:9003 aegis-dist:latest

# Run with Make:
make run

# Run with npm:
npm start
```

### 2. Interactive Client REPL CLI

Open an interactive cluster management shell:

```bash
python main.py cli --seed 127.0.0.1:9001
```

Supported CLI Commands:
- `put <key> <value>`: Write key-value pair through Raft consensus.
- `get <key>`: Query value from cluster with linearizable consistency.
- `delete <key>`: Insert tombstone and delete key.
- `scan [start_key] [limit]`: Range scan keys.
- `cas <key> <old_val> <new_val>`: Compare-And-Swap atomic operation.
- `nodes`: Inspect live cluster topology, Raft terms, and Gossip states.

### 3. Automated Chaos Engineering Suite

Execute leader crash failovers and network split-brain simulations:

```bash
python main.py chaos
# or:
make chaos
```

### 4. High-Throughput Load Testing & Benchmarks

Run concurrent load generators and latency percentile measurements (P50, P90, P95, P99):

```bash
python main.py benchmark --workers 4 --ops-per-worker 200
# or:
make bench
```

### 5. Automated Unit & Integration Tests

Run the complete test suite:

```bash
python main.py test
# or:
pytest
```

---

## 📖 Usage & API Reference

### Python Client SDK Example

```python
from aegis.client.sdk import AegisClient

# Connect to cluster seeds
client = AegisClient(seed_nodes=[("127.0.0.1", 9001), ("127.0.0.1", 9002), ("127.0.0.1", 9003)])

# Put value
client.put("user:1001", "Alice")

# Get value
found, val = client.get("user:1001")
print(f"Read: {val}")  # Alice

# Atomic Compare-And-Swap
success, prev = client.cas("user:1001", expected_val="Alice", new_val="Alice Smith")

# Range Scan
entries = client.scan(start_key="user:1000", limit=10)
```

### REST API Endpoints

- `GET /api/cluster`: Returns JSON summary of all active cluster nodes, Raft roles, and Gossip heartbeats.
- `GET /api/kv?key=<key>`: Reads key value.
- `POST /api/kv`: Body `{"key": "k", "value": "v"}` writes to cluster.
- `DELETE /api/kv?key=<key>`: Deletes key from cluster.
