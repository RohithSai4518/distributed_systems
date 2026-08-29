// Aegis Distributed Systems Cluster Visualizer Frontend Engine
document.addEventListener("DOMContentLoaded", () => {
    const canvas = document.getElementById("topology-canvas");
    const ctx = canvas.getContext("2d");

    let nodesData = [];
    let activeLeaderId = null;
    let pulseRadius = 0;

    // Resize canvas
    function resizeCanvas() {
        canvas.width = canvas.parentElement.clientWidth;
        canvas.height = canvas.parentElement.clientHeight;
    }
    window.addEventListener("resize", resizeCanvas);
    resizeCanvas();

    // Logger helper
    function appendLog(text, level = "info") {
        const stream = document.getElementById("log-stream");
        const ts = new Date().toLocaleTimeString();
        const div = document.createElement("div");
        div.className = `log-item ${level}`;
        div.innerHTML = `<span class="log-ts">[${ts}]</span> ${text}`;
        stream.appendChild(div);
        stream.scrollTop = stream.scrollHeight;
    }

    // Telemetry Poller
    async function fetchClusterState() {
        try {
            const resp = await fetch("/api/cluster");
            if (!resp.ok) throw new Error("Cluster offline");
            const data = await resp.json();
            nodesData = data.nodes || [];

            updateClusterUI();
        } catch (e) {
            document.getElementById("health-text").textContent = "Cluster Disconnected";
            document.getElementById("cluster-health").style.borderColor = "var(--accent-red)";
            document.getElementById("cluster-health").style.color = "var(--accent-red)";
        }
    }

    function updateClusterUI() {
        const container = document.getElementById("nodes-container");
        container.innerHTML = "";

        let highestTerm = 0;
        activeLeaderId = null;

        nodesData.forEach(node => {
            const isLeader = node.raft.state === "LEADER";
            if (isLeader) activeLeaderId = node.node_id;
            if (node.raft.term > highestTerm) highestTerm = node.raft.term;

            const card = document.createElement("div");
            card.className = `node-card ${node.raft.state.toLowerCase()}`;
            card.innerHTML = `
                <div class="node-title">
                    <span>${node.node_id}</span>
                    <span class="role-badge ${node.raft.state}">${node.raft.state}</span>
                </div>
                <div class="node-stat-row">
                    <span>Term: ${node.raft.term}</span>
                    <span>Log Idx: ${node.raft.last_log_index}</span>
                </div>
                <div class="node-stat-row">
                    <span>Commit: ${node.raft.commit_index}</span>
                    <span>Applied: ${node.raft.last_applied}</span>
                </div>
                <div class="node-stat-row">
                    <span>MemTable: ${Math.round(node.storage.memtable_bytes / 1024)} KB</span>
                    <span>SST L0/L1: ${node.storage.l0_sst_count}/${node.storage.l1_sst_count}</span>
                </div>
            `;
            container.appendChild(card);
        });

        document.getElementById("active-term").textContent = `Term: ${highestTerm}`;
        document.getElementById("health-text").textContent = `Cluster Active (${nodesData.length} Nodes)`;
        document.getElementById("cluster-health").style.borderColor = "rgba(16, 185, 129, 0.3)";
        document.getElementById("cluster-health").style.color = "var(--accent-green)";
    }

    // Canvas Animation Loop
    function renderCanvas() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        const count = nodesData.length;
        if (count === 0) {
            requestAnimationFrame(renderCanvas);
            return;
        }

        const centerX = canvas.width / 2;
        const centerY = canvas.height / 2;
        const radius = Math.min(centerX, centerY) - 50;

        const positions = [];
        for (let i = 0; i < count; i++) {
            const angle = (i / count) * 2 * Math.PI - Math.PI / 2;
            const x = centerX + radius * Math.cos(angle);
            const y = centerY + radius * Math.sin(angle);
            positions.push({ x, y, node: nodesData[i] });
        }

        // Draw connections / mesh links
        ctx.lineWidth = 1;
        for (let i = 0; i < count; i++) {
            for (let j = i + 1; j < count; j++) {
                ctx.beginPath();
                ctx.strokeStyle = "rgba(59, 130, 246, 0.15)";
                ctx.moveTo(positions[i].x, positions[i].y);
                ctx.lineTo(positions[j].x, positions[j].y);
                ctx.stroke();
            }
        }

        // Animated leader pulse
        pulseRadius = (pulseRadius + 0.3) % 25;

        // Draw Nodes
        positions.forEach(p => {
            const isLeader = p.node.raft.state === "LEADER";

            if (isLeader) {
                ctx.beginPath();
                ctx.arc(p.x, p.y, 22 + pulseRadius, 0, 2 * Math.PI);
                ctx.strokeStyle = `rgba(6, 182, 212, ${1 - pulseRadius / 25})`;
                ctx.stroke();
            }

            ctx.beginPath();
            ctx.arc(p.x, p.y, 18, 0, 2 * Math.PI);
            ctx.fillStyle = isLeader ? "#06b6d4" : "#1e293b";
            ctx.fill();
            ctx.lineWidth = 2;
            ctx.strokeStyle = isLeader ? "#38bdf8" : "#3b82f6";
            ctx.stroke();

            // Label
            ctx.fillStyle = isLeader ? "#000" : "#f8fafc";
            ctx.font = "bold 10px JetBrains Mono";
            ctx.textAlign = "center";
            ctx.textBaseline = "middle";
            ctx.fillText(p.node.node_id.replace("node-", "N"), p.x, p.y);
        });

        requestAnimationFrame(renderCanvas);
    }

    // KV Operations
    document.getElementById("btn-put").addEventListener("click", async () => {
        const key = document.getElementById("kv-key").value.trim();
        const value = document.getElementById("kv-value").value.trim();
        if (!key) return;

        try {
            const resp = await fetch("/api/kv", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ key, value })
            });
            const data = await resp.json();
            document.getElementById("kv-output").textContent = JSON.stringify(data, null, 2);
            appendLog(`PUT [${key}] = '${value}' => ${data.success ? 'SUCCESS (Raft Quorum)' : 'FAIL'}`);
        } catch (e) {
            document.getElementById("kv-output").textContent = `Error: ${e.message}`;
        }
    });

    document.getElementById("btn-get").addEventListener("click", async () => {
        const key = document.getElementById("kv-key").value.trim();
        if (!key) return;

        try {
            const resp = await fetch(`/api/kv?key=${encodeURIComponent(key)}`);
            const data = await resp.json();
            document.getElementById("kv-output").textContent = JSON.stringify(data, null, 2);
            appendLog(`GET [${key}] => ${data.found ? `'${data.value}'` : 'NOT FOUND'}`);
        } catch (e) {
            document.getElementById("kv-output").textContent = `Error: ${e.message}`;
        }
    });

    document.getElementById("btn-delete").addEventListener("click", async () => {
        const key = document.getElementById("kv-key").value.trim();
        if (!key) return;

        try {
            const resp = await fetch(`/api/kv?key=${encodeURIComponent(key)}`, { method: "DELETE" });
            const data = await resp.json();
            document.getElementById("kv-output").textContent = JSON.stringify(data, null, 2);
            appendLog(`DELETE [${key}] => ${data.success ? 'DELETED' : 'FAIL'}`, "warn");
        } catch (e) {
            document.getElementById("kv-output").textContent = `Error: ${e.message}`;
        }
    });

    document.getElementById("btn-scan").addEventListener("click", async () => {
        try {
            const resp = await fetch("/api/kv/scan");
            const data = await resp.json();
            document.getElementById("kv-output").textContent = JSON.stringify(data, null, 2);
            appendLog(`SCAN => Retrieved ${data.count} items.`);
        } catch (e) {
            document.getElementById("kv-output").textContent = `Error: ${e.message}`;
        }
    });

    // Chaos Controls
    document.getElementById("btn-partition").addEventListener("click", async () => {
        try {
            const resp = await fetch("/api/chaos/partition", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ partition_a: ["node-1", "node-2"], partition_b: ["node-3"] })
            });
            const data = await resp.json();
            appendLog("CHAOS: Injected Split-Brain Partition (node-1,2 vs node-3)", "warn");
        } catch (e) {
            appendLog(`Chaos Error: ${e.message}`, "error");
        }
    });

    document.getElementById("btn-kill-leader").addEventListener("click", async () => {
        if (!activeLeaderId) return;
        try {
            await fetch("/api/chaos/kill", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ node_id: activeLeaderId })
            });
            appendLog(`CHAOS: Killed Leader Node (${activeLeaderId})! Awaiting re-election...`, "error");
        } catch (e) {
            appendLog(`Chaos Error: ${e.message}`, "error");
        }
    });

    document.getElementById("btn-heal").addEventListener("click", async () => {
        try {
            await fetch("/api/chaos/heal", { method: "POST" });
            appendLog("CHAOS: Healed all network partitions and re-enabled links.", "info");
        } catch (e) {
            appendLog(`Chaos Error: ${e.message}`, "error");
        }
    });

    document.getElementById("btn-clear-logs").addEventListener("click", () => {
        document.getElementById("log-stream").innerHTML = "";
    });

    // Start Loops
    setInterval(fetchClusterState, 1000);
    fetchClusterState();
    requestAnimationFrame(renderCanvas);
});
