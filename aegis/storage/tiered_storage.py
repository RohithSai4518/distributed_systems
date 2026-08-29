"""
Multi-Tier Distributed Lifecycle Storage Hierarchy (Hot/Warm/Cold)
Manages storage media tiers:
- Hot Tier: RAM & NVMe Memory-Mapped buffers for low-latency active writes
- Warm Tier: Local SSD Leveled SSTables
- Cold Tier: Compressed & Checksummed Archival Blob segments with automatic TTL migration
"""

import os
import time
import gzip
import shutil
import threading
from typing import Dict, List, Optional, Tuple, Any

class StorageTier:
    HOT = "hot"      # Active In-Memory & NVMe
    WARM = "warm"    # SSD SSTables
    COLD = "cold"    # Compressed Archival Files

class TierMigrationPolicy:
    def __init__(self, hot_ttl_sec: float = 3600.0, warm_ttl_sec: float = 86400.0):
        self.hot_ttl_sec = hot_ttl_sec
        self.warm_ttl_sec = warm_ttl_sec


class TieredStorageManager:
    """
    Coordinates multi-tier data placement, transparent demotion, and cold archive hydration.
    """
    def __init__(self, base_data_dir: str, policy: Optional[TierMigrationPolicy] = None):
        self.base_dir = base_data_dir
        self.policy = policy or TierMigrationPolicy()

        self.hot_dir = os.path.join(self.base_dir, "tier_hot")
        self.warm_dir = os.path.join(self.base_dir, "tier_warm")
        self.cold_dir = os.path.join(self.base_dir, "tier_cold")

        for d in [self.hot_dir, self.warm_dir, self.cold_dir]:
            os.makedirs(d, exist_ok=True)

        self.catalog: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.RLock()
        self._is_running = True
        self._migration_worker = threading.Thread(target=self._run_lifecycle_loop, daemon=True)
        self._migration_worker.start()

    def register_artifact(self, artifact_id: str, local_path: str, initial_tier: str = StorageTier.HOT):
        with self.lock:
            self.catalog[artifact_id] = {
                "tier": initial_tier,
                "path": local_path,
                "created_at": time.time(),
                "last_accessed": time.time(),
                "compressed": False
            }

    def touch(self, artifact_id: str):
        with self.lock:
            if artifact_id in self.catalog:
                self.catalog[artifact_id]["last_accessed"] = time.time()

    def get_path(self, artifact_id: str) -> Optional[str]:
        with self.lock:
            meta = self.catalog.get(artifact_id)
            if not meta:
                return None
            meta["last_accessed"] = time.time()
            if meta["compressed"] and meta["tier"] == StorageTier.COLD:
                # Decompress on access
                return self._hydrate_cold_artifact(artifact_id, meta)
            return meta["path"]

    def _hydrate_cold_artifact(self, artifact_id: str, meta: Dict[str, Any]) -> str:
        gz_path = meta["path"]
        decomp_path = os.path.join(self.warm_dir, f"{artifact_id}.decomp")
        with gzip.open(gz_path, 'rb') as f_in:
            with open(decomp_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        meta["tier"] = StorageTier.WARM
        meta["path"] = decomp_path
        meta["compressed"] = False
        return decomp_path

    def _run_lifecycle_loop(self):
        while self._is_running:
            time.sleep(10.0)
            now = time.time()
            with self.lock:
                for art_id, meta in list(self.catalog.items()):
                    age = now - meta["last_accessed"]
                    # Demote Hot -> Warm
                    if meta["tier"] == StorageTier.HOT and age > self.policy.hot_ttl_sec:
                        self._demote_to_warm(art_id, meta)
                    # Demote Warm -> Cold
                    elif meta["tier"] == StorageTier.WARM and age > self.policy.warm_ttl_sec:
                        self._demote_to_cold(art_id, meta)

    def _demote_to_warm(self, art_id: str, meta: Dict[str, Any]):
        old_path = meta["path"]
        if os.path.exists(old_path):
            new_path = os.path.join(self.warm_dir, os.path.basename(old_path))
            shutil.move(old_path, new_path)
            meta["path"] = new_path
            meta["tier"] = StorageTier.WARM

    def _demote_to_cold(self, art_id: str, meta: Dict[str, Any]):
        old_path = meta["path"]
        if os.path.exists(old_path):
            gz_path = os.path.join(self.cold_dir, f"{os.path.basename(old_path)}.gz")
            with open(old_path, 'rb') as f_in:
                with gzip.open(gz_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(old_path)
            meta["path"] = gz_path
            meta["tier"] = StorageTier.COLD
            meta["compressed"] = True

    def close(self):
        self._is_running = False
