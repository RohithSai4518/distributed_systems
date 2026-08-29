"""
Hierarchical Navigable Small World (HNSW) Vector Search Index
Implements Malkov & Yashunin (2018) scalable k-NN similarity search over embedding vectors.
Metrics supported:
- Cosine Distance
- Euclidean (L2) Distance
- Dot Product Inner Distance
"""

import math
import random
import heapq
import threading
from typing import List, Tuple, Dict, Optional, Set, Any

class VectorMetric:
    EUCLIDEAN = "euclidean"
    COSINE = "cosine"
    DOT_PRODUCT = "dot_product"

def compute_euclidean(a: List[float], b: List[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

def compute_cosine_distance(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 1.0
    return 1.0 - (dot / (norm_a * norm_b))

def compute_dot_distance(a: List[float], b: List[float]) -> float:
    return -sum(x * y for x, y in zip(a, b))


class HNSWNode:
    def __init__(self, doc_id: str, vector: List[float], level: int):
        self.doc_id = doc_id
        self.vector = vector
        self.level = level
        # neighbors per level: List of Sets of neighbor node references
        self.neighbors: List[Set['HNSWNode']] = [set() for _ in range(level + 1)]


class HNSWIndex:
    """
    Thread-safe HNSW vector similarity search engine for high-dimensional distributed retrieval.
    """
    def __init__(self, dim: int = 128, m: int = 16, ef_construction: int = 64, ef_search: int = 32, metric: str = VectorMetric.COSINE):
        self.dim = dim
        self.m = m                          # Max bidirectional connections per node per layer
        self.m_max0 = 2 * m                 # Max connections for layer 0
        self.ef_construction = ef_construction
        self.ef_search = ef_search
        self.metric = metric
        self.ml = 1.0 / math.log(m)         # Layer selection normalization factor

        self.enter_point: Optional[HNSWNode] = None
        self.max_level = -1
        self.nodes: Dict[str, HNSWNode] = {}
        self.lock = threading.RLock()

    def _dist(self, a: List[float], b: List[float]) -> float:
        if self.metric == VectorMetric.COSINE:
            return compute_cosine_distance(a, b)
        elif self.metric == VectorMetric.EUCLIDEAN:
            return compute_euclidean(a, b)
        return compute_dot_distance(a, b)

    def _random_level(self) -> int:
        r = random.random()
        if r == 0:
            r = 0.0000001
        return int(-math.log(r) * self.ml)

    def insert(self, doc_id: str, vector: List[float]) -> bool:
        if len(vector) != self.dim:
            raise ValueError(f"Vector dim {len(vector)} does not match index dim {self.dim}")

        with self.lock:
            if doc_id in self.nodes:
                return False  # Already exists

            node_level = self._random_level()
            new_node = HNSWNode(doc_id, vector, node_level)
            self.nodes[doc_id] = new_node

            if self.enter_point is None:
                self.enter_point = new_node
                self.max_level = node_level
                return True

            curr = self.enter_point
            curr_dist = self._dist(vector, curr.vector)

            # Phase 1: Search top down from max_level to node_level + 1
            for l in range(self.max_level, node_level, -1):
                changed = True
                while changed:
                    changed = False
                    for neighbor in curr.neighbors[l]:
                        d = self._dist(vector, neighbor.vector)
                        if d < curr_dist:
                            curr_dist = d
                            curr = neighbor
                            changed = True

            # Phase 2: Insert into layers from min(max_level, node_level) down to 0
            ep_set = {curr}
            for l in range(min(self.max_level, node_level), -1, -1):
                w = self._search_layer(vector, ep_set, self.ef_construction, l)
                neighbors = self._select_neighbors(new_node, w, self.m if l > 0 else self.m_max0)

                for n in neighbors:
                    new_node.neighbors[l].add(n)
                    n.neighbors[l].add(new_node)
                    # Shrink connections if exceeded
                    max_conn = self.m_max0 if l == 0 else self.m
                    if len(n.neighbors[l]) > max_conn:
                        n.neighbors[l] = set(self._select_neighbors(n, list(n.neighbors[l]), max_conn))

                ep_set = set(w)

            if node_level > self.max_level:
                self.max_level = node_level
                self.enter_point = new_node

            return True

    def _search_layer(self, query: List[float], enter_points: Set[HNSWNode], ef: int, level: int) -> List[HNSWNode]:
        visited = set(enter_points)
        candidates = []
        w = []

        for ep in enter_points:
            d = self._dist(query, ep.vector)
            heapq.heappush(candidates, (d, id(ep), ep))
            heapq.heappush(w, (-d, id(ep), ep))

        while candidates:
            c_dist, _, c_node = heapq.heappop(candidates)
            furthest_w_dist = -w[0][0]

            if c_dist > furthest_w_dist:
                break

            for neighbor in c_node.neighbors[level]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    furthest_w_dist = -w[0][0]
                    d_neighbor = self._dist(query, neighbor.vector)

                    if d_neighbor < furthest_w_dist or len(w) < ef:
                        heapq.heappush(candidates, (d_neighbor, id(neighbor), neighbor))
                        heapq.heappush(w, (-d_neighbor, id(neighbor), neighbor))
                        if len(w) > ef:
                            heapq.heappop(w)

        return [node for _, _, node in sorted(w, key=lambda x: -x[0])]

    def _select_neighbors(self, base_node: HNSWNode, candidates: List[HNSWNode], m: int) -> List[HNSWNode]:
        scored = [(self._dist(base_node.vector, c.vector), c) for c in candidates if c != base_node]
        scored.sort(key=lambda x: x[0])
        return [node for _, node in scored[:m]]

    def search_knn(self, query: List[float], k: int = 10) -> List[Tuple[str, float]]:
        """Finds top-k nearest neighbors for query embedding vector."""
        if self.enter_point is None:
            return []

        with self.lock:
            curr = self.enter_point
            curr_dist = self._dist(query, curr.vector)

            for l in range(self.max_level, 0, -1):
                changed = True
                while changed:
                    changed = False
                    for neighbor in curr.neighbors[l]:
                        d = self._dist(query, neighbor.vector)
                        if d < curr_dist:
                            curr_dist = d
                            curr = neighbor
                            changed = True

            candidates = self._search_layer(query, {curr}, max(self.ef_search, k), 0)
            scored = [(c.doc_id, self._dist(query, c.vector)) for c in candidates]
            scored.sort(key=lambda x: x[1])
            return scored[:k]
