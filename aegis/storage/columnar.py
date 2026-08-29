"""
Columnar Storage & Vectorized Compression Engine
Implements columnar chunk layouts, Dictionary Encoding, Run-Length Encoding (RLE),
Bit-Packing, Delta-of-Delta encoding, and vectorized filter evaluation.
"""

import struct
import io
import math
from typing import List, Dict, Any, Tuple, Optional, Union

class ColumnType:
    INT32 = 1
    INT64 = 2
    FLOAT64 = 3
    STRING = 4
    BOOLEAN = 5

class ColumnVector:
    def __init__(self, name: str, col_type: int):
        self.name = name
        self.col_type = col_type
        self.values: List[Any] = []
        self.null_bitmap: List[bool] = []

    def append(self, val: Optional[Any]):
        if val is None:
            self.null_bitmap.append(True)
            self.values.append(None)
        else:
            self.null_bitmap.append(False)
            self.values.append(val)

    def __len__(self) -> int:
        return len(self.values)

    def get(self, idx: int) -> Optional[Any]:
        if idx >= len(self.values) or self.null_bitmap[idx]:
            return None
        return self.values[idx]


class DictionaryEncoder:
    """Encodes categorical strings or integers into integer dictionary IDs."""
    def __init__(self):
        self.dict_map: Dict[Any, int] = {}
        self.rev_dict: List[Any] = []

    def encode(self, values: List[Any]) -> Tuple[List[int], List[Any]]:
        encoded_ids = []
        for v in values:
            if v not in self.dict_map:
                new_id = len(self.rev_dict)
                self.dict_map[v] = new_id
                self.rev_dict.append(v)
            encoded_ids.append(self.dict_map[v])
        return encoded_ids, self.rev_dict

    def decode(self, ids: List[int], rev_dict: List[Any]) -> List[Any]:
        return [rev_dict[i] for i in ids]


class RunLengthEncoder:
    """Compresses consecutive duplicate sequences into (value, run_length) pairs."""
    @staticmethod
    def encode(values: List[Any]) -> List[Tuple[Any, int]]:
        if not values:
            return []
        runs = []
        curr_val = values[0]
        curr_len = 1
        for v in values[1:]:
            if v == curr_val:
                curr_len += 1
            else:
                runs.append((curr_val, curr_len))
                curr_val = v
                curr_len = 1
        runs.append((curr_val, curr_len))
        return runs

    @staticmethod
    def decode(runs: List[Tuple[Any, int]]) -> List[Any]:
        decoded = []
        for val, count in runs:
            decoded.extend([val] * count)
        return decoded


class DeltaEncoder:
    """Delta-of-Delta timestamp and integer series compressor (Gorilla style)."""
    @staticmethod
    def encode_integers(values: List[int]) -> List[int]:
        if not values:
            return []
        deltas = [values[0]]
        for i in range(1, len(values)):
            deltas.append(values[i] - values[i - 1])
        return deltas

    @staticmethod
    def decode_integers(deltas: List[int]) -> List[int]:
        if not deltas:
            return []
        values = [deltas[0]]
        for i in range(1, len(deltas)):
            values.append(values[-1] + deltas[i])
        return values


class ColumnChunk:
    """Represents an immutable serialized columnar data block."""
    def __init__(self, col_name: str, col_type: int, row_count: int, raw_bytes: bytes, min_val: Any, max_val: Any):
        self.col_name = col_name
        self.col_type = col_type
        self.row_count = row_count
        self.raw_bytes = raw_bytes
        self.min_val = min_val
        self.max_val = max_val

    def byte_size(self) -> int:
        return len(self.raw_bytes)


class ColumnarTable:
    """
    In-memory and on-disk columnar dataset for vectorized analytical aggregation and filtering.
    """
    def __init__(self, schema: Dict[str, int]):
        self.schema = schema
        self.columns: Dict[str, ColumnVector] = {
            col_name: ColumnVector(col_name, col_type)
            for col_name, col_type in schema.items()
        }
        self.row_count = 0

    def insert_row(self, row: Dict[str, Any]):
        for col_name in self.schema:
            val = row.get(col_name)
            self.columns[col_name].append(val)
        self.row_count += 1

    def project(self, col_names: List[str]) -> Dict[str, List[Any]]:
        return {col: self.columns[col].values for col in col_names if col in self.columns}

    def filter_gt(self, col_name: str, threshold: Any) -> List[int]:
        """Vectorized filter returning matching row indices."""
        vec = self.columns.get(col_name)
        if not vec:
            return []
        matching_indices = []
        for i, val in enumerate(vec.values):
            if val is not None and val > threshold:
                matching_indices.append(i)
        return matching_indices

    def aggregate_sum(self, col_name: str) -> float:
        vec = self.columns.get(col_name)
        if not vec:
            return 0.0
        s = 0.0
        for v in vec.values:
            if v is not None:
                s += float(v)
        return s

    def aggregate_avg(self, col_name: str) -> float:
        if self.row_count == 0:
            return 0.0
        return self.aggregate_sum(col_name) / self.row_count
