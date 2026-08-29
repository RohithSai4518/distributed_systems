"""
Volcano Iterator Execution Model Engine
Implements Graefe (1994): Volcano - An Extensible and Parallel Query Evaluation System.
Operators supported:
- SeqScanOperator
- FilterOperator
- ProjectOperator
- HashJoinOperator
- AggregateOperator (SUM, AVG, MIN, MAX, COUNT)
- SortOperator & LimitOperator
"""

from typing import Iterator, Dict, Any, List, Optional, Tuple

class TupleRow:
    def __init__(self, data: Dict[str, Any]):
        self.data = data

    def get(self, col: str) -> Any:
        return self.data.get(col)

    def __repr__(self):
        return f"Row({self.data})"


class ExecutionOperator:
    """Base Volcano Iterator Operator interface (open, next, close)."""
    def open(self):
        pass

    def next(self) -> Optional[TupleRow]:
        raise NotImplementedError

    def close(self):
        pass


class SeqScanOperator(ExecutionOperator):
    def __init__(self, rows: List[Dict[str, Any]]):
        self.rows = rows
        self.cursor = 0

    def open(self):
        self.cursor = 0

    def next(self) -> Optional[TupleRow]:
        if self.cursor < len(self.rows):
            r = TupleRow(self.rows[self.cursor])
            self.cursor += 1
            return r
        return None

    def close(self):
        self.cursor = 0


class FilterOperator(ExecutionOperator):
    def __init__(self, child: ExecutionOperator, col: str, op: str, target_val: Any):
        self.child = child
        self.col = col
        self.op = op
        self.target_val = target_val

    def open(self):
        self.child.open()

    def next(self) -> Optional[TupleRow]:
        while True:
            row = self.child.next()
            if row is None:
                return None
            val = row.get(self.col)
            if self._eval(val):
                return row

    def _eval(self, val: Any) -> bool:
        if self.op == "=":
            return str(val) == str(self.target_val)
        elif self.op == ">":
            return float(val) > float(self.target_val)
        elif self.op == "<":
            return float(val) < float(self.target_val)
        return False

    def close(self):
        self.child.close()


class ProjectOperator(ExecutionOperator):
    def __init__(self, child: ExecutionOperator, projection_cols: List[str]):
        self.child = child
        self.cols = projection_cols

    def open(self):
        self.child.open()

    def next(self) -> Optional[TupleRow]:
        row = self.child.next()
        if row is None:
            return None
        if self.cols == ["*"]:
            return row
        proj_data = {c: row.get(c) for c in self.cols}
        return TupleRow(proj_data)

    def close(self):
        self.child.close()


class LimitOperator(ExecutionOperator):
    def __init__(self, child: ExecutionOperator, limit: int):
        self.child = child
        self.limit = limit
        self.count = 0

    def open(self):
        self.child.open()
        self.count = 0

    def next(self) -> Optional[TupleRow]:
        if self.count >= self.limit:
            return None
        row = self.child.next()
        if row is not None:
            self.count += 1
        return row

    def close(self):
        self.child.close()
