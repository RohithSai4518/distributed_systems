"""
Linearizability Verification Engine (Wing & Gong Model Checking).
Validates whether a concurrent execution history of Read and Write operations
matches a valid sequential single-threaded execution without causality violations.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class OperationType(Enum):
    WRITE = "WRITE"
    READ = "READ"
    CAS = "CAS"


@dataclass
class OpEvent:
    op_id: str
    client_id: str
    op_type: OperationType
    key: str
    input_val: Optional[Any]
    output_val: Optional[Any]
    start_ts: int
    end_ts: int


class LinearizabilityChecker:
    """
    State-space search checker for linearizability verification.
    """

    @staticmethod
    def verify_single_key(events: List[OpEvent], initial_value: Optional[Any] = None) -> Tuple[bool, Optional[str]]:
        """
        Verifies if events for a single key are linearizable.
        Uses recursive branch search respecting real-time precedence order:
        If e1.end_ts < e2.start_ts, then e1 MUST be linearized before e2.
        """
        # Sort events by start timestamp
        n = len(events)
        if n == 0:
            return True, None

        # Build precedence graph: must_precede[i][j] is True if events[i].end_ts < events[j].start_ts
        must_precede = [[False] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                if i != j and events[i].end_ts < events[j].start_ts:
                    must_precede[i][j] = True

        used = [False] * n

        def search(current_state_val: Optional[Any], count_applied: int) -> bool:
            if count_applied == n:
                return True

            for i in range(n):
                if not used[i]:
                    # Check if all events that must precede i have already been used
                    can_execute = True
                    for prev_idx in range(n):
                        if must_precede[prev_idx][i] and not used[prev_idx]:
                            can_execute = False
                            break

                    if not can_execute:
                        continue

                    # Check if operation semantics match current_state_val
                    ev = events[i]
                    if ev.op_type == OperationType.READ:
                        if ev.output_val != current_state_val:
                            continue  # Mismatch, cannot linearize here
                        next_state = current_state_val
                    elif ev.op_type == OperationType.WRITE:
                        next_state = ev.input_val
                    elif ev.op_type == OperationType.CAS:
                        prev_expected = ev.input_val[0] if isinstance(ev.input_val, (list, tuple)) else ev.input_val
                        new_val = ev.input_val[1] if isinstance(ev.input_val, (list, tuple)) and len(ev.input_val) > 1 else ev.input_val
                        cas_success = ev.output_val
                        if cas_success:
                            if current_state_val != prev_expected:
                                continue
                            next_state = new_val
                        else:
                            if current_state_val == prev_expected:
                                continue
                            next_state = current_state_val

                    # Recurse
                    used[i] = True
                    if search(next_state, count_applied + 1):
                        return True
                    used[i] = False

            return False

        is_linearizable = search(initial_value, 0)
        if is_linearizable:
            return True, "History is strictly linearizable."
        else:
            return False, "Linearizability violation detected! Read stale or uncommitted state."
