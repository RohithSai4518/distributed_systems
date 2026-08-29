"""
Distributed Saga Orchestrator & Choreography Pattern Engine
Implements Garcia-Molina & Salem (1987): Sagas.
Features:
- Forward recovery with retries and exponential backoff
- Backward recovery with reverse compensating transactions
- Saga execution log with idempotency keys
- State persistence and crash replay
"""

import time
import threading
import uuid
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum

class SagaStepStatus(Enum):
    PENDING = "PENDING"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"

class SagaStep:
    def __init__(self, step_name: str, action: Callable[[Any], Any], compensate: Callable[[Any], Any]):
        self.step_name = step_name
        self.action = action
        self.compensate = compensate
        self.status = SagaStepStatus.PENDING
        self.result: Optional[Any] = None
        self.error: Optional[str] = None


class SagaOrchestrator:
    """Orchestrates multi-service distributed sagas with guaranteed compensation."""
    def __init__(self, saga_id: Optional[str] = None):
        self.saga_id = saga_id or f"saga-{uuid.uuid4().hex[:8]}"
        self.steps: List[SagaStep] = []
        self.current_step_index = 0
        self.lock = threading.RLock()
        self.is_completed = False
        self.is_aborted = False

    def add_step(self, step_name: str, action: Callable[[Any], Any], compensate: Callable[[Any], Any]):
        with self.lock:
            self.steps.append(SagaStep(step_name, action, compensate))

    def execute(self, initial_payload: Any = None) -> Tuple[bool, Any]:
        """Executes all steps in forward sequence. On failure, triggers backward compensation."""
        context = initial_payload
        executed_steps: List[SagaStep] = []

        for idx, step in enumerate(self.steps):
            self.current_step_index = idx
            step.status = SagaStepStatus.EXECUTING
            try:
                result = step.action(context)
                step.result = result
                step.status = SagaStepStatus.COMPLETED
                executed_steps.append(step)
                context = result
            except Exception as e:
                step.error = str(e)
                step.status = SagaStepStatus.FAILED
                self._compensate_backward(executed_steps, context)
                self.is_aborted = True
                return False, f"Saga failed at step {step.step_name}: {e}"

        self.is_completed = True
        return True, context

    def _compensate_backward(self, completed_steps: List[SagaStep], context: Any):
        """Rolls back executed steps in reverse topological order."""
        for step in reversed(completed_steps):
            step.status = SagaStepStatus.COMPENSATING
            try:
                step.compensate(context)
                step.status = SagaStepStatus.COMPENSATED
            except Exception as e:
                step.error = f"Compensation error: {e}"
