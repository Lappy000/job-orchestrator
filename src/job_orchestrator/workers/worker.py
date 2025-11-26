"""
Base Worker classes for the Job Orchestrator.

This module defines the abstract base worker class and supporting data structures
for worker state management and monitoring.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional
import logging
import uuid

if TYPE_CHECKING:
    from ..scheduler.scheduler import Scheduler
    from ..core.job import Job


logger = logging.getLogger(__name__)


class WorkerState(Enum):
    """
    Worker lifecycle states.
    
    Workers transition through these states during their lifecycle:
    - IDLE: Worker is waiting for a job
    - BUSY: Worker is currently executing a job
    - STOPPING: Worker is gracefully shutting down
    - STOPPED: Worker has stopped completely
    """
    IDLE = "idle"
    BUSY = "busy"
    STOPPING = "stopping"
    STOPPED = "stopped"


class WorkerType(Enum):
    """
    Worker execution model types.
    
    Determines how the worker executes jobs:
    - THREAD: Jobs run in a separate thread (GIL-bound, good for I/O)
    - PROCESS: Jobs run in a separate process (true parallelism, good for CPU)
    - ASYNC: Jobs run in an asyncio event loop (cooperative multitasking)
    """
    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"


@dataclass
class WorkerInfo:
    """
    Worker status information for monitoring and introspection.
    
    Provides a snapshot of a worker's current state and statistics.
    
    Attributes:
        worker_id: Unique identifier for the worker.
        worker_type: The type of worker (thread, process, async).
        state: Current lifecycle state.
        current_job_id: ID of the job being executed (if any).
        jobs_completed: Total number of successfully completed jobs.
        jobs_failed: Total number of failed jobs.
        started_at: When the worker was started.
        last_heartbeat: Last time the worker reported being alive.
        total_execution_time: Total time spent executing jobs in seconds.
        avg_job_time: Average job execution time in seconds.
    """
    worker_id: str
    worker_type: WorkerType
    state: WorkerState
    current_job_id: Optional[str] = None
    jobs_completed: int = 0
    jobs_failed: int = 0
    started_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    total_execution_time: float = 0.0
    
    @property
    def avg_job_time(self) -> float:
        """Calculate average job execution time."""
        total_jobs = self.jobs_completed + self.jobs_failed
        if total_jobs == 0:
            return 0.0
        return self.total_execution_time / total_jobs
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "worker_id": self.worker_id,
            "worker_type": self.worker_type.value,
            "state": self.state.value,
            "current_job_id": self.current_job_id,
            "jobs_completed": self.jobs_completed,
            "jobs_failed": self.jobs_failed,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "total_execution_time": self.total_execution_time,
            "avg_job_time": self.avg_job_time,
        }


class BaseWorker(ABC):
    """
    Abstract base class for workers.
    
    Defines the interface that all worker implementations must follow.
    Workers are responsible for fetching jobs from the scheduler and
    executing them.
    
    Subclasses must implement:
    - start(): Start the worker
    - stop(): Stop the worker
    - _run_loop(): Main worker loop for fetching and executing jobs
    
    Attributes:
        worker_id: Unique identifier for this worker.
        worker_type: The type of worker (must be set by subclass).
    """
    
    # Must be set by subclass
    worker_type: WorkerType = WorkerType.THREAD
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        scheduler: Optional["Scheduler"] = None,
    ):
        """
        Initialize the base worker.
        
        Args:
            worker_id: Unique identifier for this worker. If None, a UUID is generated.
            scheduler: The scheduler to fetch jobs from.
        """
        self.worker_id = worker_id or str(uuid.uuid4())
        self._scheduler = scheduler
        self._state = WorkerState.STOPPED
        self._current_job: Optional["Job"] = None
        
        # Statistics
        self._jobs_completed = 0
        self._jobs_failed = 0
        self._total_execution_time = 0.0
        
        # Timestamps
        self._started_at: Optional[datetime] = None
        self._last_heartbeat: Optional[datetime] = None
        
        logger.debug(f"Worker {self.worker_id} initialized")
    
    @property
    def state(self) -> WorkerState:
        """Get the current worker state."""
        return self._state
    
    @state.setter
    def state(self, value: WorkerState) -> None:
        """Set the worker state."""
        old_state = self._state
        self._state = value
        logger.debug(
            f"Worker {self.worker_id} state changed: "
            f"{old_state.value} -> {value.value}"
        )
    
    def set_scheduler(self, scheduler: "Scheduler") -> None:
        """
        Set the scheduler for this worker.
        
        Args:
            scheduler: The scheduler to fetch jobs from.
        """
        self._scheduler = scheduler
    
    @abstractmethod
    def start(self) -> None:
        """
        Start the worker.
        
        This should initialize any resources (threads, processes, etc.)
        and begin the job processing loop.
        """
        pass
    
    @abstractmethod
    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Stop the worker.
        
        This should gracefully shut down the worker, optionally waiting
        for the current job to complete.
        
        Args:
            wait: If True, wait for the current job to complete.
            timeout: Maximum time to wait for shutdown in seconds.
        """
        pass
    
    @abstractmethod
    def _run_loop(self) -> None:
        """
        Main worker loop - fetch and execute jobs.
        
        This method should:
        1. Fetch jobs from the scheduler
        2. Execute each job
        3. Handle success and failure
        4. Update statistics
        5. Continue until stopped
        """
        pass
    
    def _execute_job(self, job: "Job") -> Any:
        """
        Execute a job and track statistics.
        
        This is a helper method that subclasses can use for job execution.
        It handles state transitions and timing.
        
        Args:
            job: The job to execute.
            
        Returns:
            The result of the job execution.
        """
        from datetime import datetime
        
        self._state = WorkerState.BUSY
        self._current_job = job
        self._update_heartbeat()
        
        start_time = datetime.utcnow()
        
        try:
            # Run the job through the scheduler
            result = self._scheduler.run_job(job)
            
            # Update statistics based on result
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            self._total_execution_time += execution_time
            
            if result.success:
                self._jobs_completed += 1
                logger.debug(
                    f"Worker {self.worker_id} completed job {job.id} "
                    f"in {execution_time:.2f}s"
                )
            else:
                self._jobs_failed += 1
                logger.debug(
                    f"Worker {self.worker_id} failed job {job.id} "
                    f"in {execution_time:.2f}s: {result.error}"
                )
            
            return result
            
        finally:
            self._state = WorkerState.IDLE
            self._current_job = None
            self._update_heartbeat()
    
    def _update_heartbeat(self) -> None:
        """Update the last heartbeat timestamp."""
        self._last_heartbeat = datetime.utcnow()
    
    def get_info(self) -> WorkerInfo:
        """
        Get current worker status information.
        
        Returns:
            WorkerInfo with current state and statistics.
        """
        return WorkerInfo(
            worker_id=self.worker_id,
            worker_type=self.worker_type,
            state=self._state,
            current_job_id=str(self._current_job.id) if self._current_job else None,
            jobs_completed=self._jobs_completed,
            jobs_failed=self._jobs_failed,
            started_at=self._started_at,
            last_heartbeat=self._last_heartbeat,
            total_execution_time=self._total_execution_time,
        )
    
    @property
    def is_alive(self) -> bool:
        """
        Check if the worker is alive (not stopped).
        
        Returns:
            True if worker is running (idle or busy), False otherwise.
        """
        return self._state in {WorkerState.IDLE, WorkerState.BUSY}
    
    @property
    def is_idle(self) -> bool:
        """
        Check if the worker is idle (not processing a job).
        
        Returns:
            True if worker is idle, False otherwise.
        """
        return self._state == WorkerState.IDLE
    
    @property
    def is_busy(self) -> bool:
        """
        Check if the worker is busy (processing a job).
        
        Returns:
            True if worker is busy, False otherwise.
        """
        return self._state == WorkerState.BUSY
    
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"id={self.worker_id!r}, "
            f"type={self.worker_type.value}, "
            f"state={self._state.value})"
        )


__all__ = [
    "WorkerState",
    "WorkerType",
    "WorkerInfo",
    "BaseWorker",
]