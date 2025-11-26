"""
Job model and related data structures for the Job Orchestrator.

This module defines the core Job dataclass along with supporting enums
and the RetryPolicy configuration.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
from uuid import UUID, uuid4
import random


class JobState(Enum):
    """
    Job lifecycle states.
    
    Jobs progress through these states during their lifecycle:
    - PENDING: Job has been created but not yet queued
    - SCHEDULED: Job is in the queue waiting for a worker
    - RUNNING: Job is currently being executed by a worker
    - COMPLETED: Job has finished successfully
    - FAILED: Job has failed after exhausting all retries
    - RETRYING: Job has failed and is waiting for retry
    - CANCELLED: Job was manually cancelled
    - TIMEOUT: Job exceeded its execution timeout
    """
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class JobPriority(Enum):
    """
    Job priority levels.
    
    Lower numeric values indicate higher priority.
    Jobs with higher priority are executed before lower priority jobs.
    """
    CRITICAL = 0    # Highest priority - urgent jobs
    HIGH = 1        # High priority jobs
    NORMAL = 2      # Default priority
    LOW = 3         # Low priority jobs
    BACKGROUND = 4  # Lowest priority - background tasks


@dataclass
class RetryPolicy:
    """
    Configuration for job retry behavior.
    
    Controls how jobs are retried after failures, including the number
    of retries and the delay calculation strategy.
    
    Attributes:
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds for the first retry.
        max_delay: Maximum delay in seconds (caps exponential growth).
        exponential_base: Multiplier for exponential backoff calculation.
        jitter: If True, adds randomness to prevent thundering herd.
        retry_on: Tuple of exception types that should trigger a retry.
    """
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 300.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on: tuple = field(default_factory=lambda: (Exception,))
    
    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt using exponential backoff.
        
        Args:
            attempt: The current retry attempt number (0-indexed).
            
        Returns:
            The delay in seconds before the next retry.
        """
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        if self.jitter:
            # Add randomness: 50-150% of calculated delay
            delay *= (0.5 + random.random())
        return delay
    
    def should_retry(self, exception: Exception) -> bool:
        """
        Check if the given exception should trigger a retry.
        
        Args:
            exception: The exception that was raised.
            
        Returns:
            True if the exception type is in retry_on, False otherwise.
        """
        return isinstance(exception, self.retry_on)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize retry policy to dictionary."""
        return {
            "max_retries": self.max_retries,
            "base_delay": self.base_delay,
            "max_delay": self.max_delay,
            "exponential_base": self.exponential_base,
            "jitter": self.jitter,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetryPolicy":
        """Deserialize retry policy from dictionary."""
        return cls(
            max_retries=data.get("max_retries", 3),
            base_delay=data.get("base_delay", 1.0),
            max_delay=data.get("max_delay", 300.0),
            exponential_base=data.get("exponential_base", 2.0),
            jitter=data.get("jitter", True),
        )


@dataclass
class Job:
    """
    Core job entity representing a unit of work.
    
    A Job encapsulates all the information needed to execute a function
    with given arguments, track its state, handle retries, and store results.
    
    Attributes:
        id: Unique identifier for the job.
        name: Human-readable name for the job.
        description: Optional description of what the job does.
        func: The callable function to execute (may be None if using func_path).
        func_path: Importable path to the function (e.g., 'mymodule.tasks.process').
        args: Positional arguments to pass to the function.
        kwargs: Keyword arguments to pass to the function.
        priority: Job priority level for queue ordering.
        scheduled_at: When the job should be executed (None = immediately).
        timeout: Maximum execution time in seconds (None = no timeout).
        state: Current lifecycle state of the job.
        attempt: Current attempt number (0 = first attempt).
        depends_on: List of job IDs this job depends on.
        retry_policy: Configuration for retry behavior.
        created_at: When the job was created.
        started_at: When execution began (None if not started).
        completed_at: When execution finished (None if not completed).
        result: The return value from successful execution.
        error: Error message if job failed.
        traceback: Full traceback if job failed.
        tags: Metadata tags for filtering and grouping.
        metadata: Additional custom metadata.
    """
    
    # Identity
    id: UUID = field(default_factory=uuid4)
    name: str = ""
    description: str = ""
    
    # Execution
    func: Optional[Callable] = field(default=None, repr=False)
    func_path: str = ""
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    
    # Scheduling
    priority: JobPriority = JobPriority.NORMAL
    scheduled_at: Optional[datetime] = None
    timeout: Optional[float] = None
    
    # State
    state: JobState = JobState.PENDING
    attempt: int = 0
    
    # Dependencies
    depends_on: List[UUID] = field(default_factory=list)
    
    # Retry
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    retry_count: int = 0
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Results
    result: Any = field(default=None, repr=False)
    error: Optional[str] = None
    traceback: Optional[str] = field(default=None, repr=False)
    
    # Metadata
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self) -> None:
        """Normalize basic fields after construction."""
        if isinstance(self.priority, str):
            self.priority = JobPriority[self.priority.upper()]
        if self.scheduled_at and self.scheduled_at.tzinfo is not None:
            self.scheduled_at = self.scheduled_at.replace(tzinfo=None)
        if self.created_at.tzinfo is not None:
            self.created_at = self.created_at.replace(tzinfo=None)
    
    def __lt__(self, other: "Job") -> bool:
        """
        Comparison for priority queue ordering.
        
        Jobs are compared first by priority (lower value = higher priority),
        then by scheduled time (earlier = higher priority).
        """
        if not isinstance(other, Job):
            return NotImplemented
        
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        
        self_time = self.scheduled_at or self.created_at
        other_time = other.scheduled_at or other.created_at
        return self_time < other_time
    
    def __eq__(self, other: object) -> bool:
        """Jobs are equal if they have the same ID."""
        if not isinstance(other, Job):
            return NotImplemented
        return self.id == other.id
    
    def __hash__(self) -> int:
        """Hash based on job ID."""
        return hash(self.id)
    
    @property
    def is_terminal(self) -> bool:
        """Check if job is in a terminal state (finished)."""
        return self.state in {
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.CANCELLED,
        }
    
    @property
    def is_active(self) -> bool:
        """Check if job is active (not finished)."""
        return self.state in {
            JobState.PENDING,
            JobState.SCHEDULED,
            JobState.RUNNING,
            JobState.RETRYING,
        }
    
    @property
    def can_retry(self) -> bool:
        """Check if job has retries remaining."""
        return self.attempt < self.retry_policy.max_retries
    
    @property
    def execution_time(self) -> Optional[float]:
        """
        Calculate the execution time in seconds.
        
        Returns None if the job hasn't started or completed.
        """
        if self.started_at is None:
            return None
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize job to dictionary for storage.
        
        Note: The func attribute is not serialized; use func_path instead.
        
        Returns:
            Dictionary representation of the job.
        """
        return {
            "id": str(self.id),
            "name": self.name,
            "description": self.description,
            "func_path": self.func_path,
            "args": list(self.args),
            "kwargs": self.kwargs,
            "priority": self.priority.value,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "timeout": self.timeout,
            "state": self.state.value,
            "attempt": self.attempt,
            "depends_on": [str(uid) for uid in self.depends_on],
            "retry_policy": self.retry_policy.to_dict(),
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self._serialize_result(self.result),
            "error": self.error,
            "traceback": self.traceback,
            "tags": self.tags,
            "metadata": self.metadata,
        }
    
    @staticmethod
    def _serialize_result(result: Any) -> Any:
        """
        Attempt to serialize a result value.
        
        For complex objects that can't be JSON serialized,
        returns a string representation.
        """
        if result is None:
            return None
        
        # Try to keep simple types as-is
        if isinstance(result, (str, int, float, bool, list, dict)):
            return result
        
        # For complex types, convert to string
        try:
            return str(result)
        except Exception:
            return "<non-serializable result>"
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        """
        Deserialize job from dictionary.
        
        Args:
            data: Dictionary representation of the job.
            
        Returns:
            A new Job instance.
        """
        job = cls(
            id=UUID(data["id"]),
            name=data.get("name", ""),
            description=data.get("description", ""),
            func_path=data.get("func_path", ""),
            args=tuple(data.get("args", [])),
            kwargs=data.get("kwargs", {}),
            priority=JobPriority(data.get("priority", JobPriority.NORMAL.value)),
            timeout=data.get("timeout"),
            state=JobState(data.get("state", JobState.PENDING.value)),
            attempt=data.get("attempt", 0),
            depends_on=[UUID(uid) for uid in data.get("depends_on", [])],
            tags=data.get("tags", {}),
            metadata=data.get("metadata", {}),
        )
        
        # Parse timestamps
        if data.get("scheduled_at"):
            job.scheduled_at = datetime.fromisoformat(data["scheduled_at"])
        if data.get("created_at"):
            job.created_at = datetime.fromisoformat(data["created_at"])
        if data.get("started_at"):
            job.started_at = datetime.fromisoformat(data["started_at"])
        if data.get("completed_at"):
            job.completed_at = datetime.fromisoformat(data["completed_at"])
        
        # Parse retry policy
        if retry_data := data.get("retry_policy"):
            job.retry_policy = RetryPolicy.from_dict(retry_data)
        
        # Set results
        job.result = data.get("result")
        job.error = data.get("error")
        job.traceback = data.get("traceback")
        
        return job
    
    def copy(self) -> "Job":
        """
        Create a copy of this job with a new ID.
        
        Useful for re-running a job with the same configuration.
        
        Returns:
            A new Job instance with the same configuration but new ID.
        """
        return Job(
            name=self.name,
            description=self.description,
            func=self.func,
            func_path=self.func_path,
            args=self.args,
            kwargs=self.kwargs.copy(),
            priority=self.priority,
            timeout=self.timeout,
            depends_on=self.depends_on.copy(),
            retry_policy=RetryPolicy(
                max_retries=self.retry_policy.max_retries,
                base_delay=self.retry_policy.base_delay,
                max_delay=self.retry_policy.max_delay,
                exponential_base=self.retry_policy.exponential_base,
                jitter=self.retry_policy.jitter,
                retry_on=self.retry_policy.retry_on,
            ),
            tags=self.tags.copy(),
            metadata=self.metadata.copy(),
        )
    
    # ------------------------------------------------------------------
    # Factory helpers demanded by tests
    # ------------------------------------------------------------------
    @classmethod
    def create(
        cls,
        *,
        name: str,
        func: Optional[Callable] = None,
        args: Union[Sequence[Any], Tuple[Any, ...]] = (),
        kwargs: Optional[Dict[str, Any]] = None,
        priority: JobPriority = JobPriority.NORMAL,
        scheduled_at: Optional[datetime] = None,
        timeout: Optional[float] = None,
        tags: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        depends_on: Optional[Sequence[UUID]] = None,
        retry_policy: Optional[RetryPolicy] = None,
        func_path: str = "",
    ) -> "Job":
        """Test-friendly factory that mirrors legacy API expectations."""
        if isinstance(priority, str):
            priority = JobPriority[priority.upper()]
        job = cls(
            name=name,
            func=func,
            func_path=func_path or (f"{func.__module__}.{func.__qualname__}" if func else ""),
            args=tuple(args),
            kwargs=kwargs or {},
            priority=priority,
            scheduled_at=scheduled_at,
            timeout=timeout,
            tags=tags or {},
            metadata=metadata or {},
            depends_on=list(depends_on or []),
            retry_policy=retry_policy or RetryPolicy(),
        )
        return job


__all__ = [
    "JobState",
    "JobPriority",
    "RetryPolicy",
    "Job",
]