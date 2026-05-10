"""
Job Orchestrator - A lightweight, high-performance background job processing system.

This package provides DAG-based job scheduling, priority queuing, automatic retries,
and dynamic worker scaling with minimal external dependencies.

Basic Usage:
    from job_orchestrator import Job, JobState, JobPriority, ThreadSafePriorityQueue
    
    # Create a job
    job = Job(name="my_task", priority=JobPriority.HIGH)
    
    # Add to queue
    queue = ThreadSafePriorityQueue()
    queue.push(job)
    
DAG Workflow:
    from job_orchestrator import DAGBuilder
    
    dag = (DAGBuilder("my_pipeline")
        .add_job(task_a, job_id="a")
        .add_job(task_b, job_id="b", depends_on=["a"])
        .build())
"""

__version__ = "0.1.0"
__author__ = "Lappy000"

# Core exports
from .core import (
    # Job model
    Job,
    JobState,
    JobPriority,
    RetryPolicy,
    # DAG
    DAG,
    DAGNode,
    DAGBuilder,
    # State machine
    StateMachine,
    # Configuration
    OrchestratorConfig,
    WorkerPoolConfig,
    WorkerConfig,  # Backwards compatibility alias
    QueueConfig,
    RetryConfig,
    DeadLetterQueueConfig,
    StorageConfig,
    LockConfig,
    LoggingConfig,
    MetricsConfig,
    # Exceptions
    JobOrchestratorError,
    JobNotFoundError,
    JobAlreadyExistsError,
    InvalidStateTransitionError,
    CyclicDependencyError,
    DAGValidationError,
    LockAcquisitionError,
    JobTimeoutError,
    JobFailedError,
    JobCancelledError,
    QueueFullError,
    WorkerPoolError,
    StorageError,
    SerializationError,
    ConfigurationError,
)

# Queue exports
from .queue import (
    ThreadSafePriorityQueue,
    QueueEntry,
)

# Scheduler exports
from .scheduler import (
    Scheduler,
    RetryHandler,
    DeadLetterQueue,
    DLQEntry,
    DLQEntryStatus,
    DLQStats,
)


__all__ = [
    # Version
    "__version__",
    "__author__",
    # Job model
    "Job",
    "JobState",
    "JobPriority",
    "RetryPolicy",
    # DAG
    "DAG",
    "DAGNode",
    "DAGBuilder",
    # State machine
    "StateMachine",
    # Queue
    "ThreadSafePriorityQueue",
    "QueueEntry",
    # Configuration
    "OrchestratorConfig",
    "WorkerPoolConfig",
    "WorkerConfig",  # Backwards compatibility alias
    "QueueConfig",
    "RetryConfig",
    "DeadLetterQueueConfig",
    "StorageConfig",
    "LockConfig",
    "LoggingConfig",
    "MetricsConfig",
    # Scheduler
    "Scheduler",
    "RetryHandler",
    "RetryPolicy",
    "DeadLetterQueue",
    "DLQEntry",
    "DLQEntryStatus",
    "DLQStats",
    # Exceptions
    "JobOrchestratorError",
    "JobNotFoundError",
    "JobAlreadyExistsError",
    "InvalidStateTransitionError",
    "CyclicDependencyError",
    "DAGValidationError",
    "LockAcquisitionError",
    "JobTimeoutError",
    "JobFailedError",
    "JobCancelledError",
    "QueueFullError",
    "WorkerPoolError",
    "StorageError",
    "SerializationError",
    "ConfigurationError",
]