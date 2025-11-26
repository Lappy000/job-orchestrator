"""
Core components for the Job Orchestrator.

This module contains the fundamental data structures and classes:
- Job model and related enums
- DAG (Directed Acyclic Graph) for job dependencies
- State machine for job lifecycle management
- Configuration dataclasses
- Custom exceptions
"""

from .job import Job, JobState, JobPriority, RetryPolicy
from .dag import DAG, DAGNode, DAGBuilder
from .state import StateMachine
from .config import (
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
)
from .exceptions import (
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


__all__ = [
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