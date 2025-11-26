"""
Custom exceptions for the Job Orchestrator.

This module defines a hierarchy of exceptions for error handling throughout
the job orchestrator system.
"""

from typing import Optional
from uuid import UUID


class JobOrchestratorError(Exception):
    """
    Base exception for all job orchestrator errors.
    
    All custom exceptions in the job orchestrator inherit from this class,
    allowing for easy catching of any orchestrator-related error.
    """
    
    def __init__(self, message: str = ""):
        self.message = message
        super().__init__(self.message)


class JobNotFoundError(JobOrchestratorError):
    """
    Raised when a job cannot be found by its ID.
    
    Attributes:
        job_id: The UUID of the job that was not found.
    """
    
    def __init__(self, job_id: UUID, message: Optional[str] = None):
        self.job_id = job_id
        msg = message or f"Job not found: {job_id}"
        super().__init__(msg)


class JobAlreadyExistsError(JobOrchestratorError):
    """
    Raised when attempting to submit a job with a duplicate ID.
    
    Attributes:
        job_id: The UUID of the job that already exists.
    """
    
    def __init__(self, job_id: UUID, message: Optional[str] = None):
        self.job_id = job_id
        msg = message or f"Job already exists: {job_id}"
        super().__init__(msg)


class InvalidStateTransitionError(JobOrchestratorError):
    """
    Raised when an invalid state transition is attempted.
    
    Attributes:
        job_id: The UUID of the job.
        current_state: The current state of the job.
        target_state: The state that was attempted.
    """
    
    def __init__(
        self,
        job_id: UUID,
        current_state: str,
        target_state: str,
        message: Optional[str] = None
    ):
        self.job_id = job_id
        self.current_state = current_state
        self.target_state = target_state
        msg = message or (
            f"Invalid state transition for job {job_id}: "
            f"{current_state} -> {target_state}"
        )
        super().__init__(msg)


class CyclicDependencyError(JobOrchestratorError):
    """
    Raised when a cycle is detected in the DAG.
    
    A DAG by definition cannot contain cycles, so this error indicates
    an invalid workflow configuration.
    
    Attributes:
        dag_id: The UUID of the DAG containing the cycle (if available).
        cycle_path: List of job IDs forming the cycle (if detected).
    """
    
    def __init__(
        self,
        message: Optional[str] = None,
        dag_id: Optional[UUID] = None,
        cycle_path: Optional[list] = None
    ):
        self.dag_id = dag_id
        self.cycle_path = cycle_path or []
        msg = message or "Cycle detected in DAG"
        if dag_id:
            msg = f"Cycle detected in DAG {dag_id}"
        if cycle_path:
            msg += f": {' -> '.join(str(j) for j in cycle_path)}"
        super().__init__(msg)


class DAGValidationError(JobOrchestratorError):
    """
    Raised when DAG validation fails.
    
    This can occur due to cycles, missing dependencies, or other
    structural issues in the DAG.
    
    Attributes:
        dag_id: The UUID of the invalid DAG (if available).
        errors: List of validation error messages.
    """
    
    def __init__(
        self,
        message: Optional[str] = None,
        dag_id: Optional[UUID] = None,
        errors: Optional[list] = None
    ):
        self.dag_id = dag_id
        self.errors = errors or []
        msg = message or "DAG validation failed"
        if dag_id:
            msg = f"DAG {dag_id} validation failed"
        if errors:
            msg += f": {'; '.join(errors)}"
        super().__init__(msg)


class LockAcquisitionError(JobOrchestratorError):
    """
    Raised when a lock cannot be acquired within the timeout period.
    
    Attributes:
        lock_name: The name of the lock that could not be acquired.
        timeout: The timeout value that was exceeded.
        owner: The identifier of the entity that tried to acquire the lock.
    """
    
    def __init__(
        self,
        lock_name: str,
        timeout: Optional[float] = None,
        owner: Optional[str] = None,
        message: Optional[str] = None
    ):
        self.lock_name = lock_name
        self.timeout = timeout
        self.owner = owner
        msg = message or f"Failed to acquire lock: {lock_name}"
        if timeout is not None:
            msg += f" (timeout: {timeout}s)"
        super().__init__(msg)


class JobTimeoutError(JobOrchestratorError):
    """
    Raised when a job exceeds its execution timeout.
    
    Attributes:
        job_id: The UUID of the job that timed out.
        timeout: The timeout value that was exceeded.
        elapsed: The actual elapsed time before timeout.
    """
    
    def __init__(
        self,
        job_id: UUID,
        timeout: float,
        elapsed: Optional[float] = None,
        message: Optional[str] = None
    ):
        self.job_id = job_id
        self.timeout = timeout
        self.elapsed = elapsed
        msg = message or f"Job {job_id} exceeded timeout of {timeout}s"
        if elapsed is not None:
            msg += f" (elapsed: {elapsed:.2f}s)"
        super().__init__(msg)


class JobFailedError(JobOrchestratorError):
    """
    Raised when a job fails execution.
    
    Attributes:
        job_id: The UUID of the job that failed.
        error: The error message from the failed job.
        traceback: The full traceback of the error.
    """
    
    def __init__(
        self,
        job_id: UUID,
        error: str,
        traceback: Optional[str] = None,
        message: Optional[str] = None
    ):
        self.job_id = job_id
        self.error = error
        self.traceback = traceback
        msg = message or f"Job {job_id} failed: {error}"
        super().__init__(msg)


class JobCancelledError(JobOrchestratorError):
    """
    Raised when a job is cancelled during execution or while waiting.
    
    Attributes:
        job_id: The UUID of the cancelled job.
        reason: The reason for cancellation (if provided).
    """
    
    def __init__(
        self,
        job_id: UUID,
        reason: Optional[str] = None,
        message: Optional[str] = None
    ):
        self.job_id = job_id
        self.reason = reason
        msg = message or f"Job {job_id} was cancelled"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)


class QueueFullError(JobOrchestratorError):
    """
    Raised when attempting to add a job to a full queue.
    
    Attributes:
        queue_size: The current size of the queue.
        max_size: The maximum allowed size of the queue.
    """
    
    def __init__(
        self,
        queue_size: int,
        max_size: int,
        message: Optional[str] = None
    ):
        self.queue_size = queue_size
        self.max_size = max_size
        msg = message or f"Queue is full: {queue_size}/{max_size}"
        super().__init__(msg)


class WorkerPoolError(JobOrchestratorError):
    """
    Raised when there's an error with the worker pool.
    
    This can include errors related to worker creation, management,
    or communication.
    """
    pass


class StorageError(JobOrchestratorError):
    """
    Raised when there's an error with the storage backend.
    
    This can include connection errors, serialization errors,
    or other persistence-related issues.
    """
    pass


class SerializationError(JobOrchestratorError):
    """
    Raised when job serialization or deserialization fails.
    
    Attributes:
        job_id: The UUID of the job (if available).
        reason: The reason for the serialization failure.
    """
    
    def __init__(
        self,
        reason: str,
        job_id: Optional[UUID] = None,
        message: Optional[str] = None
    ):
        self.job_id = job_id
        self.reason = reason
        msg = message or f"Serialization error: {reason}"
        if job_id:
            msg = f"Serialization error for job {job_id}: {reason}"
        super().__init__(msg)


class ConfigurationError(JobOrchestratorError):
    """
    Raised when there's an error in the orchestrator configuration.
    
    Attributes:
        parameter: The name of the misconfigured parameter.
        reason: The reason the configuration is invalid.
    """
    
    def __init__(
        self,
        parameter: str,
        reason: str,
        message: Optional[str] = None
    ):
        self.parameter = parameter
        self.reason = reason
        msg = message or f"Configuration error for '{parameter}': {reason}"
        super().__init__(msg)


__all__ = [
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