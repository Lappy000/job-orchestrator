"""
Base storage interface for the Job Orchestrator.

This module defines abstract base classes for storage backends
that persist job state, results, and metadata.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from ..core.job import Job


class BaseStorage(ABC):
    """
    Abstract base class for job storage backends.
    
    Storage backends are responsible for persisting job state and results.
    Implementations can use different storage technologies such as
    in-memory storage, Redis, PostgreSQL, or file-based storage.
    
    All methods should be thread-safe for use with concurrent workers.
    
    Example:
        >>> class MyStorage(BaseStorage):
        ...     def save(self, job: Job) -> None:
        ...         # Persist job to storage
        ...         pass
        ...
        ...     def get(self, job_id: str) -> Optional[Job]:
        ...         # Retrieve job from storage
        ...         pass
    """
    
    @abstractmethod
    def save(self, job: "Job") -> None:
        """
        Save or create a job in storage.
        
        Args:
            job: The job to save.
            
        Raises:
            StorageError: If the save operation fails.
        """
        ...
    
    @abstractmethod
    def update(self, job: "Job") -> None:
        """
        Update an existing job in storage.
        
        Args:
            job: The job with updated state.
            
        Raises:
            StorageError: If the update operation fails.
            JobNotFoundError: If the job doesn't exist.
        """
        ...
    
    @abstractmethod
    def get(self, job_id: str) -> Optional["Job"]:
        """
        Retrieve a job by ID.
        
        Args:
            job_id: The unique identifier of the job.
            
        Returns:
            The job if found, None otherwise.
            
        Raises:
            StorageError: If the retrieval operation fails.
        """
        ...
    
    @abstractmethod
    def delete(self, job_id: str) -> bool:
        """
        Delete a job from storage.
        
        Args:
            job_id: The unique identifier of the job to delete.
            
        Returns:
            True if the job was deleted, False if not found.
            
        Raises:
            StorageError: If the delete operation fails.
        """
        ...
    
    @abstractmethod
    def list_jobs(
        self,
        state: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List["Job"]:
        """
        List jobs with optional filtering.
        
        Args:
            state: Filter by job state (e.g., "pending", "running").
            limit: Maximum number of jobs to return.
            offset: Number of jobs to skip (for pagination).
            
        Returns:
            List of jobs matching the criteria.
            
        Raises:
            StorageError: If the list operation fails.
        """
        ...
    
    def exists(self, job_id: str) -> bool:
        """
        Check if a job exists in storage.
        
        Args:
            job_id: The unique identifier of the job.
            
        Returns:
            True if the job exists, False otherwise.
        """
        return self.get(job_id) is not None
    
    def count(self, state: Optional[str] = None) -> int:
        """
        Count jobs in storage.
        
        Args:
            state: Optional state filter.
            
        Returns:
            Number of jobs matching the criteria.
        """
        return len(self.list_jobs(state=state))


class InMemoryStorage(BaseStorage):
    """
    In-memory storage backend for development and testing.
    
    WARNING: All data is lost when the process exits.
    Use this only for development, testing, or single-process deployments.
    """
    
    def __init__(self) -> None:
        """Initialize the in-memory storage."""
        self._jobs: Dict[str, "Job"] = {}
    
    def save(self, job: "Job") -> None:
        """Save a job to memory."""
        self._jobs[str(job.id)] = job
    
    def update(self, job: "Job") -> None:
        """Update a job in memory."""
        self._jobs[str(job.id)] = job
    
    def get(self, job_id: str) -> Optional["Job"]:
        """Get a job from memory."""
        return self._jobs.get(job_id)
    
    def delete(self, job_id: str) -> bool:
        """Delete a job from memory."""
        if job_id in self._jobs:
            del self._jobs[job_id]
            return True
        return False
    
    def list_jobs(
        self,
        state: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List["Job"]:
        """List jobs from memory with optional filtering."""
        jobs = list(self._jobs.values())
        
        if state:
            jobs = [j for j in jobs if j.state.value == state]
        
        jobs = jobs[offset:]
        
        if limit:
            jobs = jobs[:limit]
        
        return jobs
    
    def clear(self) -> None:
        """Clear all jobs from storage."""
        self._jobs.clear()


__all__ = [
    "BaseStorage",
    "InMemoryStorage",
]