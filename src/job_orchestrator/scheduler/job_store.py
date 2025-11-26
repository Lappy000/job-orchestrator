"""
Job Store implementation for the Job Orchestrator.

This module provides in-memory storage for job state and metadata.
"""

import threading
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional
from uuid import UUID

from ..core.job import Job, JobState, JobPriority


class JobStore:
    """
    Thread-safe in-memory storage for jobs.
    
    Manages job persistence, retrieval, and querying with thread-safe operations.
    """
    
    def __init__(self):
        """Initialize the job store."""
        self._jobs: Dict[UUID, Job] = {}
        self._lock = threading.RLock()
    
    @property
    def is_empty(self) -> bool:
        """Return True when the store has no jobs."""
        with self._lock:
            return len(self._jobs) == 0
    
    def add(self, job: Job) -> None:
        """
        Add a job to the store.
        
        Args:
            job: The job to add.
        """
        with self._lock:
            self._jobs[job.id] = job
    
    def get(self, job_id: UUID) -> Optional[Job]:
        """
        Get a job by ID.
        
        Args:
            job_id: The job ID.
            
        Returns:
            The job if found, None otherwise.
        """
        with self._lock:
            return self._jobs.get(job_id)
    
    def update(self, job: Job) -> None:
        """
        Update a job in the store.
        
        Args:
            job: The job to update.
        """
        with self._lock:
            self._jobs[job.id] = job
    
    def delete(self, job_id: UUID) -> bool:
        """
        Delete a job from the store.
        
        Args:
            job_id: The job ID to delete.
            
        Returns:
            True if deleted, False if not found.
        """
        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                return True
            return False
    
    def remove(self, job_id: UUID) -> bool:
        """Alias for delete() to match test expectations."""
        return self.delete(job_id)
    
    def contains(self, job_id: UUID) -> bool:
        """
        Check if a job exists in the store.
        
        Args:
            job_id: The job ID to check.
            
        Returns:
            True if the job exists, False otherwise.
        """
        with self._lock:
            return job_id in self._jobs
    
    def get_all(self) -> List[Job]:
        """
        Get all jobs in the store.
        
        Returns:
            List of all jobs.
        """
        with self._lock:
            return list(self._jobs.values())
    
    def get_by_state(self, state: JobState) -> List[Job]:
        """
        Get all jobs with a specific state.
        
        Args:
            state: The job state to filter by.
            
        Returns:
            List of jobs with the specified state.
        """
        with self._lock:
            return [job for job in self._jobs.values() if job.state == state]
    
    def get_by_priority(self, priority: JobPriority) -> List[Job]:
        """
        Get all jobs with a specific priority.
        
        Args:
            priority: The job priority to filter by.
            
        Returns:
            List of jobs with the specified priority.
        """
        with self._lock:
            return [job for job in self._jobs.values() if job.priority == priority]
    
    def get_pending(self) -> List[Job]:
        """Get all pending jobs."""
        return self.get_by_state(JobState.PENDING)
    
    def get_scheduled(self) -> List[Job]:
        """Get all jobs that have a scheduled time set."""
        with self._lock:
            return [job for job in self._jobs.values() if job.scheduled_at is not None]
    
    def get_running(self) -> List[Job]:
        """Get all running jobs."""
        return self.get_by_state(JobState.RUNNING)
    
    def get_completed(self) -> List[Job]:
        """Get all completed jobs."""
        return self.get_by_state(JobState.COMPLETED)
    
    def get_failed(self) -> List[Job]:
        """Get all failed jobs."""
        return self.get_by_state(JobState.FAILED)
    
    def get_scheduled_jobs(self) -> List[Job]:
        """Get all jobs that have a scheduled_at time."""
        return self.get_scheduled()
    
    def get_ready_scheduled_jobs(self, now: Optional[datetime] = None) -> List[Job]:
        """
        Get scheduled jobs that are ready to run.
        
        Args:
            now: Current time (defaults to datetime.utcnow()).
            
        Returns:
            List of jobs ready to run.
        """
        if now is None:
            now = datetime.utcnow()
        
        with self._lock:
            return [
                job for job in self._jobs.values()
                if job.scheduled_at is not None and job.scheduled_at <= now
            ]

    def get_ready_scheduled(self, now: Optional[datetime] = None) -> List[Job]:
        """Backward-compatible alias for get_ready_scheduled_jobs."""
        return self.get_ready_scheduled_jobs(now=now)
    
    def get_next_scheduled_time(self) -> Optional[datetime]:
        """
        Get the next scheduled time for any job.
        
        Returns:
            The earliest scheduled time, or None if no scheduled jobs.
        """
        with self._lock:
            scheduled_jobs = [
                job for job in self._jobs.values()
                if job.scheduled_at is not None
            ]
            if not scheduled_jobs:
                return None
            return min(job.scheduled_at for job in scheduled_jobs)
    
    def clear(self) -> int:
        """
        Clear all jobs from the store.
        
        Returns:
            Number of jobs cleared.
        """
        with self._lock:
            count = len(self._jobs)
            self._jobs.clear()
            return count
    
    def cleanup_completed(self, older_than: Optional[datetime] = None) -> int:
        """
        Remove completed jobs.
        
        Args:
            older_than: Only remove jobs completed before this time.
                       If None, removes all completed jobs.
            
        Returns:
            Number of jobs removed.
        """
        with self._lock:
            to_remove = []
            for job_id, job in self._jobs.items():
                if job.state == JobState.COMPLETED:
                    if older_than is None or (job.completed_at and job.completed_at < older_than):
                        to_remove.append(job_id)
            
            for job_id in to_remove:
                del self._jobs[job_id]
            
            return len(to_remove)
    
    def cleanup_old_jobs(self, older_than: datetime) -> int:
        """
        Remove old jobs regardless of state.
        
        Args:
            older_than: Remove jobs created before this time.
            
        Returns:
            Number of jobs removed.
        """
        with self._lock:
            to_remove = [
                job_id for job_id, job in self._jobs.items()
                if job.created_at < older_than
            ]
            
            for job_id in to_remove:
                del self._jobs[job_id]
            
            return len(to_remove)
    
    def cleanup_older_than(self, max_age: timedelta) -> int:
        """Remove jobs whose completed_at (or created_at) is older than now - max_age."""
        threshold = datetime.utcnow() - max_age
        with self._lock:
            to_remove = []
            for job_id, job in self._jobs.items():
                reference = job.completed_at or job.created_at
                if reference < threshold:
                    to_remove.append(job_id)
            for job_id in to_remove:
                del self._jobs[job_id]
            return len(to_remove)
    
    def get_stats(self) -> Dict:
        """
        Get statistics about the job store.
        
        Returns:
            Dictionary with job store statistics.
        """
        with self._lock:
            total = len(self._jobs)
            by_state = {}
            for state in JobState:
                count = len([j for j in self._jobs.values() if j.state == state])
                if count > 0:
                    by_state[state.value] = count
            flat_counts = {state: count for state, count in by_state.items()}
            flat_counts.update({state.upper(): count for state, count in by_state.items()})
            return {
                "total": total,
                "by_state": by_state,
                **flat_counts,
            }
    
    def count_by_state(self, state: JobState) -> int:
        """Return the number of jobs currently in the specified state."""
        with self._lock:
            return len([job for job in self._jobs.values() if job.state == state])
    
    def mark_running(self, job_id: UUID) -> bool:
        """Transition a job to RUNNING state."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            job.state = JobState.RUNNING
            job.started_at = datetime.utcnow()
            return True
    
    def mark_completed(self, job_id: UUID, result: Optional[object] = None) -> bool:
        """Transition a job to COMPLETED state and store the result."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            job.state = JobState.COMPLETED
            job.result = result
            job.completed_at = datetime.utcnow()
            job.error = None
            return True
    
    def mark_failed(self, job_id: UUID, error: Optional[Exception] = None) -> bool:
        """Transition a job to FAILED state and record the error message."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            job.state = JobState.FAILED
            job.error = str(error) if error else None
            job.completed_at = datetime.utcnow()
            return True
    
    def mark_cancelled(self, job_id: UUID) -> bool:
        """Transition a job to CANCELLED state."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            job.state = JobState.CANCELLED
            job.completed_at = datetime.utcnow()
            return True
    
    def __len__(self) -> int:
        """Return the number of jobs in the store."""
        with self._lock:
            return len(self._jobs)
    
    def __contains__(self, job_id: UUID) -> bool:
        """Check if a job ID is in the store."""
        return self.contains(job_id)
    
    def __iter__(self):
        """Iterate over all jobs."""
        with self._lock:
            return iter(list(self._jobs.values()))
    
    def __repr__(self) -> str:
        return f"JobStore(jobs={len(self._jobs)})"


__all__ = ["JobStore"]