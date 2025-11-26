"""
Retry handler implementation for the Job Orchestrator.

This module provides the RetryPolicy configuration and RetryHandler class
for managing job retry logic with exponential backoff and jitter.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional, Tuple, Type
import logging
import math
import random

from ..core.job import Job, JobState

if TYPE_CHECKING:
    from .scheduler import Scheduler


logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """
    Configuration for job retry behavior.
    
    Controls how jobs are retried after failures, including the number
    of retries and the delay calculation strategy using exponential backoff.
    
    Attributes:
        max_retries: Maximum number of retry attempts (default: 3).
        initial_delay: Initial delay in seconds for the first retry (default: 1.0).
        max_delay: Maximum delay in seconds (caps exponential growth, default: 300.0).
        exponential_base: Multiplier for exponential backoff calculation (default: 2.0).
        jitter: If True, adds randomness to prevent thundering herd (default: True).
        retry_on: Tuple of exception types that should trigger a retry.
        
    Example:
        >>> policy = RetryPolicy(max_retries=5, initial_delay=2.0)
        >>> delay = policy.calculate_delay(retry_count=2)
        >>> print(f"Wait {delay:.2f}s before retry")
    """
    max_retries: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 300.0  # 5 minutes max
    exponential_base: float = 2.0
    jitter: bool = True  # Add randomness to prevent thundering herd
    retry_on: Tuple[Type[Exception], ...] = field(default_factory=lambda: (Exception,))
    
    def calculate_delay(self, retry_count: int) -> float:
        """
        Calculate delay before next retry using exponential backoff.
        
        The formula is: delay = initial_delay * (base ^ retry_count)
        With optional jitter: delay * random(0.5, 1.5)
        Capped at max_delay.
        
        Args:
            retry_count: The current retry attempt number (0-indexed).
            
        Returns:
            The delay in seconds before the next retry.
            
        Example:
            >>> policy = RetryPolicy(initial_delay=1.0, exponential_base=2.0)
            >>> policy.calculate_delay(0)  # ~1.0s (with jitter variation)
            >>> policy.calculate_delay(1)  # ~2.0s
            >>> policy.calculate_delay(2)  # ~4.0s
            >>> policy.calculate_delay(3)  # ~8.0s
        """
        # Calculate base delay using exponential backoff
        delay = self.initial_delay * (self.exponential_base ** retry_count)
        
        # Cap at max_delay
        delay = min(delay, self.max_delay)
        
        # Add jitter: random(0.5, 1.5) * delay
        if self.jitter:
            jitter_factor = 0.5 + random.random()  # Random between 0.5 and 1.5
            delay *= jitter_factor
        
        return delay
    
    def should_retry(self, retry_count: int, error: Optional[Exception] = None) -> bool:
        """
        Determine if job should be retried.
        
        Checks if retries are remaining and if the error type (if provided)
        is in the list of retryable exceptions.
        
        Args:
            retry_count: The current retry attempt number.
            error: The exception that caused the failure (optional).
            
        Returns:
            True if the job should be retried, False otherwise.
        """
        # Check retry count
        if retry_count >= self.max_retries:
            return False
        
        # Check error type if provided
        if error is not None:
            return isinstance(error, self.retry_on)
        
        return True
    
    def to_dict(self) -> dict:
        """Serialize retry policy to dictionary."""
        return {
            "max_retries": self.max_retries,
            "initial_delay": self.initial_delay,
            "max_delay": self.max_delay,
            "exponential_base": self.exponential_base,
            "jitter": self.jitter,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "RetryPolicy":
        """Deserialize retry policy from dictionary."""
        return cls(
            max_retries=data.get("max_retries", 3),
            initial_delay=data.get("initial_delay", 1.0),
            max_delay=data.get("max_delay", 300.0),
            exponential_base=data.get("exponential_base", 2.0),
            jitter=data.get("jitter", True),
        )


class RetryHandler:
    """
    Handles job retry logic with exponential backoff.
    
    This class manages the retry process for failed jobs, calculating
    delays using exponential backoff and determining when jobs should
    be moved to the dead letter queue.
    
    Attributes:
        _default_policy: The default retry policy used when jobs don't specify one.
        
    Example:
        >>> handler = RetryHandler()
        >>> 
        >>> # Schedule a retry for a failed job
        >>> if handler.schedule_retry(job, scheduler):
        ...     print(f"Job scheduled for retry in {handler.get_retry_delay(job):.1f}s")
        ... else:
        ...     print("Job exhausted retries, moving to DLQ")
    """
    
    def __init__(self, default_policy: Optional[RetryPolicy] = None):
        """
        Initialize the retry handler.
        
        Args:
            default_policy: The default retry policy to use when jobs
                don't specify their own. If None, uses the default RetryPolicy.
        """
        self._default_policy = default_policy or RetryPolicy()
    
    @property
    def default_policy(self) -> RetryPolicy:
        """Get the default retry policy."""
        return self._default_policy
    
    def get_policy_for_job(self, job: Job) -> RetryPolicy:
        """
        Get the retry policy for a specific job.
        
        Returns the job's custom retry policy if available,
        otherwise returns the default policy.
        
        Args:
            job: The job to get the policy for.
            
        Returns:
            The applicable RetryPolicy for this job.
        """
        # Check if job has a custom retry policy
        if hasattr(job, 'retry_policy') and job.retry_policy is not None:
            # Convert from core RetryPolicy to scheduler RetryPolicy if needed
            core_policy = job.retry_policy
            return RetryPolicy(
                max_retries=core_policy.max_retries,
                initial_delay=core_policy.base_delay,
                max_delay=core_policy.max_delay,
                exponential_base=core_policy.exponential_base,
                jitter=core_policy.jitter,
                retry_on=core_policy.retry_on,
            )
        return self._default_policy
    
    def get_retry_delay(self, job: Job) -> float:
        """
        Calculate delay before next retry for a job.
        
        Uses the job's retry policy (or default) to calculate
        the delay based on the current retry count.
        
        Args:
            job: The job to calculate retry delay for.
            
        Returns:
            The delay in seconds before the job should be retried.
        """
        policy = self.get_policy_for_job(job)
        return policy.calculate_delay(job.attempt)
    
    def should_retry(self, job: Job, error: Optional[Exception] = None) -> bool:
        """
        Determine if a job should be retried.
        
        Checks the job's retry policy and current attempt count
        to determine if another retry attempt should be made.
        
        Args:
            job: The job to check.
            error: The exception that caused the failure (optional).
            
        Returns:
            True if the job should be retried, False if it should go to DLQ.
        """
        policy = self.get_policy_for_job(job)
        return policy.should_retry(job.attempt, error)
    
    def schedule_retry(
        self,
        job: Job,
        scheduler: "Scheduler",
        error: Optional[Exception] = None
    ) -> bool:
        """
        Schedule a job for retry.
        
        If the job has retries remaining and the error type is retryable,
        schedules the job for a delayed retry. Otherwise, the job should
        be moved to the dead letter queue.
        
        Args:
            job: The job to retry.
            scheduler: The scheduler to submit the retry to.
            error: The exception that caused the failure (optional).
            
        Returns:
            True if the job was scheduled for retry, False if max retries
            exceeded (should go to DLQ).
        """
        policy = self.get_policy_for_job(job)
        
        # Check if we should retry
        if not policy.should_retry(job.attempt, error):
            logger.info(
                f"Job {job.id} exhausted retries ({job.attempt}/{policy.max_retries})"
            )
            return False  # Should go to DLQ
        
        # Calculate delay
        delay = policy.calculate_delay(job.attempt)
        
        # Update job for retry
        job.attempt += 1
        job.scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
        job.state = JobState.RETRYING
        
        # Clear previous error for new attempt
        job.error = None
        job.traceback = None
        
        logger.info(
            f"Job {job.id} scheduled for retry {job.attempt}/{policy.max_retries} "
            f"in {delay:.1f}s"
        )
        
        # Re-queue with delay
        scheduler.submit(job)
        
        return True
    
    def prepare_for_retry(self, job: Job) -> Tuple[float, datetime]:
        """
        Prepare a job for retry without submitting it.
        
        Updates the job's retry counter and calculates the retry delay.
        Useful when the caller wants to handle submission separately.
        
        Args:
            job: The job to prepare for retry.
            
        Returns:
            Tuple of (delay_seconds, scheduled_at_datetime).
        """
        policy = self.get_policy_for_job(job)
        
        # Calculate delay
        delay = policy.calculate_delay(job.attempt)
        scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
        
        # Update job
        job.attempt += 1
        job.scheduled_at = scheduled_at
        job.state = JobState.RETRYING
        
        return delay, scheduled_at
    
    def get_remaining_retries(self, job: Job) -> int:
        """
        Get the number of remaining retry attempts for a job.
        
        Args:
            job: The job to check.
            
        Returns:
            The number of remaining retry attempts.
        """
        policy = self.get_policy_for_job(job)
        return max(0, policy.max_retries - job.attempt)
    
    def create_retry_policy(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 300.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    ) -> RetryPolicy:
        """
        Factory method to create custom retry policies.
        
        Convenience method for creating RetryPolicy instances with
        customized parameters.
        
        Args:
            max_retries: Maximum number of retry attempts.
            initial_delay: Initial delay in seconds.
            max_delay: Maximum delay in seconds.
            exponential_base: Exponential backoff multiplier.
            jitter: Whether to add jitter to delays.
            retry_on: Tuple of exception types to retry on.
            
        Returns:
            A new RetryPolicy with the specified parameters.
        """
        return RetryPolicy(
            max_retries=max_retries,
            initial_delay=initial_delay,
            max_delay=max_delay,
            exponential_base=exponential_base,
            jitter=jitter,
            retry_on=retry_on or (Exception,),
        )


# Pre-defined retry policies for common use cases
AGGRESSIVE_RETRY = RetryPolicy(
    max_retries=10,
    initial_delay=0.5,
    max_delay=60.0,
    exponential_base=1.5,
    jitter=True,
)
"""Aggressive retry policy with many attempts and short delays."""

CONSERVATIVE_RETRY = RetryPolicy(
    max_retries=3,
    initial_delay=5.0,
    max_delay=600.0,
    exponential_base=3.0,
    jitter=True,
)
"""Conservative retry policy with fewer attempts and longer delays."""

NO_RETRY = RetryPolicy(
    max_retries=0,
    initial_delay=0.0,
    max_delay=0.0,
)
"""No retry policy - fail immediately on first error."""

LINEAR_BACKOFF = RetryPolicy(
    max_retries=5,
    initial_delay=10.0,
    max_delay=60.0,
    exponential_base=1.0,  # Linear backoff (no exponential growth)
    jitter=True,
)
"""Linear backoff policy with constant delay between retries."""


__all__ = [
    "RetryPolicy",
    "RetryHandler",
    "AGGRESSIVE_RETRY",
    "CONSERVATIVE_RETRY",
    "NO_RETRY",
    "LINEAR_BACKOFF",
]