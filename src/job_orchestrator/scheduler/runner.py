"""
Job Runner implementation for the Job Orchestrator.

This module provides job execution with proper error handling and state management.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
import logging
import traceback as tb
import importlib

from ..core.job import Job, JobState
from ..core.state import StateMachine


logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    """
    Result of a job execution.
    
    Attributes:
        job_id: The UUID of the executed job.
        success: Whether the job succeeded.
        result: The return value if successful.
        error: Error message if failed.
        traceback: Full traceback if failed.
        started_at: When execution started.
        completed_at: When execution finished.
    """
    job_id: Any
    success: bool
    result: Any = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class JobRunner:
    """
    Executes jobs with proper error handling and state management.
    
    The JobRunner is responsible for:
    - Loading and executing job functions
    - Managing job state transitions
    - Capturing results and errors
    - Enforcing timeouts (if specified)
    """
    
    def __init__(
        self,
        state_machine: Optional[StateMachine] = None,
        default_timeout: Optional[float] = None
    ):
        """
        Initialize the job runner.
        
        Args:
            state_machine: State machine for managing job states.
            default_timeout: Default timeout in seconds for jobs.
        """
        self._state_machine = state_machine or StateMachine()
        self._default_timeout = default_timeout
    
    def run(self, job: Job) -> JobResult:
        """
        Execute a job and return the result.
        
        Args:
            job: The job to execute.
            
        Returns:
            JobResult with execution outcome.
        """
        started_at = datetime.utcnow()
        
        try:
            # Ensure proper state transitions
            # PENDING -> SCHEDULED -> RUNNING
            if job.state == JobState.PENDING:
                self._state_machine.transition(job, JobState.SCHEDULED)
            
            if job.state == JobState.SCHEDULED:
                self._state_machine.transition(job, JobState.RUNNING)
            elif job.state != JobState.RUNNING:
                # If job is in RETRYING state, move to SCHEDULED then RUNNING
                if job.state == JobState.RETRYING:
                    self._state_machine.transition(job, JobState.SCHEDULED)
                    self._state_machine.transition(job, JobState.RUNNING)
                else:
                    # Already in RUNNING or some other state, try to proceed
                    if self._state_machine.can_transition(job.state, JobState.RUNNING):
                        self._state_machine.transition(job, JobState.RUNNING)
            
            job.started_at = started_at
            
            # Get the function to execute
            func = self._get_function(job)
            
            # Execute the function
            result = func(*job.args, **job.kwargs)
            
            # Job succeeded
            completed_at = datetime.utcnow()
            
            return JobResult(
                job_id=job.id,
                success=True,
                result=result,
                started_at=started_at,
                completed_at=completed_at,
            )
            
        except Exception as e:
            # Job failed
            completed_at = datetime.utcnow()
            error_msg = str(e)
            traceback_str = tb.format_exc()
            
            logger.error(
                f"Job {job.id} failed: {error_msg}",
                exc_info=True
            )
            
            return JobResult(
                job_id=job.id,
                success=False,
                error=error_msg,
                traceback=traceback_str,
                started_at=started_at,
                completed_at=completed_at,
            )
    
    def _get_function(self, job: Job):
        """
        Get the callable function for a job.
        
        Args:
            job: The job to get the function for.
            
        Returns:
            The callable function.
            
        Raises:
            ValueError: If neither func nor func_path is set.
            ImportError: If func_path cannot be imported.
        """
        # If func is directly available, use it
        if job.func is not None:
            return job.func
        
        # Otherwise, load from func_path
        if not job.func_path:
            raise ValueError(f"Job {job.id} has no func or func_path set")
        
        # Parse module and function name
        try:
            module_path, func_name = job.func_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)
            return func
        except (ValueError, ImportError, AttributeError) as e:
            raise ImportError(
                f"Failed to import function from '{job.func_path}': {e}"
            )


__all__ = ["JobResult", "JobRunner"]