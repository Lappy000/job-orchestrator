"""
Main scheduler implementation for the Job Orchestrator.

This module provides the Scheduler class that orchestrates job execution,
managing the queue, workers, DAG execution, retry handling, and dead letter queue.
"""

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import UUID
import logging
import threading
import time
import traceback

from ..core.job import Job, JobState, JobPriority
from ..core.dag import DAG
from ..core.state import StateMachine
from ..core.config import OrchestratorConfig
from ..core.exceptions import (
    JobNotFoundError,
    JobAlreadyExistsError,
    QueueFullError,
)
from ..queue.priority_queue import ThreadSafePriorityQueue

from .job_store import JobStore
from .dag_executor import DAGExecutor, DAGStatus, DAGExecution
from .runner import JobRunner, JobResult
from .retry import RetryHandler, RetryPolicy
from .dlq import DeadLetterQueue, DLQEntry, DLQEntryStatus, DLQStats


logger = logging.getLogger(__name__)


class Scheduler:
    """
    Main job scheduler with DAG support.
    
    The Scheduler is the central coordinator for job execution, managing:
    - Job submission and queuing
    - DAG-based workflow execution
    - Job state transitions
    - Result handling and error recovery
    
    Example:
        >>> config = OrchestratorConfig()
        >>> scheduler = Scheduler(config)
        >>> scheduler.start()
        >>> 
        >>> # Submit a single job
        >>> job = Job(name="my_job", func=my_function)
        >>> job_id = scheduler.submit(job)
        >>> 
        >>> # Submit a DAG
        >>> dag_id = scheduler.submit_dag(my_dag)
        >>> 
        >>> # Get status
        >>> status = scheduler.get_job_status(job_id)
        >>> 
        >>> scheduler.stop()
    """
    
    def __init__(self, config: Optional[OrchestratorConfig] = None):
        """
        Initialize the scheduler.
        
        Args:
            config: Configuration for the scheduler. If None, uses defaults.
        """
        self._config = config or OrchestratorConfig()
        self._lock = threading.RLock()
        
        # Core components
        self._queue = ThreadSafePriorityQueue(
            max_size=self._config.queue.max_size
        )
        self._state_machine = StateMachine()
        self._job_store = JobStore()
        self._runner = JobRunner(
            self._state_machine,
            default_timeout=self._config.job_timeout
        )
        self._dag_executor = DAGExecutor(
            self,
            on_dag_complete=self._on_dag_complete,
            on_dag_failed=self._on_dag_failed,
        )
        
        # Retry handler with default policy from config
        default_retry_policy = RetryPolicy(
            max_retries=self._config.retry.max_retries,
            initial_delay=self._config.retry.base_delay,
            max_delay=self._config.retry.max_delay,
            exponential_base=self._config.retry.exponential_base,
            jitter=self._config.retry.jitter,
        )
        self._retry_handler = RetryHandler(default_policy=default_retry_policy)
        
        # Dead letter queue
        self._dlq = DeadLetterQueue(
            max_size=self._config.dlq.max_size,
            ttl_days=self._config.dlq.auto_cleanup_days,
        )
        
        # Connect state machine to job store for persistence
        self._state_machine.set_job_store(self._job_store)
        
        # Scheduler state
        self._running = False
        self._shutdown_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None
        
        # Callbacks
        self._on_job_complete_callbacks: List[Callable[[Job, JobResult], None]] = []
        self._on_job_failed_callbacks: List[Callable[[Job, JobResult], None]] = []
        self._on_dlq_entry_callbacks: List[Callable[[DLQEntry], None]] = []
        
        # Statistics
        self._stats = {
            "jobs_submitted": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "jobs_retried": 0,
            "jobs_sent_to_dlq": 0,
            "dags_submitted": 0,
            "dags_completed": 0,
            "dags_failed": 0,
        }
    
    def submit(self, job: Job) -> str:
        """
        Submit a single job for execution.
        
        The job is added to the store, transitioned to SCHEDULED state,
        and pushed to the priority queue.
        
        Args:
            job: The job to submit.
            
        Returns:
            The job ID as a string.
            
        Raises:
            JobAlreadyExistsError: If a job with the same ID already exists.
            QueueFullError: If the queue is at maximum capacity.
        """
        with self._lock:
            # Check for duplicates
            if self._job_store.contains(job.id):
                raise JobAlreadyExistsError(job.id)
            
            # Add to store
            self._job_store.add(job)
            
            # Transition to SCHEDULED
            self._state_machine.transition(job, JobState.SCHEDULED)
            
            # Push to queue
            if not self._queue.push(job):
                self._job_store.delete(job.id)
                raise QueueFullError(
                    queue_size=len(self._queue),
                    max_size=self._config.queue.max_size or 0
                )
            
            self._stats["jobs_submitted"] += 1
            
            logger.info(f"Submitted job {job.id}: {job.name}")
            
            return str(job.id)
    
    def submit_dag(self, dag: DAG) -> str:
        """
        Submit a DAG of jobs for execution.
        
        All jobs in the DAG are added to the store, and jobs with
        no dependencies (root jobs) are queued for execution.
        
        Args:
            dag: The DAG to submit.
            
        Returns:
            The DAG ID as a string.
        """
        with self._lock:
            # Validate DAG
            dag.validate()
            
            # Set DAG state to SCHEDULED
            dag.state = JobState.SCHEDULED
            dag.started_at = datetime.utcnow()
            
            # Start DAG execution
            self._dag_executor.start_dag(dag)
            
            self._stats["dags_submitted"] += 1
            
            logger.info(
                f"Submitted DAG {dag.id}: {dag.name} "
                f"with {len(dag.jobs)} jobs"
            )
            
            return str(dag.id)
    
    def get_next_job(self, timeout: Optional[float] = None) -> Optional[Job]:
        """
        Get the next ready job from the queue.
        
        This method is typically called by workers to get jobs to execute.
        It blocks until a job is available or the timeout expires.
        
        Args:
            timeout: Maximum time to wait in seconds. None for indefinite.
            
        Returns:
            The next job, or None if timeout expired or scheduler stopped.
        """
        if not self._running:
            return None
        
        job = self._queue.pop(timeout=timeout)
        
        if job:
            logger.debug(f"Dequeued job {job.id}: {job.name}")
        
        return job
    
    def complete_job(self, job_id: str, result: Any = None) -> None:
        """
        Mark a job as completed and trigger dependents.
        
        Updates the job state to COMPLETED, stores the result,
        and notifies the DAG executor to queue dependent jobs.
        
        Args:
            job_id: The job ID (as string).
            result: The result of the job execution.
        """
        job_uuid = UUID(job_id)
        
        with self._lock:
            job = self._job_store.get(job_uuid)
            if not job:
                raise JobNotFoundError(job_uuid)
            
            # Update job
            job.result = result
            job.completed_at = datetime.utcnow()
            
            # Transition to COMPLETED
            self._state_machine.transition(job, JobState.COMPLETED)
            
            self._stats["jobs_completed"] += 1
            
            logger.info(f"Job {job_id} completed successfully")
            
            # Notify DAG executor
            dag_id = self._dag_executor.get_dag_for_job(job_uuid)
            if dag_id:
                newly_ready = self._dag_executor.on_job_complete(job_uuid)
                if newly_ready:
                    logger.debug(
                        f"Queued {len(newly_ready)} dependent jobs "
                        f"after completion of {job_id}"
                    )
    
    def fail_job(self, job_id: str, error: Exception) -> None:
        """
        Mark a job as failed, handle retry or move to DLQ.
        
        Uses the retry handler to determine if the job should be retried.
        If retries are exhausted, the job is moved to the dead letter queue.
        
        Args:
            job_id: The job ID (as string).
            error: The exception that caused the failure.
        """
        job_uuid = UUID(job_id)
        
        with self._lock:
            job = self._job_store.get(job_uuid)
            if not job:
                raise JobNotFoundError(job_uuid)
            
            error_str = str(error)
            traceback_str = traceback.format_exc()
            
            job.error = error_str
            job.traceback = traceback_str
            
            # Try to schedule retry using the retry handler
            if self._retry_handler.should_retry(job, error):
                # Prepare job for retry
                delay, scheduled_at = self._retry_handler.prepare_for_retry(job)
                
                # Transition to RETRYING then SCHEDULED
                self._state_machine.transition(job, JobState.RETRYING)
                self._state_machine.transition(job, JobState.SCHEDULED)
                
                # Re-queue with delay
                self._queue.push(job)
                
                self._stats["jobs_retried"] += 1
                
                logger.info(
                    f"Job {job_id} scheduled for retry {job.attempt}/{self._retry_handler.get_policy_for_job(job).max_retries} "
                    f"in {delay:.1f}s"
                )
                return
            
            # Max retries exceeded - move to DLQ
            self._state_machine.transition(job, JobState.FAILED)
            job.completed_at = datetime.utcnow()
            
            # Add to dead letter queue
            entry_id = self._dlq.add(job, error, traceback_str)
            
            self._stats["jobs_failed"] += 1
            self._stats["jobs_sent_to_dlq"] += 1
            
            logger.error(
                f"Job {job_id} failed permanently after {job.attempt} retries, "
                f"moved to DLQ (entry: {entry_id}): {error}"
            )
            
            # Notify callbacks
            for callback in self._on_job_failed_callbacks:
                try:
                    callback(job, JobResult(
                        job_id=job.id,
                        success=False,
                        error=error_str,
                        traceback=traceback_str,
                    ))
                except Exception as e:
                    logger.error(f"Error in job failed callback: {e}")
            
            # Notify DAG executor
            dag_id = self._dag_executor.get_dag_for_job(job_uuid)
            if dag_id:
                self._dag_executor.on_job_failed(job_uuid, error_str)
    
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a pending or scheduled job.
        
        Jobs that are already running cannot be cancelled.
        
        Args:
            job_id: The job ID (as string).
            
        Returns:
            True if the job was cancelled, False if it couldn't be cancelled.
        """
        job_uuid = UUID(job_id)
        
        with self._lock:
            job = self._job_store.get(job_uuid)
            if not job:
                raise JobNotFoundError(job_uuid)
            
            # Can only cancel jobs that haven't started running
            if job.state not in {JobState.PENDING, JobState.SCHEDULED, JobState.RETRYING}:
                logger.warning(
                    f"Cannot cancel job {job_id} in state {job.state.value}"
                )
                return False
            
            # Remove from queue
            self._queue.remove(job_uuid)
            
            # Transition to CANCELLED
            self._state_machine.transition(job, JobState.CANCELLED)
            job.completed_at = datetime.utcnow()
            
            logger.info(f"Job {job_id} cancelled")
            
            return True
    
    def get_job_status(self, job_id: str) -> JobState:
        """
        Get the current status of a job.
        
        Args:
            job_id: The job ID (as string).
            
        Returns:
            The current JobState.
            
        Raises:
            JobNotFoundError: If the job is not found.
        """
        job_uuid = UUID(job_id)
        job = self._job_store.get(job_uuid)
        
        if not job:
            raise JobNotFoundError(job_uuid)
        
        return job.state
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """
        Get a job by its ID.
        
        Args:
            job_id: The job ID (as string).
            
        Returns:
            The job if found, None otherwise.
        """
        return self._job_store.get(UUID(job_id))
    
    def get_dag_status(self, dag_id: str) -> Dict[str, Any]:
        """
        Get the status of all jobs in a DAG.
        
        Args:
            dag_id: The DAG ID (as string).
            
        Returns:
            Dictionary with DAG status including all job states.
        """
        dag_uuid = UUID(dag_id)
        execution = self._dag_executor.get_status(dag_uuid)
        
        if not execution:
            return {
                "dag_id": dag_id,
                "status": "not_found",
            }
        
        jobs_status: List[Dict] = []
        for job_id, job in execution.dag.jobs.items():
            jobs_status.append({
                "job_id": str(job_id),
                "name": job.name,
                "state": job.state.value,
            })
        
        return {
            "dag_id": dag_id,
            "name": execution.dag.name,
            "status": execution.status.value,
            "progress": execution.progress,
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
            "jobs": jobs_status,
            "completed": len(execution.completed_jobs),
            "failed": len(execution.failed_jobs),
            "running": len(execution.running_jobs),
            "cancelled": len(execution.cancelled_jobs),
            "total": execution.total_jobs,
            "error": execution.error,
        }
    
    def cancel_dag(self, dag_id: str) -> bool:
        """
        Cancel all pending jobs in a DAG.
        
        Args:
            dag_id: The DAG ID (as string).
            
        Returns:
            True if the DAG was cancelled, False otherwise.
        """
        return self._dag_executor.cancel_dag(UUID(dag_id))
    
    def run_job(self, job: Job) -> JobResult:
        """
        Execute a job synchronously and return the result.
        
        This method runs the job in the current thread and handles
        all state transitions and result/error handling.
        
        Args:
            job: The job to execute.
            
        Returns:
            JobResult with the execution outcome.
        """
        # Run the job
        result = self._runner.run(job)
        
        # Handle result
        if result.success:
            job.result = result.result
            job.completed_at = result.completed_at
            self._state_machine.transition(job, JobState.COMPLETED)
            self._stats["jobs_completed"] += 1
            
            # Notify DAG executor
            dag_id = self._dag_executor.get_dag_for_job(job.id)
            if dag_id:
                self._dag_executor.on_job_complete(job.id)
            
            # Call callbacks
            for callback in self._on_job_complete_callbacks:
                try:
                    callback(job, result)
                except Exception as e:
                    logger.error(f"Error in job complete callback: {e}")
        else:
            job.error = result.error
            job.traceback = result.traceback
            
            # Check for retry
            if job.can_retry:
                delay = job.retry_policy.calculate_delay(job.attempt)
                job.scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
                self._state_machine.transition(job, JobState.RETRYING)
                self._state_machine.transition(job, JobState.SCHEDULED)
                self._queue.push(job)
            else:
                self._state_machine.transition(job, JobState.FAILED)
                job.completed_at = result.completed_at
                self._stats["jobs_failed"] += 1
                
                # Notify DAG executor
                dag_id = self._dag_executor.get_dag_for_job(job.id)
                if dag_id:
                    self._dag_executor.on_job_failed(job.id, result.error or "Unknown error")
                
                # Call callbacks
                for callback in self._on_job_failed_callbacks:
                    try:
                        callback(job, result)
                    except Exception as e:
                        logger.error(f"Error in job failed callback: {e}")
        
        return result
    
    def start(self) -> None:
        """
        Start the scheduler.
        
        This starts the internal scheduler loop that processes jobs.
        """
        with self._lock:
            if self._running:
                logger.warning("Scheduler is already running")
                return
            
            self._running = True
            self._shutdown_event.clear()
            
            logger.info("Scheduler started")
    
    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Stop the scheduler gracefully.
        
        Args:
            wait: If True, wait for running jobs to complete.
            timeout: Maximum time to wait for shutdown.
        """
        with self._lock:
            if not self._running:
                logger.warning("Scheduler is not running")
                return
            
            self._running = False
            self._shutdown_event.set()
            
            # Signal queue shutdown to unblock waiting threads
            self._queue.shutdown()
        
        if wait:
            # Wait for scheduler thread if running
            if self._scheduler_thread and self._scheduler_thread.is_alive():
                self._scheduler_thread.join(timeout=timeout)
        
        # Shutdown DLQ cleanup thread
        self._dlq.shutdown()
        
        logger.info("Scheduler stopped")
    
    @property
    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._running
    
    def on_job_complete(self, callback: Callable[[Job, JobResult], None]) -> None:
        """
        Register a callback for job completion.
        
        Args:
            callback: Function to call when a job completes successfully.
        """
        self._on_job_complete_callbacks.append(callback)
    
    def on_job_failed(self, callback: Callable[[Job, JobResult], None]) -> None:
        """
        Register a callback for job failure.
        
        Args:
            callback: Function to call when a job fails.
        """
        self._on_job_failed_callbacks.append(callback)
    
    def _on_dag_complete(self, dag_id: UUID, status: DAGStatus) -> None:
        """Handle DAG completion callback from executor."""
        self._stats["dags_completed"] += 1
    
    def _on_dag_failed(self, dag_id: UUID, error: str) -> None:
        """Handle DAG failure callback from executor."""
        self._stats["dags_failed"] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get scheduler statistics.
        
        Returns:
            Dictionary with scheduler statistics.
        """
        with self._lock:
            return {
                **self._stats,
                "queue": self._queue.get_stats(),
                "job_store": self._job_store.get_stats(),
                "dag_executor": self._dag_executor.get_stats(),
                "dlq": self._dlq.get_stats().to_dict(),
                "is_running": self._running,
            }
    
    # ==================== Dead Letter Queue Operations ====================
    
    def get_dlq_entries(
        self,
        status: Optional[DLQEntryStatus] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[DLQEntry]:
        """
        Get entries from the dead letter queue.
        
        Args:
            status: Filter by entry status (None for all).
            limit: Maximum number of entries to return.
            offset: Number of entries to skip.
            
        Returns:
            List of DLQEntry objects.
        """
        return self._dlq.get_all(status=status, limit=limit, offset=offset)
    
    def get_dlq_entry(self, entry_id: str) -> Optional[DLQEntry]:
        """
        Get a specific DLQ entry by ID.
        
        Args:
            entry_id: The DLQ entry ID.
            
        Returns:
            The DLQEntry if found, None otherwise.
        """
        return self._dlq.get(entry_id)
    
    def requeue_dlq_entry(
        self,
        entry_id: str,
        reset_retry_count: bool = True,
        resolved_by: Optional[str] = None
    ) -> bool:
        """
        Requeue a job from the dead letter queue.
        
        Args:
            entry_id: The DLQ entry ID.
            reset_retry_count: If True, reset the retry count.
            resolved_by: Identifier of who requeued the entry.
            
        Returns:
            True if the job was requeued, False otherwise.
        """
        return self._dlq.requeue(
            entry_id=entry_id,
            scheduler=self,
            reset_retry_count=reset_retry_count,
            resolved_by=resolved_by
        )
    
    def discard_dlq_entry(
        self,
        entry_id: str,
        notes: str = "",
        resolved_by: Optional[str] = None
    ) -> bool:
        """
        Discard a DLQ entry (permanently give up on the job).
        
        Args:
            entry_id: The DLQ entry ID.
            notes: Notes about why it was discarded.
            resolved_by: Identifier of who discarded the entry.
            
        Returns:
            True if the entry was discarded, False otherwise.
        """
        return self._dlq.discard(
            entry_id=entry_id,
            notes=notes,
            resolved_by=resolved_by
        )
    
    def resolve_dlq_entry(
        self,
        entry_id: str,
        notes: str = "",
        resolved_by: Optional[str] = None
    ) -> bool:
        """
        Mark a DLQ entry as resolved (issue fixed externally).
        
        Args:
            entry_id: The DLQ entry ID.
            notes: Notes about how it was resolved.
            resolved_by: Identifier of who resolved the entry.
            
        Returns:
            True if the entry was resolved, False otherwise.
        """
        return self._dlq.resolve(
            entry_id=entry_id,
            notes=notes,
            resolved_by=resolved_by
        )
    
    def get_dlq_stats(self) -> DLQStats:
        """
        Get dead letter queue statistics.
        
        Returns:
            DLQStats with current DLQ state.
        """
        return self._dlq.get_stats()
    
    def get_dlq_analytics(self) -> Dict[str, Any]:
        """
        Get failure analytics from the dead letter queue.
        
        Returns:
            Dictionary with failure pattern analytics.
        """
        return self._dlq.get_failure_analytics()
    
    def on_dlq_entry_added(self, callback: Callable[[DLQEntry], None]) -> None:
        """
        Register a callback for when entries are added to the DLQ.
        
        Useful for alerting and monitoring systems.
        
        Args:
            callback: Function to call when an entry is added.
        """
        self._dlq.on_entry_added(callback)
    
    # ==================== Retry Handler Operations ====================
    
    @property
    def retry_handler(self) -> RetryHandler:
        """Get the retry handler instance."""
        return self._retry_handler
    
    @property
    def dlq(self) -> DeadLetterQueue:
        """Get the dead letter queue instance."""
        return self._dlq
    
    def list_jobs(
        self,
        state: Optional[JobState] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Job]:
        """
        List jobs with optional filtering.
        
        Args:
            state: Filter by job state (None for all).
            limit: Maximum number of jobs to return.
            offset: Number of jobs to skip.
            
        Returns:
            List of jobs matching the criteria.
        """
        if state:
            jobs = self._job_store.get_by_state(state)
        else:
            jobs = self._job_store.get_all()
        
        # Sort by created_at descending
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        
        return jobs[offset:offset + limit]
    
    def list_dags(
        self,
        status: Optional[DAGStatus] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[DAGExecution]:
        """
        List DAG executions with optional filtering.
        
        Args:
            status: Filter by DAG status (None for all).
            limit: Maximum number of DAGs to return.
            offset: Number of DAGs to skip.
            
        Returns:
            List of DAGExecution objects matching the criteria.
        """
        with self._lock:
            executions = list(self._dag_executor._active_dags.values())
            
            if status:
                executions = [e for e in executions if e.status == status]
            
            # Sort by started_at descending
            executions.sort(
                key=lambda e: e.started_at or datetime.min,
                reverse=True
            )
            
            return executions[offset:offset + limit]


__all__ = [
    "Scheduler",
    "RetryHandler",
    "RetryPolicy",
    "DeadLetterQueue",
    "DLQEntry",
    "DLQEntryStatus",
    "DLQStats",
]