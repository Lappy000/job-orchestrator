"""
DAG Executor implementation for the Job Orchestrator.

This module provides DAG workflow execution with dependency management.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set
from uuid import UUID
import logging
import threading

from ..core.job import Job, JobState
from ..core.dag import DAG

if TYPE_CHECKING:
    from .scheduler import Scheduler


logger = logging.getLogger(__name__)


class DAGStatus(Enum):
    """Status of DAG execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class DAGExecution:
    """
    Tracks the execution state of a DAG.
    
    Attributes:
        dag: The DAG being executed.
        status: Current execution status.
        started_at: When execution started.
        completed_at: When execution finished.
        completed_jobs: Set of completed job IDs.
        failed_jobs: Set of failed job IDs.
        running_jobs: Set of currently running job IDs.
        cancelled_jobs: Set of cancelled job IDs.
        error: Error message if DAG failed.
    """
    dag: DAG
    status: DAGStatus = DAGStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    completed_jobs: Set[UUID] = field(default_factory=set)
    failed_jobs: Set[UUID] = field(default_factory=set)
    running_jobs: Set[UUID] = field(default_factory=set)
    cancelled_jobs: Set[UUID] = field(default_factory=set)
    error: Optional[str] = None
    
    @property
    def total_jobs(self) -> int:
        """Total number of jobs in the DAG."""
        return len(self.dag.jobs)
    
    @property
    def progress(self) -> float:
        """Completion progress as a percentage (0.0 to 1.0)."""
        if self.total_jobs == 0:
            return 1.0
        return len(self.completed_jobs) / self.total_jobs


class DAGExecutor:
    """
    Executes DAG workflows with dependency management.
    
    The DAGExecutor is responsible for:
    - Managing DAG execution lifecycle
    - Tracking job dependencies
    - Queuing jobs when dependencies are met
    - Handling failures and cancellations
    """
    
    def __init__(
        self,
        scheduler: "Scheduler",
        on_dag_complete: Optional[Callable[[UUID, DAGStatus], None]] = None,
        on_dag_failed: Optional[Callable[[UUID, str], None]] = None,
    ):
        """
        Initialize the DAG executor.
        
        Args:
            scheduler: The scheduler to submit jobs to.
            on_dag_complete: Callback when a DAG completes.
            on_dag_failed: Callback when a DAG fails.
        """
        self._scheduler = scheduler
        self._active_dags: Dict[UUID, DAGExecution] = {}
        self._job_to_dag: Dict[UUID, UUID] = {}  # Map job ID to DAG ID
        self._lock = threading.RLock()
        self._on_dag_complete = on_dag_complete
        self._on_dag_failed = on_dag_failed
    
    def start_dag(self, dag: DAG) -> UUID:
        """
        Start executing a DAG.
        
        Args:
            dag: The DAG to execute.
            
        Returns:
            The DAG ID.
        """
        with self._lock:
            execution = DAGExecution(
                dag=dag,
                status=DAGStatus.RUNNING,
                started_at=datetime.utcnow(),
            )
            
            self._active_dags[dag.id] = execution
            
            # Map all jobs to this DAG
            for job_id in dag.jobs:
                self._job_to_dag[job_id] = dag.id
            
            # Queue root jobs (jobs with no dependencies)
            root_jobs = dag.get_root_nodes()
            for job_id in root_jobs:
                job = dag.jobs[job_id]
                self._queue_job(job, execution)
            
            logger.info(f"Started DAG {dag.id} with {len(root_jobs)} root jobs")
            
            return dag.id
    
    def _queue_job(self, job: Job, execution: DAGExecution) -> None:
        """Queue a job for execution."""
        try:
            self._scheduler.submit(job)
            execution.running_jobs.add(job.id)
            logger.debug(f"Queued job {job.id} for DAG {execution.dag.id}")
        except Exception as e:
            logger.error(f"Failed to queue job {job.id}: {e}")
            execution.failed_jobs.add(job.id)
    
    def on_job_complete(self, job_id: UUID) -> List[Job]:
        """
        Handle job completion and queue dependent jobs.
        
        Args:
            job_id: The ID of the completed job.
            
        Returns:
            List of newly queued jobs.
        """
        with self._lock:
            dag_id = self._job_to_dag.get(job_id)
            if not dag_id:
                return []
            
            execution = self._active_dags.get(dag_id)
            if not execution:
                return []
            
            # Mark job as completed
            execution.running_jobs.discard(job_id)
            execution.completed_jobs.add(job_id)
            
            logger.debug(
                f"Job {job_id} completed for DAG {dag_id} "
                f"({len(execution.completed_jobs)}/{execution.total_jobs})"
            )
            
            # Find and queue jobs that are now ready
            newly_queued = []
            dag = execution.dag
            node = dag.nodes.get(job_id)
            
            if node:
                for dependent_id in node.dependents:
                    if self._is_job_ready(dependent_id, execution):
                        job = dag.jobs[dependent_id]
                        self._queue_job(job, execution)
                        newly_queued.append(job)
            
            # Check if DAG is complete
            self._check_dag_completion(execution)
            
            return newly_queued
    
    def _is_job_ready(self, job_id: UUID, execution: DAGExecution) -> bool:
        """Check if a job's dependencies are all completed."""
        dag = execution.dag
        node = dag.nodes.get(job_id)
        
        if not node:
            return False
        
        # Check if already queued/running/completed/failed
        if job_id in execution.running_jobs:
            return False
        if job_id in execution.completed_jobs:
            return False
        if job_id in execution.failed_jobs:
            return False
        if job_id in execution.cancelled_jobs:
            return False
        
        # Check all dependencies are completed
        for dep_id in node.dependencies:
            if dep_id not in execution.completed_jobs:
                return False
        
        return True
    
    def on_job_failed(self, job_id: UUID, error: str) -> None:
        """
        Handle job failure.
        
        Args:
            job_id: The ID of the failed job.
            error: The error message.
        """
        with self._lock:
            dag_id = self._job_to_dag.get(job_id)
            if not dag_id:
                return
            
            execution = self._active_dags.get(dag_id)
            if not execution:
                return
            
            execution.running_jobs.discard(job_id)
            execution.failed_jobs.add(job_id)
            
            logger.error(f"Job {job_id} failed in DAG {dag_id}: {error}")
            
            # Check fail-fast mode
            if execution.dag.fail_fast:
                self._fail_dag(execution, f"Job {job_id} failed: {error}")
            else:
                # Continue with other jobs
                self._check_dag_completion(execution)
    
    def _fail_dag(self, execution: DAGExecution, error: str) -> None:
        """Mark a DAG as failed (idempotent — safe to call multiple times)."""
        if execution.status in (DAGStatus.FAILED, DAGStatus.CANCELLED):
            return  # Already in a terminal state
        
        execution.status = DAGStatus.FAILED
        execution.completed_at = datetime.utcnow()
        execution.error = error
        
        logger.error(f"DAG {execution.dag.id} failed: {error}")
        
        # Cancel pending jobs
        for job_id in execution.dag.jobs:
            if job_id not in execution.completed_jobs and job_id not in execution.failed_jobs:
                execution.cancelled_jobs.add(job_id)
        
        # Notify callback
        if self._on_dag_failed:
            try:
                self._on_dag_failed(execution.dag.id, error)
            except Exception as e:
                logger.error(f"Error in DAG failed callback: {e}")
    
    def _check_dag_completion(self, execution: DAGExecution) -> None:
        """Check if a DAG has completed."""
        # Check if there are any running jobs
        if execution.running_jobs:
            return
        
        # Check if all jobs are accounted for
        total_accounted = (
            len(execution.completed_jobs) +
            len(execution.failed_jobs) +
            len(execution.cancelled_jobs)
        )
        
        if total_accounted < execution.total_jobs:
            # There might be jobs waiting for dependencies
            # Try to queue any ready jobs
            dag = execution.dag
            queued_any = False
            for job_id in dag.jobs:
                if self._is_job_ready(job_id, execution):
                    job = dag.jobs[job_id]
                    self._queue_job(job, execution)
                    queued_any = True
            
            # If we queued something, we're not done yet
            if queued_any:
                return
        
        # DAG is complete
        if execution.failed_jobs:
            self._fail_dag(execution, f"{len(execution.failed_jobs)} job(s) failed")
        else:
            execution.status = DAGStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            
            logger.info(
                f"DAG {execution.dag.id} completed successfully "
                f"({len(execution.completed_jobs)} jobs)"
            )
            
            # Notify callback
            if self._on_dag_complete:
                try:
                    self._on_dag_complete(execution.dag.id, execution.status)
                except Exception as e:
                    logger.error(f"Error in DAG complete callback: {e}")
    
    def cancel_dag(self, dag_id: UUID) -> bool:
        """
        Cancel a DAG execution.
        
        Args:
            dag_id: The DAG ID to cancel.
            
        Returns:
            True if cancelled, False if not found.
        """
        with self._lock:
            execution = self._active_dags.get(dag_id)
            if not execution:
                return False
            
            execution.status = DAGStatus.CANCELLED
            execution.completed_at = datetime.utcnow()
            
            # Mark all non-completed jobs as cancelled
            for job_id in execution.dag.jobs:
                if job_id not in execution.completed_jobs and job_id not in execution.failed_jobs:
                    execution.cancelled_jobs.add(job_id)
                    # Try to cancel the job in scheduler
                    try:
                        self._scheduler.cancel_job(str(job_id))
                    except:
                        pass
            
            logger.info(f"Cancelled DAG {dag_id}")
            return True
    
    def get_status(self, dag_id: UUID) -> Optional[DAGExecution]:
        """
        Get the execution status of a DAG.
        
        Args:
            dag_id: The DAG ID.
            
        Returns:
            The DAGExecution if found, None otherwise.
        """
        with self._lock:
            return self._active_dags.get(dag_id)
    
    def get_dag_for_job(self, job_id: UUID) -> Optional[UUID]:
        """
        Get the DAG ID that a job belongs to.
        
        Args:
            job_id: The job ID.
            
        Returns:
            The DAG ID if found, None otherwise.
        """
        with self._lock:
            return self._job_to_dag.get(job_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get executor statistics.
        
        Returns:
            Dictionary with executor statistics.
        """
        with self._lock:
            return {
                "active_dags": len(self._active_dags),
                "total_jobs": len(self._job_to_dag),
            }
    
    def __repr__(self) -> str:
        return f"DAGExecutor(active_dags={len(self._active_dags)})"


__all__ = [
    "DAGStatus",
    "DAGExecution",
    "DAGExecutor",
]