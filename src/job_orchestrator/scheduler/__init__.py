"""
Scheduler module for the Job Orchestrator.

Provides job scheduling, dependency resolution, DAG execution coordination,
retry handling with exponential backoff, and dead letter queue management.

Components:
- Scheduler: Main job scheduler with DAG support
- JobStore: Thread-safe in-memory job storage
- DAGExecutor: Coordinates DAG-based job workflows
- JobRunner: Executes individual jobs with error handling
- RetryHandler: Manages job retry logic with exponential backoff
- DeadLetterQueue: Stores jobs that have exhausted all retries

Example:
    >>> from job_orchestrator.scheduler import Scheduler, RetryPolicy
    >>> from job_orchestrator.core import Job, OrchestratorConfig
    >>>
    >>> config = OrchestratorConfig()
    >>> scheduler = Scheduler(config)
    >>> scheduler.start()
    >>>
    >>> # Submit a job with custom retry policy
    >>> job = Job(name="my_job", func=lambda: "Hello!")
    >>> job_id = scheduler.submit(job)
    >>>
    >>> result = scheduler.run_job(job)
    >>> print(result.result)
    'Hello!'
    >>>
    >>> # Check DLQ for failed jobs
    >>> dlq_entries = scheduler.get_dlq_entries()
    >>>
    >>> scheduler.stop()
"""

from .scheduler import Scheduler
from .job_store import JobStore
from .dag_executor import DAGExecutor, DAGExecution, DAGStatus
from .runner import JobRunner, JobResult
from .retry import (
    RetryHandler,
    RetryPolicy,
    AGGRESSIVE_RETRY,
    CONSERVATIVE_RETRY,
    NO_RETRY,
    LINEAR_BACKOFF,
)
from .dlq import (
    DeadLetterQueue,
    DLQEntry,
    DLQEntryStatus,
    DLQStats,
)


__all__ = [
    # Main scheduler
    "Scheduler",
    # Job storage
    "JobStore",
    # DAG execution
    "DAGExecutor",
    "DAGExecution",
    "DAGStatus",
    # Job runner
    "JobRunner",
    "JobResult",
    # Retry handling
    "RetryHandler",
    "RetryPolicy",
    "AGGRESSIVE_RETRY",
    "CONSERVATIVE_RETRY",
    "NO_RETRY",
    "LINEAR_BACKOFF",
    # Dead letter queue
    "DeadLetterQueue",
    "DLQEntry",
    "DLQEntryStatus",
    "DLQStats",
]