"""
Workers module for the Job Orchestrator.

Provides worker pool management, auto-scaling, and job execution handling.

This module contains:
- Base worker classes and state management
- Thread-based worker for I/O-bound workloads
- Process-based worker for CPU-bound workloads
- Async worker for async/await patterns
- Worker pool with dynamic auto-scaling

Example:
    >>> from job_orchestrator.scheduler import Scheduler
    >>> from job_orchestrator.workers import WorkerPool, PoolConfig, WorkerType
    >>> 
    >>> scheduler = Scheduler()
    >>> scheduler.start()
    >>> 
    >>> config = PoolConfig(
    ...     min_workers=2,
    ...     max_workers=10,
    ...     worker_type=WorkerType.THREAD,
    ...     scale_up_threshold=0.8,
    ...     scale_down_threshold=0.2,
    ... )
    >>> 
    >>> pool = WorkerPool(scheduler=scheduler, config=config)
    >>> pool.start()
    >>> 
    >>> # Submit jobs to the scheduler...
    >>> # Workers will process them automatically
    >>> 
    >>> pool.stop(wait=True)
    >>> scheduler.stop()
"""

from .worker import (
    BaseWorker,
    WorkerState,
    WorkerType,
    WorkerInfo,
)
from .thread_worker import ThreadWorker
from .process_worker import ProcessWorker
from .async_worker import AsyncWorker
from .pool import (
    WorkerPool,
    PoolConfig,
    PoolStats,
)

__all__ = [
    # Base classes and enums
    "BaseWorker",
    "WorkerState",
    "WorkerType",
    "WorkerInfo",
    # Worker implementations
    "ThreadWorker",
    "ProcessWorker",
    "AsyncWorker",
    # Pool management
    "WorkerPool",
    "PoolConfig",
    "PoolStats",
]