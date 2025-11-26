"""
Worker Pool Manager for the Job Orchestrator.

This module provides the WorkerPool class that manages a pool of workers
with dynamic scaling based on queue utilization and load.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type
import logging
import threading
import time
import uuid

from .worker import BaseWorker, WorkerState, WorkerType, WorkerInfo
from .thread_worker import ThreadWorker
from .process_worker import ProcessWorker
from .async_worker import AsyncWorker

if TYPE_CHECKING:
    from ..scheduler.scheduler import Scheduler
    from ..queue.priority_queue import ThreadSafePriorityQueue


logger = logging.getLogger(__name__)


@dataclass
class PoolConfig:
    """
    Configuration for the worker pool.
    
    Attributes:
        min_workers: Minimum number of workers to maintain.
        max_workers: Maximum number of workers allowed.
        worker_type: Type of workers to create (thread, process, async).
        scale_up_threshold: Queue utilization percentage to trigger scale up.
        scale_down_threshold: Queue utilization percentage to trigger scale down.
        health_check_interval: Seconds between health checks.
        worker_max_idle_time: Seconds before an idle worker is removed.
        scale_interval: Seconds between scaling decisions.
        worker_heartbeat_timeout: Seconds before a worker is considered dead.
    """
    min_workers: int = 2
    max_workers: int = 10
    worker_type: WorkerType = WorkerType.THREAD
    scale_up_threshold: float = 0.8
    scale_down_threshold: float = 0.2
    health_check_interval: float = 5.0
    worker_max_idle_time: float = 60.0
    scale_interval: float = 10.0
    worker_heartbeat_timeout: float = 30.0
    
    def validate(self) -> None:
        """
        Validate the configuration.
        
        Raises:
            ValueError: If configuration is invalid.
        """
        if self.min_workers < 0:
            raise ValueError("min_workers must be non-negative")
        if self.max_workers < self.min_workers:
            raise ValueError("max_workers must be >= min_workers")
        if not 0 <= self.scale_up_threshold <= 1:
            raise ValueError("scale_up_threshold must be between 0 and 1")
        if not 0 <= self.scale_down_threshold <= 1:
            raise ValueError("scale_down_threshold must be between 0 and 1")
        if self.scale_down_threshold >= self.scale_up_threshold:
            raise ValueError("scale_down_threshold must be < scale_up_threshold")


@dataclass
class PoolStats:
    """
    Worker pool statistics.
    
    Attributes:
        total_workers: Total number of workers in the pool.
        idle_workers: Number of workers currently idle.
        busy_workers: Number of workers currently executing jobs.
        stopping_workers: Number of workers currently stopping.
        jobs_completed: Total jobs completed by all workers.
        jobs_failed: Total jobs failed by all workers.
        avg_job_time: Average job execution time across all workers.
        queue_size: Current size of the job queue.
        queue_utilization: Current queue utilization ratio.
        uptime_seconds: Seconds since the pool was started.
    """
    total_workers: int = 0
    idle_workers: int = 0
    busy_workers: int = 0
    stopping_workers: int = 0
    jobs_completed: int = 0
    jobs_failed: int = 0
    avg_job_time: float = 0.0
    queue_size: int = 0
    queue_utilization: float = 0.0
    uptime_seconds: float = 0.0
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "total_workers": self.total_workers,
            "idle_workers": self.idle_workers,
            "busy_workers": self.busy_workers,
            "stopping_workers": self.stopping_workers,
            "jobs_completed": self.jobs_completed,
            "jobs_failed": self.jobs_failed,
            "avg_job_time": self.avg_job_time,
            "queue_size": self.queue_size,
            "queue_utilization": self.queue_utilization,
            "uptime_seconds": self.uptime_seconds,
        }


class WorkerPool:
    """
    Manages a pool of workers with dynamic scaling.
    
    The WorkerPool is responsible for:
    - Creating and managing workers based on configuration
    - Auto-scaling workers based on queue utilization
    - Health monitoring and dead worker replacement
    - Graceful shutdown with job completion
    
    Auto-scaling Logic:
    - Queue depth and worker utilization are monitored periodically
    - Scale up when utilization > scale_up_threshold
    - Scale down (remove idle workers) when utilization < scale_down_threshold
    - Never go below min_workers or above max_workers
    
    Example:
        >>> from job_orchestrator.scheduler import Scheduler
        >>> scheduler = Scheduler()
        >>> 
        >>> config = PoolConfig(
        ...     min_workers=2,
        ...     max_workers=10,
        ...     worker_type=WorkerType.THREAD,
        ... )
        >>> pool = WorkerPool(scheduler=scheduler, config=config)
        >>> pool.start()
        >>> 
        >>> # Pool is now running with auto-scaling
        >>> 
        >>> pool.stop(wait=True)
    """
    
    # Worker class mapping
    WORKER_CLASSES: Dict[WorkerType, Type[BaseWorker]] = {
        WorkerType.THREAD: ThreadWorker,
        WorkerType.PROCESS: ProcessWorker,
        WorkerType.ASYNC: AsyncWorker,
    }
    
    def __init__(
        self,
        scheduler: "Scheduler",
        config: Optional[PoolConfig] = None,
    ):
        """
        Initialize the worker pool.
        
        Args:
            scheduler: The scheduler to assign to workers.
            config: Pool configuration. If None, uses defaults.
        """
        self._scheduler = scheduler
        self._config = config or PoolConfig()
        self._config.validate()
        
        self._workers: Dict[str, BaseWorker] = {}
        self._lock = threading.RLock()
        self._running = False
        
        # Auto-scaler thread
        self._scaler_thread: Optional[threading.Thread] = None
        self._scaler_stop = threading.Event()
        
        # Health check thread
        self._health_thread: Optional[threading.Thread] = None
        self._health_stop = threading.Event()
        
        # Worker idle time tracking
        self._worker_idle_since: Dict[str, datetime] = {}
        
        # Statistics
        self._started_at: Optional[datetime] = None
        
        logger.debug(
            f"WorkerPool initialized with config: "
            f"min={self._config.min_workers}, max={self._config.max_workers}, "
            f"type={self._config.worker_type.value}"
        )
    
    def start(self) -> None:
        """
        Start the pool with minimum workers.
        
        Spawns the minimum number of workers and starts the auto-scaler
        and health check background threads.
        """
        with self._lock:
            if self._running:
                logger.warning("WorkerPool is already running")
                return
            
            self._running = True
            self._started_at = datetime.utcnow()
            
            # Spawn minimum workers
            for _ in range(self._config.min_workers):
                self._spawn_worker()
            
            # Start auto-scaler thread
            self._scaler_stop.clear()
            self._scaler_thread = threading.Thread(
                target=self._auto_scale_loop,
                name="WorkerPool-AutoScaler",
                daemon=True,
            )
            self._scaler_thread.start()
            
            # Start health check thread
            self._health_stop.clear()
            self._health_thread = threading.Thread(
                target=self._health_check_loop,
                name="WorkerPool-HealthCheck",
                daemon=True,
            )
            self._health_thread.start()
            
            logger.info(
                f"WorkerPool started with {len(self._workers)} workers"
            )
    
    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Stop all workers gracefully.
        
        Signals all workers to stop and optionally waits for them
        to finish their current jobs.
        
        Args:
            wait: If True, wait for workers to finish current jobs.
            timeout: Maximum time to wait for shutdown.
        """
        with self._lock:
            if not self._running:
                logger.warning("WorkerPool is not running")
                return
            
            self._running = False
        
        # Stop background threads
        self._scaler_stop.set()
        self._health_stop.set()
        
        if self._scaler_thread and self._scaler_thread.is_alive():
            self._scaler_thread.join(timeout=2.0)
        
        if self._health_thread and self._health_thread.is_alive():
            self._health_thread.join(timeout=2.0)
        
        # Calculate per-worker timeout
        worker_timeout = None
        if timeout:
            worker_count = len(self._workers)
            worker_timeout = timeout / max(worker_count, 1)
        
        # Stop all workers
        with self._lock:
            for worker in list(self._workers.values()):
                worker.stop(wait=wait, timeout=worker_timeout)
        
        logger.info("WorkerPool stopped")
    
    def scale_up(self, count: int = 1) -> int:
        """
        Add workers to the pool.
        
        Args:
            count: Number of workers to add.
            
        Returns:
            Actual number of workers added (may be less if max reached).
        """
        added = 0
        
        with self._lock:
            for _ in range(count):
                if len(self._workers) >= self._config.max_workers:
                    break
                self._spawn_worker()
                added += 1
        
        if added > 0:
            logger.info(f"Scaled up by {added} workers")
        
        return added
    
    def scale_down(self, count: int = 1) -> int:
        """
        Remove idle workers from the pool.
        
        Preferentially removes workers that have been idle longest.
        Will not reduce below min_workers.
        
        Args:
            count: Number of workers to remove.
            
        Returns:
            Actual number of workers removed.
        """
        removed = 0
        
        with self._lock:
            # Get idle workers sorted by idle time
            idle_workers = [
                (worker_id, self._worker_idle_since[worker_id])
                for worker_id, worker in self._workers.items()
                if worker.state == WorkerState.IDLE
                and worker_id in self._worker_idle_since
            ]
            idle_workers.sort(key=lambda x: x[1])  # Longest idle first
            
            for worker_id, _ in idle_workers[:count]:
                if len(self._workers) <= self._config.min_workers:
                    break
                
                self._remove_worker(worker_id)
                removed += 1
        
        if removed > 0:
            logger.info(f"Scaled down by {removed} workers")
        
        return removed
    
    def _spawn_worker(self) -> BaseWorker:
        """
        Create and start a new worker.
        
        Returns:
            The newly created worker.
        """
        worker_class = self.WORKER_CLASSES[self._config.worker_type]
        worker_id = str(uuid.uuid4())
        
        worker = worker_class(
            worker_id=worker_id,
            scheduler=self._scheduler,
        )
        
        self._workers[worker_id] = worker
        self._worker_idle_since[worker_id] = datetime.utcnow()
        
        worker.start()
        
        logger.debug(f"Spawned new worker: {worker_id}")
        
        return worker
    
    def _remove_worker(self, worker_id: str) -> None:
        """
        Remove a worker from the pool.
        
        Args:
            worker_id: ID of the worker to remove.
        """
        if worker_id not in self._workers:
            return
        
        worker = self._workers[worker_id]
        
        # Stop the worker (don't wait - it will finish current job)
        worker.stop(wait=False)
        
        # Remove from tracking
        del self._workers[worker_id]
        self._worker_idle_since.pop(worker_id, None)
        
        logger.debug(f"Removed worker: {worker_id}")
    
    def _auto_scale_loop(self) -> None:
        """
        Auto-scaling loop that adjusts pool size based on load.
        
        Runs periodically and:
        1. Calculates current utilization
        2. Scales up if above threshold
        3. Scales down if below threshold
        """
        logger.debug("Auto-scaler started")
        
        while not self._scaler_stop.is_set():
            try:
                self._scaler_stop.wait(timeout=self._config.scale_interval)
                
                if self._scaler_stop.is_set():
                    break
                
                self._perform_scaling_decision()
                
            except Exception as e:
                logger.error(f"Error in auto-scaler: {e}", exc_info=True)
        
        logger.debug("Auto-scaler stopped")
    
    def _perform_scaling_decision(self) -> None:
        """
        Perform a single scaling decision based on current metrics.
        """
        with self._lock:
            current_workers = len([
                w for w in self._workers.values()
                if w.state != WorkerState.STOPPED
            ])
            
            busy_workers = len([
                w for w in self._workers.values()
                if w.state == WorkerState.BUSY
            ])
            
            # Calculate utilization
            utilization = busy_workers / max(current_workers, 1)
            
            # Get queue size for additional scaling signal
            try:
                queue_size = len(self._scheduler._queue)
            except Exception:
                queue_size = 0
        
        # Scale up decision
        if (utilization >= self._config.scale_up_threshold or 
            queue_size > current_workers * 2):
            if current_workers < self._config.max_workers:
                # Scale up by 1 worker (or more for high queue depth)
                to_add = min(
                    1 + queue_size // 10,  # Add more for higher queue
                    self._config.max_workers - current_workers
                )
                self.scale_up(to_add)
                logger.debug(
                    f"Scaling up: utilization={utilization:.2f}, "
                    f"queue={queue_size}, adding {to_add} workers"
                )
        
        # Scale down decision
        elif utilization <= self._config.scale_down_threshold and queue_size == 0:
            if current_workers > self._config.min_workers:
                self.scale_down(1)
                logger.debug(
                    f"Scaling down: utilization={utilization:.2f}, "
                    f"queue={queue_size}"
                )
    
    def _health_check_loop(self) -> None:
        """
        Health check loop that monitors worker health.
        
        Runs periodically and:
        1. Checks worker heartbeats
        2. Restarts dead workers
        3. Updates idle time tracking
        """
        logger.debug("Health checker started")
        
        while not self._health_stop.is_set():
            try:
                self._health_stop.wait(timeout=self._config.health_check_interval)
                
                if self._health_stop.is_set():
                    break
                
                self._perform_health_check()
                
            except Exception as e:
                logger.error(f"Error in health checker: {e}", exc_info=True)
        
        logger.debug("Health checker stopped")
    
    def _perform_health_check(self) -> None:
        """
        Perform a single health check on all workers.
        """
        now = datetime.utcnow()
        workers_to_restart: List[str] = []
        workers_to_remove: List[str] = []
        
        with self._lock:
            for worker_id, worker in list(self._workers.items()):
                # Check if worker is alive
                if not worker.is_alive and worker.state != WorkerState.STOPPED:
                    workers_to_restart.append(worker_id)
                    continue
                
                # Check heartbeat timeout
                info = worker.get_info()
                if info.last_heartbeat:
                    heartbeat_age = (now - info.last_heartbeat).total_seconds()
                    if heartbeat_age > self._config.worker_heartbeat_timeout:
                        if worker.state == WorkerState.BUSY:
                            # Worker is stuck - needs restart
                            logger.warning(
                                f"Worker {worker_id} heartbeat timeout "
                                f"({heartbeat_age:.1f}s), restarting"
                            )
                            workers_to_restart.append(worker_id)
                
                # Update idle tracking
                if worker.state == WorkerState.IDLE:
                    if worker_id not in self._worker_idle_since:
                        self._worker_idle_since[worker_id] = now
                elif worker_id in self._worker_idle_since:
                    del self._worker_idle_since[worker_id]
                
                # Check for long idle workers (beyond max_idle_time)
                if worker_id in self._worker_idle_since:
                    idle_time = (now - self._worker_idle_since[worker_id]).total_seconds()
                    if (idle_time > self._config.worker_max_idle_time and
                        len(self._workers) > self._config.min_workers):
                        workers_to_remove.append(worker_id)
        
        # Restart dead workers
        for worker_id in workers_to_restart:
            self._restart_worker(worker_id)
        
        # Remove long-idle workers (only if above min)
        for worker_id in workers_to_remove:
            with self._lock:
                if len(self._workers) > self._config.min_workers:
                    self._remove_worker(worker_id)
    
    def _restart_worker(self, worker_id: str) -> None:
        """
        Restart a dead or stuck worker.
        
        Args:
            worker_id: ID of the worker to restart.
        """
        with self._lock:
            if worker_id in self._workers:
                old_worker = self._workers[worker_id]
                
                # Try to stop gracefully first
                try:
                    old_worker.stop(wait=False)
                except Exception:
                    pass
                
                # Remove old worker
                del self._workers[worker_id]
                self._worker_idle_since.pop(worker_id, None)
        
        # Spawn a replacement
        self._spawn_worker()
        
        logger.info(f"Restarted worker {worker_id}")
    
    def get_stats(self) -> PoolStats:
        """
        Get pool statistics.
        
        Returns:
            PoolStats with current pool state.
        """
        with self._lock:
            workers = list(self._workers.values())
            
            total = len(workers)
            idle = len([w for w in workers if w.state == WorkerState.IDLE])
            busy = len([w for w in workers if w.state == WorkerState.BUSY])
            stopping = len([w for w in workers if w.state == WorkerState.STOPPING])
            
            # Aggregate worker statistics
            jobs_completed = sum(w.get_info().jobs_completed for w in workers)
            jobs_failed = sum(w.get_info().jobs_failed for w in workers)
            total_exec_time = sum(w.get_info().total_execution_time for w in workers)
            
            # Calculate average job time
            total_jobs = jobs_completed + jobs_failed
            avg_job_time = total_exec_time / total_jobs if total_jobs > 0 else 0.0
            
            # Queue information
            try:
                queue_size = len(self._scheduler._queue)
            except Exception:
                queue_size = 0
            
            # Calculate utilization
            utilization = busy / max(total, 1)
            
            # Uptime
            uptime = 0.0
            if self._started_at:
                uptime = (datetime.utcnow() - self._started_at).total_seconds()
            
            return PoolStats(
                total_workers=total,
                idle_workers=idle,
                busy_workers=busy,
                stopping_workers=stopping,
                jobs_completed=jobs_completed,
                jobs_failed=jobs_failed,
                avg_job_time=avg_job_time,
                queue_size=queue_size,
                queue_utilization=utilization,
                uptime_seconds=uptime,
            )
    
    def get_worker_info(self) -> List[WorkerInfo]:
        """
        Get information about all workers in the pool.
        
        Returns:
            List of WorkerInfo for each worker.
        """
        with self._lock:
            return [worker.get_info() for worker in self._workers.values()]
    
    def get_worker(self, worker_id: str) -> Optional[BaseWorker]:
        """
        Get a specific worker by ID.
        
        Args:
            worker_id: The ID of the worker to get.
            
        Returns:
            The worker if found, None otherwise.
        """
        with self._lock:
            return self._workers.get(worker_id)
    
    @property
    def is_running(self) -> bool:
        """Check if the pool is running."""
        return self._running
    
    @property
    def worker_count(self) -> int:
        """Get the current number of workers."""
        with self._lock:
            return len(self._workers)
    
    @property
    def config(self) -> PoolConfig:
        """Get the pool configuration."""
        return self._config
    
    def __repr__(self) -> str:
        return (
            f"WorkerPool("
            f"workers={self.worker_count}, "
            f"running={self._running}, "
            f"type={self._config.worker_type.value})"
        )


__all__ = [
    "PoolConfig",
    "PoolStats",
    "WorkerPool",
]