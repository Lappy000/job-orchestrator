"""
Thread-based Worker implementation for the Job Orchestrator.

This module provides a worker that runs jobs in a separate thread,
suitable for I/O-bound workloads where the GIL is not a bottleneck.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional
import logging
import threading

from .worker import BaseWorker, WorkerState, WorkerType

if TYPE_CHECKING:
    from ..scheduler.scheduler import Scheduler


logger = logging.getLogger(__name__)


class ThreadWorker(BaseWorker):
    """
    Worker that runs jobs in a separate thread.
    
    ThreadWorker is suitable for I/O-bound workloads such as:
    - Network requests
    - Database queries
    - File operations
    
    For CPU-bound workloads, consider using ProcessWorker instead,
    as Python's GIL limits parallelism in threads.
    
    Example:
        >>> scheduler = Scheduler()
        >>> worker = ThreadWorker(scheduler=scheduler)
        >>> worker.start()
        >>> # Worker now processing jobs in background thread
        >>> worker.stop(wait=True)
    """
    
    worker_type = WorkerType.THREAD
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        scheduler: Optional["Scheduler"] = None,
        poll_timeout: float = 1.0,
        name: Optional[str] = None,
        daemon: bool = True,
    ):
        """
        Initialize the thread worker.
        
        Args:
            worker_id: Unique identifier for this worker.
            scheduler: The scheduler to fetch jobs from.
            poll_timeout: Timeout in seconds for polling the job queue.
            name: Human-readable name for the worker (aliases worker_id).
            daemon: Whether the worker thread should be a daemon.
        """
        # Support 'name' as alias for 'worker_id' for test compatibility
        if name is not None and worker_id is None:
            worker_id = name
        
        super().__init__(worker_id=worker_id, scheduler=scheduler)
        
        self.name = self.worker_id  # Alias for compatibility
        self.daemon = daemon
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._poll_timeout = poll_timeout
        
        logger.debug(f"ThreadWorker {self.worker_id} created")
    
    def start(self) -> None:
        """
        Start the worker thread.
        
        Creates a new daemon thread that runs the job processing loop.
        The worker will begin fetching and executing jobs from the scheduler.
        
        Raises:
            RuntimeError: If the worker is already running.
            ValueError: If no scheduler has been set.
        """
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError(
                f"Worker {self.worker_id} is already running"
            )
        
        if self._scheduler is None:
            raise ValueError(
                f"Worker {self.worker_id} has no scheduler set"
            )
        
        self._stop_event.clear()
        self._state = WorkerState.IDLE
        self._started_at = datetime.utcnow()
        self._update_heartbeat()
        
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"Worker-{self.worker_id}",
            daemon=self.daemon,
        )
        self._thread.start()
        
        logger.info(f"ThreadWorker {self.worker_id} started")
    
    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Stop the worker thread gracefully.
        
        Signals the worker to stop and optionally waits for the thread to finish.
        If the worker is currently executing a job, it will complete that job
        before stopping (unless the timeout is exceeded).
        
        Args:
            wait: If True, block until the thread terminates.
            timeout: Maximum time to wait for the thread to finish.
                    If None, waits indefinitely.
        """
        if self._state == WorkerState.STOPPED:
            logger.debug(f"Worker {self.worker_id} already stopped")
            return
        
        self._state = WorkerState.STOPPING
        self._stop_event.set()
        
        logger.debug(f"Stop signal sent to worker {self.worker_id}")
        
        if wait and self._thread is not None:
            self._thread.join(timeout=timeout)
            
            if self._thread.is_alive():
                logger.warning(
                    f"Worker {self.worker_id} did not stop within "
                    f"timeout ({timeout}s)"
                )
            else:
                self._state = WorkerState.STOPPED
                logger.info(f"ThreadWorker {self.worker_id} stopped")
    
    def _run_loop(self) -> None:
        """
        Main worker loop - fetch and execute jobs.
        
        This loop runs in a separate thread and:
        1. Polls the scheduler for available jobs
        2. Executes each job
        3. Updates heartbeat regularly
        4. Continues until stop signal is received
        """
        logger.debug(f"Worker {self.worker_id} loop started")
        
        while not self._stop_event.is_set():
            self._update_heartbeat()
            
            # Check if we're being stopped
            if self._state == WorkerState.STOPPING:
                break
            
            try:
                # Fetch next job from scheduler with timeout
                job = self._scheduler.get_next_job(timeout=self._poll_timeout)
                
                if job is None:
                    # No job available, continue polling
                    continue
                
                # Execute the job
                self._execute_job(job)
                
            except Exception as e:
                # Log unexpected errors but continue running
                logger.error(
                    f"Unexpected error in worker {self.worker_id}: {e}",
                    exc_info=True
                )
        
        self._state = WorkerState.STOPPED
        logger.debug(f"Worker {self.worker_id} loop ended")
    
    @property
    def is_alive(self) -> bool:
        """
        Check if the worker thread is alive.
        
        Returns:
            True if the thread is running, False otherwise.
        """
        if self._thread is None:
            return False
        return self._thread.is_alive()
    
    def join(self, timeout: Optional[float] = None) -> None:
        """
        Wait for the worker thread to terminate.
        
        Args:
            timeout: Maximum time to wait in seconds. If None, waits indefinitely.
        """
        if self._thread is not None:
            self._thread.join(timeout=timeout)


__all__ = [
    "ThreadWorker",
]