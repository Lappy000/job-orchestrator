"""
Async Worker implementation for the Job Orchestrator.

This module provides a worker that runs async jobs in an asyncio event loop,
suitable for I/O-bound workloads using async/await patterns.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Optional
import asyncio
import importlib
import logging
import threading
import traceback

from .worker import BaseWorker, WorkerState, WorkerType

if TYPE_CHECKING:
    from ..scheduler.scheduler import Scheduler
    from ..core.job import Job


logger = logging.getLogger(__name__)


class AsyncWorker(BaseWorker):
    """
    Worker that runs async jobs in an asyncio event loop.
    
    AsyncWorker is suitable for async/await-based workloads such as:
    - Async HTTP requests (aiohttp, httpx)
    - Async database operations (asyncpg, motor)
    - WebSocket connections
    - Any coroutine-based I/O
    
    The worker runs its own event loop in a separate thread, allowing
    it to be used alongside synchronous code in the scheduler.
    
    Example:
        >>> scheduler = Scheduler()
        >>> worker = AsyncWorker(scheduler=scheduler)
        >>> worker.start()
        >>> # Worker now processing async jobs in background
        >>> worker.stop(wait=True)
    
    Note:
        Jobs executed by AsyncWorker must be coroutine functions (async def).
        If a regular function is provided, it will be wrapped and run
        in the event loop's executor.
    """
    
    worker_type = WorkerType.ASYNC
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        scheduler: Optional["Scheduler"] = None,
        poll_timeout: float = 1.0,
        max_concurrent_jobs: int = 10,
    ):
        """
        Initialize the async worker.
        
        Args:
            worker_id: Unique identifier for this worker.
            scheduler: The scheduler to fetch jobs from.
            poll_timeout: Timeout in seconds for polling the job queue.
            max_concurrent_jobs: Maximum number of concurrent async jobs.
        """
        super().__init__(worker_id=worker_id, scheduler=scheduler)
        
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._task: Optional[asyncio.Task] = None
        self._thread: Optional[threading.Thread] = None
        self._poll_timeout = poll_timeout
        self._max_concurrent_jobs = max_concurrent_jobs
        self._should_stop = False
        
        # Semaphore for limiting concurrent jobs
        self._semaphore: Optional[asyncio.Semaphore] = None
        
        # Track active async tasks
        self._active_tasks: set = set()
        
        logger.debug(f"AsyncWorker {self.worker_id} created")
    
    def start(self) -> None:
        """
        Start the async worker.
        
        Creates a new event loop in a separate thread and starts
        the async job processing loop.
        
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
        
        self._should_stop = False
        self._state = WorkerState.IDLE
        self._started_at = datetime.utcnow()
        self._update_heartbeat()
        
        # Start event loop in a separate thread
        self._thread = threading.Thread(
            target=self._run_event_loop,
            name=f"AsyncWorker-{self.worker_id}",
            daemon=True,
        )
        self._thread.start()
        
        logger.info(f"AsyncWorker {self.worker_id} started")
    
    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Stop the async worker gracefully.
        
        Signals the worker to stop, cancels pending tasks, and optionally
        waits for the thread to finish.
        
        Args:
            wait: If True, block until the thread terminates.
            timeout: Maximum time to wait for termination.
        """
        if self._state == WorkerState.STOPPED:
            logger.debug(f"Worker {self.worker_id} already stopped")
            return
        
        self._state = WorkerState.STOPPING
        self._should_stop = True
        
        # Cancel running tasks
        if self._loop is not None and self._loop.is_running():
            # Schedule task cancellation in the event loop
            asyncio.run_coroutine_threadsafe(
                self._cancel_all_tasks(),
                self._loop
            )
        
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
                logger.info(f"AsyncWorker {self.worker_id} stopped")
    
    def _run_event_loop(self) -> None:
        """
        Run the event loop in a thread.
        
        Creates a new event loop for this thread and runs the main
        async loop until stopped.
        """
        # Create a new event loop for this thread
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        # Create semaphore for limiting concurrency
        self._semaphore = asyncio.Semaphore(self._max_concurrent_jobs)
        
        try:
            self._loop.run_until_complete(self._run_loop_async())
        except Exception as e:
            logger.error(
                f"Error in async worker {self.worker_id} event loop: {e}",
                exc_info=True
            )
        finally:
            # Clean up the event loop
            try:
                # Cancel any remaining tasks
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                
                # Give tasks a chance to handle cancellation
                if pending:
                    self._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
                
                self._loop.close()
            except Exception:
                pass
            
            self._loop = None
            self._state = WorkerState.STOPPED
    
    def _run_loop(self) -> None:
        """
        Not used for AsyncWorker - uses _run_loop_async instead.
        """
        pass
    
    async def _run_loop_async(self) -> None:
        """
        Main async worker loop - fetch and execute async jobs.
        
        This coroutine:
        1. Polls the scheduler for available jobs
        2. Spawns async tasks for job execution
        3. Manages concurrency with a semaphore
        4. Continues until stopped
        """
        logger.debug(f"Async worker {self.worker_id} loop started")
        
        while not self._should_stop:
            self._update_heartbeat()
            
            # Check if we're being stopped
            if self._state == WorkerState.STOPPING:
                break
            
            try:
                # Try to get a job
                job = await self._get_next_job_async()
                
                if job is None:
                    continue
                
                # Start job execution task
                task = asyncio.create_task(
                    self._execute_job_with_semaphore(job)
                )
                self._active_tasks.add(task)
                task.add_done_callback(self._active_tasks.discard)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Unexpected error in async worker {self.worker_id}: {e}",
                    exc_info=True
                )
        
        # Wait for active tasks to complete
        if self._active_tasks:
            logger.debug(
                f"Waiting for {len(self._active_tasks)} active tasks "
                f"to complete..."
            )
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
        
        logger.debug(f"Async worker {self.worker_id} loop ended")
    
    async def _get_next_job_async(self) -> Optional["Job"]:
        """
        Get the next job from the scheduler asynchronously.
        
        Runs the blocking get_next_job call in an executor to avoid
        blocking the event loop.
        
        Returns:
            The next available job, or None if timeout expired.
        """
        loop = asyncio.get_event_loop()
        
        # Run blocking operation in executor
        job = await loop.run_in_executor(
            None,
            lambda: self._scheduler.get_next_job(timeout=self._poll_timeout)
        )
        
        return job
    
    async def _execute_job_with_semaphore(self, job: "Job") -> None:
        """
        Execute a job with semaphore-based concurrency limiting.
        
        Args:
            job: The job to execute.
        """
        async with self._semaphore:
            await self._execute_job_async(job)
    
    async def _execute_job_async(self, job: "Job") -> Any:
        """
        Execute an async job.
        
        Args:
            job: The job to execute.
            
        Returns:
            The result of the job execution.
        """
        from datetime import datetime
        
        self._state = WorkerState.BUSY
        self._current_job = job
        self._update_heartbeat()
        
        start_time = datetime.utcnow()
        
        try:
            # Resolve the function
            func = self._resolve_function(job)
            
            # Execute the job
            if asyncio.iscoroutinefunction(func):
                # Async function - await it directly
                if job.timeout:
                    result = await asyncio.wait_for(
                        func(*job.args, **job.kwargs),
                        timeout=job.timeout
                    )
                else:
                    result = await func(*job.args, **job.kwargs)
            else:
                # Sync function - run in executor
                loop = asyncio.get_event_loop()
                if job.timeout:
                    result = await asyncio.wait_for(
                        loop.run_in_executor(
                            None,
                            lambda: func(*job.args, **job.kwargs)
                        ),
                        timeout=job.timeout
                    )
                else:
                    result = await loop.run_in_executor(
                        None,
                        lambda: func(*job.args, **job.kwargs)
                    )
            
            # Update statistics
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            self._total_execution_time += execution_time
            self._jobs_completed += 1
            
            # Notify scheduler
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._scheduler.complete_job(str(job.id), result)
            )
            
            logger.debug(
                f"Async worker {self.worker_id} completed job {job.id} "
                f"in {execution_time:.2f}s"
            )
            
            return result
            
        except asyncio.TimeoutError:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            self._total_execution_time += execution_time
            self._jobs_failed += 1
            
            error = TimeoutError(
                f"Job {job.id} exceeded timeout of {job.timeout}s"
            )
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._scheduler.fail_job(str(job.id), error)
            )
            
            logger.debug(
                f"Async worker {self.worker_id} job {job.id} timed out "
                f"after {execution_time:.2f}s"
            )
            
        except asyncio.CancelledError:
            # Job was cancelled - don't update stats
            raise
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            self._total_execution_time += execution_time
            self._jobs_failed += 1
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._scheduler.fail_job(str(job.id), e)
            )
            
            logger.debug(
                f"Async worker {self.worker_id} failed job {job.id} "
                f"in {execution_time:.2f}s: {e}"
            )
            
        finally:
            self._state = WorkerState.IDLE
            self._current_job = None
            self._update_heartbeat()
    
    def _resolve_function(self, job: "Job") -> Callable:
        """
        Resolve the function to execute for a job.
        
        If the job has a func attribute, use it directly.
        Otherwise, dynamically import from func_path.
        
        Args:
            job: The job to get the function for.
            
        Returns:
            The callable to execute.
        """
        if job.func is not None:
            return job.func
        
        if not job.func_path:
            raise ValueError(f"Job {job.id} has no func or func_path")
        
        module_path, func_name = job.func_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, func_name)
    
    async def _cancel_all_tasks(self) -> None:
        """Cancel all active tasks."""
        for task in self._active_tasks:
            task.cancel()
        
        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
    
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
    
    @property
    def active_task_count(self) -> int:
        """
        Get the number of currently active async tasks.
        
        Returns:
            Number of tasks being executed.
        """
        return len(self._active_tasks)


__all__ = [
    "AsyncWorker",
]