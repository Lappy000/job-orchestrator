"""
Process-based Worker implementation for the Job Orchestrator.

This module provides a worker that runs jobs in a separate process,
suitable for CPU-bound workloads where true parallelism is needed.
"""

from datetime import datetime
from multiprocessing import Process, Queue, Event
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple
import importlib
import logging
import traceback

from .worker import BaseWorker, WorkerState, WorkerType

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event as EventType
    from ..scheduler.scheduler import Scheduler
    from ..core.job import Job


logger = logging.getLogger(__name__)


def _worker_process_target(
    worker_id: str,
    job_queue: "Queue[Any]",
    result_queue: "Queue[Any]",
    stop_event: "EventType",
    poll_timeout: float,
) -> None:
    """
    Target function for the worker process.
    
    This function runs in a separate process and:
    1. Receives job serialized data from the job_queue
    2. Executes the job
    3. Sends results back through the result_queue
    
    Args:
        worker_id: The ID of this worker for logging.
        job_queue: Queue to receive job data from.
        result_queue: Queue to send results to.
        stop_event: Event that signals when to stop.
        poll_timeout: Timeout for polling the job queue.
    """
    import pickle
    
    logger.debug(f"Worker process {worker_id} started")
    
    while not stop_event.is_set():
        try:
            # Poll for job with timeout
            try:
                job_data = job_queue.get(timeout=poll_timeout)
            except Exception:  # Queue.Empty
                continue
            
            if job_data is None:
                # Poison pill - signal to stop
                break
            
            job_id, func_path, args, kwargs, timeout = job_data
            
            # Resolve the function
            try:
                func = _resolve_function(func_path)
            except Exception as e:
                result_queue.put((
                    job_id,
                    False,
                    None,
                    str(e),
                    traceback.format_exc(),
                ))
                continue
            
            # Execute the job
            try:
                start_time = datetime.utcnow()
                result = func(*args, **kwargs)
                execution_time = (datetime.utcnow() - start_time).total_seconds()
                
                result_queue.put((
                    job_id,
                    True,
                    result,
                    None,
                    None,
                    execution_time,
                ))
                
            except Exception as e:
                execution_time = (datetime.utcnow() - start_time).total_seconds()
                result_queue.put((
                    job_id,
                    False,
                    None,
                    str(e),
                    traceback.format_exc(),
                    execution_time,
                ))
                
        except Exception as e:
            logger.error(f"Unexpected error in worker process {worker_id}: {e}")
    
    logger.debug(f"Worker process {worker_id} ended")


def _resolve_function(func_path: str) -> Any:
    """
    Dynamically import and resolve a function from its path.
    
    Args:
        func_path: Module path to the function (e.g., 'mymodule.tasks.process').
        
    Returns:
        The resolved callable function.
        
    Raises:
        ImportError: If the module cannot be imported.
        AttributeError: If the function doesn't exist in the module.
    """
    module_path, func_name = func_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


class ProcessWorker(BaseWorker):
    """
    Worker that runs jobs in a separate process.
    
    ProcessWorker is suitable for CPU-bound workloads such as:
    - Heavy computations
    - Data processing
    - Image/video processing
    
    Since each job runs in a separate process, the Python GIL does not
    limit parallelism, allowing true multi-core utilization.
    
    Note:
        Jobs executed by ProcessWorker must have serializable (picklable)
        arguments and return values. The function itself must be importable
        via its module path (func_path).
    
    Example:
        >>> scheduler = Scheduler()
        >>> worker = ProcessWorker(scheduler=scheduler)
        >>> worker.start()
        >>> # Worker now processing jobs in background process
        >>> worker.stop(wait=True)
    """
    
    worker_type = WorkerType.PROCESS
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        scheduler: Optional["Scheduler"] = None,
        poll_timeout: float = 1.0,
    ):
        """
        Initialize the process worker.
        
        Args:
            worker_id: Unique identifier for this worker.
            scheduler: The scheduler to fetch jobs from.
            poll_timeout: Timeout in seconds for polling the job queue.
        """
        super().__init__(worker_id=worker_id, scheduler=scheduler)
        
        self._process: Optional[Process] = None
        self._job_queue: Optional["Queue[Any]"] = None
        self._result_queue: Optional["Queue[Any]"] = None
        self._stop_event: Optional[Any] = None  # multiprocessing.Event instance
        self._poll_timeout = poll_timeout
        
        # Coordinator thread for managing process communication
        self._coordinator_thread: Optional[Any] = None
        self._coordinator_stop: Optional[Any] = None
        
        logger.debug(f"ProcessWorker {self.worker_id} created")
    
    def start(self) -> None:
        """
        Start the worker process.
        
        Creates multiprocessing queues and spawns a new process that
        runs the job execution loop. Also starts a coordinator thread
        that manages communication between the scheduler and the process.
        
        Raises:
            RuntimeError: If the worker is already running.
            ValueError: If no scheduler has been set.
        """
        import threading
        
        if self._process is not None and self._process.is_alive():
            raise RuntimeError(
                f"Worker {self.worker_id} is already running"
            )
        
        if self._scheduler is None:
            raise ValueError(
                f"Worker {self.worker_id} has no scheduler set"
            )
        
        # Create communication queues
        self._job_queue = Queue()
        self._result_queue = Queue()
        self._stop_event = Event()
        
        self._state = WorkerState.IDLE
        self._started_at = datetime.utcnow()
        self._update_heartbeat()
        
        # Start the worker process
        self._process = Process(
            target=_worker_process_target,
            args=(
                self.worker_id,
                self._job_queue,
                self._result_queue,
                self._stop_event,
                self._poll_timeout,
            ),
            name=f"Worker-{self.worker_id}",
            daemon=True,
        )
        self._process.start()
        
        # Start coordinator thread
        self._coordinator_stop = threading.Event()
        self._coordinator_thread = threading.Thread(
            target=self._coordinator_loop,
            name=f"WorkerCoordinator-{self.worker_id}",
            daemon=True,
        )
        self._coordinator_thread.start()
        
        logger.info(f"ProcessWorker {self.worker_id} started (PID: {self._process.pid})")
    
    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Stop the worker process gracefully.
        
        Signals the worker to stop, sends a poison pill to the job queue,
        and optionally waits for the process to terminate.
        
        Args:
            wait: If True, block until the process terminates.
            timeout: Maximum time to wait for termination.
        """
        if self._state == WorkerState.STOPPED:
            logger.debug(f"Worker {self.worker_id} already stopped")
            return
        
        self._state = WorkerState.STOPPING
        
        # Stop coordinator thread first
        if self._coordinator_stop:
            self._coordinator_stop.set()
        
        if self._coordinator_thread and self._coordinator_thread.is_alive():
            self._coordinator_thread.join(timeout=2.0)
        
        # Signal stop to process
        if self._stop_event:
            self._stop_event.set()
        
        # Send poison pill
        if self._job_queue:
            try:
                self._job_queue.put(None)
            except Exception:
                pass
        
        logger.debug(f"Stop signal sent to worker {self.worker_id}")
        
        if wait and self._process is not None:
            self._process.join(timeout=timeout)
            
            if self._process.is_alive():
                logger.warning(
                    f"Worker {self.worker_id} did not stop within timeout, "
                    f"terminating..."
                )
                self._process.terminate()
                self._process.join(timeout=1.0)
            
            self._state = WorkerState.STOPPED
            logger.info(f"ProcessWorker {self.worker_id} stopped")
        
        # Cleanup queues
        self._cleanup_queues()
    
    def _cleanup_queues(self) -> None:
        """Clean up multiprocessing queues."""
        try:
            if self._job_queue:
                self._job_queue.close()
                self._job_queue.join_thread()
        except Exception:
            pass
        
        try:
            if self._result_queue:
                self._result_queue.close()
                self._result_queue.join_thread()
        except Exception:
            pass
    
    def _coordinator_loop(self) -> None:
        """
        Coordinator loop that bridges the scheduler and worker process.
        
        This loop runs in a thread and:
        1. Fetches jobs from the scheduler
        2. Sends job data to the worker process
        3. Receives results from the process
        4. Updates job status in the scheduler
        """
        import threading
        from queue import Empty
        
        logger.debug(f"Coordinator for worker {self.worker_id} started")
        
        current_job: Optional["Job"] = None
        
        while not self._coordinator_stop.is_set():
            self._update_heartbeat()
            
            if self._state == WorkerState.STOPPING:
                break
            
            try:
                # Check for results from worker process
                try:
                    result_data = self._result_queue.get_nowait()
                    if result_data:
                        self._handle_result(result_data, current_job)
                        self._state = WorkerState.IDLE
                        current_job = None
                except Exception:  # Empty
                    pass
                
                # If idle, fetch new job
                if self._state == WorkerState.IDLE and current_job is None:
                    job = self._scheduler.get_next_job(timeout=0.1)
                    
                    if job is not None:
                        current_job = job
                        self._current_job = job
                        self._state = WorkerState.BUSY
                        
                        # Send job to worker process
                        job_data = (
                            str(job.id),
                            job.func_path,
                            job.args,
                            job.kwargs,
                            job.timeout,
                        )
                        self._job_queue.put(job_data)
                        
                        logger.debug(
                            f"Sent job {job.id} to worker process "
                            f"{self.worker_id}"
                        )
                else:
                    # Small sleep to prevent busy-waiting
                    self._coordinator_stop.wait(timeout=0.1)
                    
            except Exception as e:
                logger.error(
                    f"Error in coordinator {self.worker_id}: {e}",
                    exc_info=True
                )
        
        logger.debug(f"Coordinator for worker {self.worker_id} ended")
    
    def _handle_result(
        self,
        result_data: Tuple[Any, ...],
        job: Optional["Job"],
    ) -> None:
        """
        Handle a result received from the worker process.
        
        Args:
            result_data: Tuple containing result information.
            job: The job that was executed (for reference).
        """
        if len(result_data) == 5:
            job_id, success, result, error, tb = result_data
            execution_time = 0.0
        else:
            job_id, success, result, error, tb, execution_time = result_data
        
        self._total_execution_time += execution_time
        
        if success:
            self._jobs_completed += 1
            if job:
                self._scheduler.complete_job(str(job.id), result)
            logger.debug(
                f"Worker {self.worker_id} completed job {job_id} "
                f"in {execution_time:.2f}s"
            )
        else:
            self._jobs_failed += 1
            if job:
                # Create an exception to pass to fail_job
                exc = Exception(error)
                self._scheduler.fail_job(str(job.id), exc)
            logger.debug(
                f"Worker {self.worker_id} failed job {job_id} "
                f"in {execution_time:.2f}s: {error}"
            )
        
        self._current_job = None
    
    def _run_loop(self) -> None:
        """
        Not used for ProcessWorker - coordination is handled by _coordinator_loop.
        
        The actual job execution loop runs in a separate process via
        _worker_process_target.
        """
        pass
    
    @property
    def is_alive(self) -> bool:
        """
        Check if the worker process is alive.
        
        Returns:
            True if the process is running, False otherwise.
        """
        if self._process is None:
            return False
        return self._process.is_alive()
    
    @property
    def pid(self) -> Optional[int]:
        """
        Get the process ID of the worker.
        
        Returns:
            The PID if the process is running, None otherwise.
        """
        if self._process is None:
            return None
        return self._process.pid


__all__ = [
    "ProcessWorker",
]