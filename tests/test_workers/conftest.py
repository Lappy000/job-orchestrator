"""
Fixtures for worker tests.
"""

import pytest
from unittest.mock import Mock
from job_orchestrator import Job, JobState, Scheduler
from job_orchestrator.workers.thread_worker import ThreadWorker
from job_orchestrator.workers.worker import WorkerState


class TestWorkerAdapter:
    """Adapter to make workers work with test expectations."""
    
    def __init__(self, worker_id=None, scheduler=None):
        self._scheduler = scheduler or Scheduler()
        self._worker = ThreadWorker(worker_id=worker_id, scheduler=self._scheduler)
        self.name = worker_id or self._worker.worker_id
        self.daemon = True
        
    @property
    def id(self):
        return self._worker.worker_id
    
    @property
    def worker_id(self):
        return self._worker.worker_id
    
    @property
    def state(self):
        return self._worker.state
    
    @property
    def is_running(self):
        return self._worker.state in (WorkerState.IDLE, WorkerState.BUSY)
    
    @property
    def is_healthy(self):
        return self._worker.is_alive
    
    def start(self):
        """Start the worker."""
        self._scheduler.start()
        self._worker.start()
    
    def stop(self, graceful=True, timeout=None, wait=True):
        """Stop the worker."""
        self._worker.stop(wait=wait, timeout=timeout)
        self._scheduler.stop(wait=False)
    
    def execute(self, job):
        """Execute a job synchronously."""
        # Set job state and execute through scheduler
        job.state = JobState.SCHEDULED
        result = self._scheduler.run_job(job)
        return result
    
    def submit(self, job):
        """Submit a job for async execution."""
        self._scheduler.submit(job)
        return None
    
    def cancel(self, job_id):
        """Cancel a job."""
        try:
            self._scheduler.cancel_job(str(job_id))
        except:
            pass
    
    def get_stats(self):
        """Get worker statistics."""
        info = self._worker.get_info()
        return {
            "jobs_processed": info.jobs_completed,
            "completed": info.jobs_completed,
            "failed": info.jobs_failed,
            "jobs_failed": info.jobs_failed,
        }
    
    def on_job_start(self, callback):
        """Register job start callback."""
        pass
    
    def on_job_complete(self, callback):
        """Register job complete callback."""
        self._scheduler.on_job_complete(lambda j, r: callback(j))
    
    def on_job_error(self, callback):
        """Register job error callback."""
        self._scheduler.on_job_failed(lambda j, r: callback(j, Exception(r.error or "Unknown")))
    
    def heartbeat(self):
        """Get last heartbeat."""
        return self._worker.get_info().last_heartbeat
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, *args):
        self.stop()
    
    def __repr__(self):
        return f"TestWorkerAdapter(id={self.worker_id}, state={self.state})"


@pytest.fixture
def worker():
    """Create a test worker."""
    w = TestWorkerAdapter()
    yield w
    # Cleanup
    try:
        w.stop()
    except:
        pass


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    def test_func():
        return "result"
    return Job(name="test_job", func=test_func)


@pytest.fixture
def failing_job():
    """Create a failing job for testing."""
    def failing_func():
        raise ValueError("Test error")
    
    from job_orchestrator.core.job import RetryPolicy
    return Job(
        name="failing_job",
        func=failing_func,
        retry_policy=RetryPolicy(max_retries=0)
    )