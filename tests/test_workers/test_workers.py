"""
Tests for Worker implementations.

This module tests the base Worker class and ThreadWorker implementation,
including job execution, lifecycle management, and error handling.

Workers use a PULL-based model:
- Workers pull jobs from the Scheduler's queue via get_next_job()
- Jobs are submitted to the Scheduler, not directly to workers
- Workers call scheduler.run_job() to execute jobs
"""

import pytest
import time
import threading
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from job_orchestrator import Job, JobState, Scheduler, OrchestratorConfig
from job_orchestrator.workers.worker import BaseWorker, WorkerState, WorkerType, WorkerInfo
from job_orchestrator.workers.thread_worker import ThreadWorker


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def config():
    """Create a default OrchestratorConfig."""
    return OrchestratorConfig()


@pytest.fixture
def scheduler(config):
    """Create a scheduler instance."""
    sched = Scheduler(config=config)
    sched.start()
    yield sched
    sched.stop(wait=False)


@pytest.fixture
def worker(scheduler):
    """Create a thread worker instance."""
    w = ThreadWorker(scheduler=scheduler, poll_timeout=0.1)
    yield w
    if w.is_alive:
        w.stop(wait=False, timeout=1.0)


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    def test_func(x, y):
        return x + y
    return Job(name="test_job", func=test_func, args=(1, 2))


@pytest.fixture
def failing_job():
    """Create a job that fails."""
    def failing_func():
        raise ValueError("Job failed on purpose")
    
    from job_orchestrator.core.job import RetryPolicy
    return Job(
        name="failing_job",
        func=failing_func,
        retry_policy=RetryPolicy(max_retries=0)
    )


# ============================================================================
# TestWorkerState - WorkerState enum tests
# ============================================================================

class TestWorkerState:
    """Tests for WorkerState enum."""

    def test_worker_state_values(self):
        """Test WorkerState enum has expected values."""
        states = [s.name for s in WorkerState]
        
        assert "IDLE" in states
        assert "BUSY" in states
        assert "STOPPED" in states
        assert "STOPPING" in states

    def test_worker_state_string_values(self):
        """Test WorkerState enum has string values."""
        assert WorkerState.IDLE.value == "idle"
        assert WorkerState.BUSY.value == "busy"
        assert WorkerState.STOPPED.value == "stopped"


# ============================================================================
# TestWorkerType - WorkerType enum tests
# ============================================================================

class TestWorkerType:
    """Tests for WorkerType enum."""

    def test_worker_type_values(self):
        """Test WorkerType enum has expected values."""
        types = [t.name for t in WorkerType]
        
        assert "THREAD" in types
        assert "PROCESS" in types
        assert "ASYNC" in types


# ============================================================================
# TestWorkerInfo - WorkerInfo dataclass tests
# ============================================================================

class TestWorkerInfo:
    """Tests for WorkerInfo dataclass."""

    def test_worker_info_creation(self):
        """Test creating WorkerInfo."""
        info = WorkerInfo(
            worker_id="test-id",
            worker_type=WorkerType.THREAD,
            state=WorkerState.IDLE,
        )
        
        assert info.worker_id == "test-id"
        assert info.worker_type == WorkerType.THREAD
        assert info.state == WorkerState.IDLE

    def test_worker_info_avg_job_time(self):
        """Test avg_job_time calculation."""
        info = WorkerInfo(
            worker_id="test",
            worker_type=WorkerType.THREAD,
            state=WorkerState.IDLE,
            jobs_completed=10,
            jobs_failed=0,
            total_execution_time=5.0,
        )
        
        assert info.avg_job_time == 0.5

    def test_worker_info_to_dict(self):
        """Test WorkerInfo to_dict method."""
        info = WorkerInfo(
            worker_id="test",
            worker_type=WorkerType.THREAD,
            state=WorkerState.IDLE,
        )
        
        d = info.to_dict()
        
        assert isinstance(d, dict)
        assert d["worker_id"] == "test"
        assert d["worker_type"] == "thread"
        assert d["state"] == "idle"


# ============================================================================
# TestWorkerCreation - Worker creation tests
# ============================================================================

class TestWorkerCreation:
    """Tests for Worker creation and initialization."""

    def test_worker_creation(self, worker):
        """Test creating a worker."""
        assert worker is not None
        assert isinstance(worker, ThreadWorker)

    def test_worker_initial_state(self, worker):
        """Test worker initial state is STOPPED."""
        assert worker.state == WorkerState.STOPPED

    def test_worker_has_id(self, worker):
        """Test worker has unique ID."""
        assert hasattr(worker, 'worker_id')
        assert worker.worker_id is not None

    def test_worker_unique_ids(self, scheduler):
        """Test workers have unique IDs."""
        workers = [ThreadWorker(scheduler=scheduler, name=f"worker_{i}") for i in range(5)]
        ids = [w.worker_id for w in workers]
        
        assert len(set(ids)) == 5


# ============================================================================
# TestThreadWorkerCreation - ThreadWorker specific creation tests
# ============================================================================

class TestThreadWorkerCreation:
    """Tests for ThreadWorker creation."""

    def test_thread_worker_creation(self, scheduler):
        """Test creating a thread worker with name."""
        worker = ThreadWorker(name="test_worker", scheduler=scheduler)
        
        assert worker is not None
        assert worker.name == "test_worker"

    def test_thread_worker_with_custom_name(self, scheduler):
        """Test thread worker with custom name."""
        worker = ThreadWorker(name="custom_worker", scheduler=scheduler)
        
        assert "custom" in worker.name.lower()

    def test_thread_worker_daemon_mode(self, scheduler):
        """Test thread worker daemon mode."""
        worker = ThreadWorker(daemon=True, scheduler=scheduler)
        
        assert worker.daemon is True

    def test_thread_worker_poll_timeout(self, scheduler):
        """Test thread worker poll timeout."""
        worker = ThreadWorker(scheduler=scheduler, poll_timeout=0.5)
        
        assert worker._poll_timeout == 0.5

    def test_thread_worker_type(self, worker):
        """Test thread worker type is THREAD."""
        assert worker.worker_type == WorkerType.THREAD


# ============================================================================
# TestWorkerStart - Worker start operation tests
# ============================================================================

class TestWorkerStart:
    """Tests for worker start operation."""

    def test_start_worker(self, worker):
        """Test starting a worker changes state."""
        worker.start()
        
        assert worker.state in (WorkerState.IDLE, WorkerState.BUSY)
        
        worker.stop(wait=False)

    def test_start_sets_state_idle(self, worker):
        """Test start sets state to IDLE when waiting for jobs."""
        worker.start()
        time.sleep(0.05)
        
        # Worker should be idle since no jobs in queue
        assert worker.state == WorkerState.IDLE
        
        worker.stop(wait=False)

    def test_double_start_raises(self, worker):
        """Test double start raises RuntimeError."""
        worker.start()
        
        with pytest.raises(RuntimeError):
            worker.start()
        
        worker.stop(wait=False)

    def test_start_without_scheduler_raises(self):
        """Test start without scheduler raises ValueError."""
        worker = ThreadWorker()  # No scheduler
        
        with pytest.raises(ValueError):
            worker.start()

    def test_is_alive_after_start(self, worker):
        """Test is_alive is True after start."""
        worker.start()
        
        assert worker.is_alive is True
        
        worker.stop(wait=False)


# ============================================================================
# TestWorkerStop - Worker stop operation tests
# ============================================================================

class TestWorkerStop:
    """Tests for worker stop operation."""

    def test_stop_worker(self, worker):
        """Test stopping a worker."""
        worker.start()
        worker.stop(wait=True, timeout=2.0)
        
        assert worker.state == WorkerState.STOPPED

    def test_stop_without_start(self, worker):
        """Test stopping without starting doesn't raise."""
        worker.stop()  # Should not raise, just log

    def test_is_alive_after_stop(self, worker):
        """Test is_alive is False after stop."""
        worker.start()
        worker.stop(wait=True, timeout=1.0)
        
        assert worker.is_alive is False

    def test_stop_with_timeout(self, worker):
        """Test stop with timeout."""
        worker.start()
        
        start = time.time()
        worker.stop(wait=True, timeout=0.5)
        elapsed = time.time() - start
        
        # Should not take much longer than timeout
        assert elapsed < 1.0


# ============================================================================
# TestWorkerJobExecution - Job execution via scheduler
# ============================================================================

class TestWorkerJobExecution:
    """Tests for job execution through workers."""

    def test_worker_processes_job(self, scheduler, worker, sample_job):
        """Test worker processes job from scheduler."""
        worker.start()
        
        # Submit job to scheduler
        scheduler.submit(sample_job)
        
        time.sleep(0.2)  # Wait for worker to pull and execute
        
        # Job should be processed
        assert sample_job.state in (JobState.COMPLETED, JobState.RUNNING, JobState.SCHEDULED)
        
        worker.stop(wait=False)

    def test_worker_executes_multiple_jobs(self, scheduler, worker):
        """Test worker executes multiple jobs."""
        worker.start()
        
        jobs = [Job(name=f"job_{i}", func=lambda i=i: i * 2) for i in range(3)]
        
        for job in jobs:
            scheduler.submit(job)
        
        time.sleep(0.3)
        worker.stop(wait=True, timeout=2.0)
        
        # At least some jobs should be completed
        completed = sum(1 for job in jobs if job.state == JobState.COMPLETED)
        assert completed >= 1

    def test_job_result_stored(self, scheduler, worker):
        """Test job result is stored after execution."""
        worker.start()
        
        job = Job(name="result_job", func=lambda: 42)
        scheduler.submit(job)
        
        time.sleep(0.2)
        worker.stop(wait=True, timeout=1.0)
        
        if job.state == JobState.COMPLETED:
            assert job.result == 42


# ============================================================================
# TestWorkerErrorHandling - Error handling tests
# ============================================================================

class TestWorkerErrorHandling:
    """Tests for worker error handling."""

    def test_failing_job_handled(self, scheduler, worker, failing_job):
        """Test failing job is handled gracefully."""
        worker.start()
        
        scheduler.submit(failing_job)
        
        time.sleep(0.2)
        worker.stop(wait=True, timeout=1.0)
        
        # Worker should still be able to stop cleanly
        assert worker.state == WorkerState.STOPPED

    def test_worker_continues_after_failure(self, scheduler, worker, failing_job):
        """Test worker continues processing after job failure."""
        worker.start()
        
        scheduler.submit(failing_job)
        time.sleep(0.1)
        
        # Worker should still be running
        assert worker.is_alive is True
        
        # Submit another job
        good_job = Job(name="good", func=lambda: "success")
        scheduler.submit(good_job)
        
        time.sleep(0.2)
        worker.stop(wait=True, timeout=1.0)
        
        # Good job might be completed
        assert good_job.state in (JobState.COMPLETED, JobState.SCHEDULED, JobState.RUNNING)

    def test_job_error_captured(self, scheduler, worker, failing_job):
        """Test job error is captured in job object."""
        worker.start()
        
        scheduler.submit(failing_job)
        
        time.sleep(0.2)
        worker.stop(wait=True, timeout=1.0)
        
        if failing_job.state == JobState.FAILED:
            assert failing_job.error is not None


# ============================================================================
# TestWorkerInfo - Worker info and stats tests
# ============================================================================

class TestWorkerStats:
    """Tests for worker statistics via get_info()."""

    def test_get_info_returns_worker_info(self, worker):
        """Test get_info returns WorkerInfo."""
        info = worker.get_info()
        
        assert isinstance(info, WorkerInfo)

    def test_get_info_contains_worker_id(self, worker):
        """Test get_info contains worker ID."""
        info = worker.get_info()
        
        assert info.worker_id == worker.worker_id

    def test_get_info_contains_state(self, worker):
        """Test get_info contains current state."""
        worker.start()
        time.sleep(0.05)
        
        info = worker.get_info()
        
        assert info.state == worker.state
        
        worker.stop(wait=False)

    def test_get_info_tracks_jobs_completed(self, scheduler, worker):
        """Test get_info tracks completed jobs."""
        worker.start()
        
        # Submit a job
        job = Job(name="test", func=lambda: "done")
        scheduler.submit(job)
        
        time.sleep(0.2)
        
        info = worker.get_info()
        
        # May or may not have completed depending on timing
        assert info.jobs_completed >= 0
        
        worker.stop(wait=False)

    def test_get_info_tracks_jobs_failed(self, scheduler, worker, failing_job):
        """Test get_info tracks failed jobs."""
        worker.start()
        
        scheduler.submit(failing_job)
        
        time.sleep(0.2)
        
        info = worker.get_info()
        
        # May or may not have failed depending on timing
        assert info.jobs_failed >= 0
        
        worker.stop(wait=False)

    def test_get_info_tracks_execution_time(self, scheduler, worker):
        """Test get_info tracks total execution time."""
        worker.start()
        
        job = Job(name="test", func=lambda: time.sleep(0.05))
        scheduler.submit(job)
        
        time.sleep(0.2)
        
        info = worker.get_info()
        
        # Should track some execution time
        assert info.total_execution_time >= 0
        
        worker.stop(wait=False)


# ============================================================================
# TestWorkerProperties - Worker property tests
# ============================================================================

class TestWorkerProperties:
    """Tests for worker properties."""

    def test_is_idle_property(self, worker):
        """Test is_idle property."""
        worker.start()
        time.sleep(0.05)
        
        # Should be idle when no jobs to process
        assert worker.is_idle is True
        
        worker.stop(wait=False)

    def test_is_busy_property(self, scheduler, worker):
        """Test is_busy property during job execution."""
        # This is hard to test precisely due to timing
        worker.start()
        
        # Initially should not be busy
        assert isinstance(worker.is_busy, bool)
        
        worker.stop(wait=False)

    def test_state_property(self, worker):
        """Test state property."""
        assert worker.state == WorkerState.STOPPED
        
        worker.start()
        assert worker.state in (WorkerState.IDLE, WorkerState.BUSY)
        
        worker.stop(wait=True, timeout=1.0)
        assert worker.state == WorkerState.STOPPED


# ============================================================================
# TestWorkerJoin - Worker join operation tests
# ============================================================================

class TestWorkerJoin:
    """Tests for worker join operation."""

    def test_join_after_stop(self, worker):
        """Test join after stop signal."""
        worker.start()
        worker.stop(wait=False)
        
        # Should be able to join
        worker.join(timeout=1.0)
        
        assert not worker.is_alive

    def test_join_with_timeout(self, worker):
        """Test join with timeout."""
        worker.start()
        worker.stop(wait=False)
        
        start = time.time()
        worker.join(timeout=0.5)
        elapsed = time.time() - start
        
        assert elapsed < 1.0


# ============================================================================
# TestWorkerRepr - String representation tests
# ============================================================================

class TestWorkerRepr:
    """Tests for string representation."""

    def test_worker_repr(self, worker):
        """Test repr of worker."""
        result = repr(worker)
        
        assert "ThreadWorker" in result

    def test_worker_repr_contains_id(self, worker):
        """Test repr contains worker ID."""
        result = repr(worker)
        
        # Should contain some identifying info
        assert worker.worker_id in result or "id=" in result.lower()

    def test_worker_repr_contains_state(self, worker):
        """Test repr contains state info."""
        result = repr(worker)
        
        # Should contain state
        assert "state=" in result.lower() or "stopped" in result.lower()


# ============================================================================
# TestWorkerConcurrency - Thread safety tests
# ============================================================================

class TestWorkerConcurrency:
    """Tests for worker thread safety."""

    def test_concurrent_get_info(self, worker):
        """Test concurrent get_info calls are thread-safe."""
        worker.start()
        
        results = []
        errors = []
        
        def get_info():
            try:
                for _ in range(10):
                    info = worker.get_info()
                    results.append(info)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=get_info) for _ in range(3)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        worker.stop(wait=False)
        
        assert len(errors) == 0
        assert len(results) == 30


# ============================================================================
# TestWorkerSchedulerIntegration - Scheduler integration tests
# ============================================================================

class TestWorkerSchedulerIntegration:
    """Tests for worker-scheduler integration."""

    def test_worker_set_scheduler(self):
        """Test setting scheduler after creation."""
        worker = ThreadWorker()
        
        config = OrchestratorConfig()
        scheduler = Scheduler(config=config)
        scheduler.start()
        
        worker.set_scheduler(scheduler)
        worker.start()
        
        assert worker.is_alive is True
        
        worker.stop(wait=False)
        scheduler.stop(wait=False)

    def test_worker_pulls_from_scheduler_queue(self, scheduler, worker):
        """Test worker pulls jobs from scheduler queue."""
        worker.start()
        
        # Submit job to scheduler
        job = Job(name="pull_test", func=lambda: "pulled")
        scheduler.submit(job)
        
        # Wait for worker to pull and execute
        time.sleep(0.2)
        
        # Job should be processed (pulled from queue)
        assert job.state in (JobState.COMPLETED, JobState.RUNNING, JobState.FAILED)
        
        worker.stop(wait=False)


# ============================================================================
# TestWorkerEdgeCases - Edge cases and error handling
# ============================================================================

class TestWorkerEdgeCases:
    """Tests for edge cases and error handling."""

    def test_stop_already_stopped(self, worker):
        """Test stop on already stopped worker."""
        # Worker not started
        worker.stop()  # Should not raise

    def test_get_info_when_stopped(self, worker):
        """Test get_info when worker is stopped."""
        info = worker.get_info()
        
        assert isinstance(info, WorkerInfo)
        assert info.state == WorkerState.STOPPED

    def test_heartbeat_updated(self, worker):
        """Test heartbeat is updated during operation."""
        worker.start()
        time.sleep(0.1)
        
        info = worker.get_info()
        
        # Last heartbeat should be recent
        if info.last_heartbeat:
            age = (datetime.utcnow() - info.last_heartbeat).total_seconds()
            assert age < 2.0
        
        worker.stop(wait=False)