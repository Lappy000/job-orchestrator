"""
Tests for the main Scheduler class.

This module tests job submission, execution, lifecycle management,
callbacks, and scheduler start/stop operations.
"""

import pytest
import time
import threading
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta
from uuid import uuid4

from job_orchestrator import Job, JobState, JobPriority, DAG, Scheduler, OrchestratorConfig
from job_orchestrator.core.config import QueueConfig, WorkerPoolConfig
from job_orchestrator.core.job import RetryPolicy


class TestSchedulerCreation:
    """Tests for scheduler creation and initialization."""

    def test_scheduler_creation_default(self, scheduler):
        """Test creating scheduler with default configuration."""
        assert scheduler is not None
        assert isinstance(scheduler, Scheduler)

    def test_scheduler_creation_with_config(self, default_config):
        """Test creating scheduler with custom configuration."""
        scheduler = Scheduler(config=default_config)
        
        assert scheduler._config == default_config

    def test_scheduler_initial_state(self, scheduler):
        """Test scheduler initial state before starting."""
        assert scheduler.is_running is False

    def test_scheduler_has_job_store(self, scheduler):
        """Test scheduler has job store."""
        assert hasattr(scheduler, '_job_store')

    def test_scheduler_has_dead_letter_queue(self, scheduler):
        """Test scheduler has dead letter queue."""
        assert hasattr(scheduler, '_dlq')


class TestSubmitJob:
    """Tests for single job submission."""

    def test_submit_job_returns_id(self, scheduler, sample_job):
        """Test submitting job returns job ID."""
        job_id = scheduler.submit(sample_job)
        
        assert job_id == str(sample_job.id)

    def test_submit_job_stores_job(self, scheduler, sample_job):
        """Test submitted job is stored."""
        scheduler.submit(sample_job)
        
        stored_job = scheduler.get_job(str(sample_job.id))
        
        assert stored_job is not None
        assert stored_job.id == sample_job.id

    def test_submit_job_sets_state(self, scheduler, sample_job):
        """Test submitted job has appropriate state."""
        scheduler.submit(sample_job)
        
        job = scheduler.get_job(str(sample_job.id))
        
        assert job.state in (JobState.PENDING, JobState.SCHEDULED)

    def test_submit_scheduled_job(self, scheduler):
        """Test submitting scheduled job."""
        future = datetime.utcnow() + timedelta(seconds=10)
        job = Job(name="scheduled", scheduled_at=future)
        
        job_id = scheduler.submit(job)
        
        assert job_id == str(job.id)
        stored = scheduler.get_job(job_id)
        assert stored.scheduled_at == future

    def test_submit_high_priority_job(self, scheduler, high_priority_job):
        """Test submitting high priority job."""
        job_id = scheduler.submit(high_priority_job)
        
        job = scheduler.get_job(job_id)
        
        assert job.priority == JobPriority.HIGH

    def test_submit_multiple_jobs(self, scheduler):
        """Test submitting multiple jobs."""
        jobs = [Job(name=f"job_{i}") for i in range(10)]
        
        ids = [scheduler.submit(job) for job in jobs]
        
        assert len(set(ids)) == 10
        for job_id in ids:
            assert scheduler.get_job(job_id) is not None


class TestSubmitDAG:
    """Tests for DAG submission."""

    def test_submit_dag_returns_id(self, scheduler, simple_dag):
        """Test submitting DAG returns DAG ID."""
        dag_id = scheduler.submit_dag(simple_dag)
        
        assert dag_id == str(simple_dag.id)

    def test_submit_dag_tracks_dag(self, scheduler, simple_dag):
        """Test DAG is tracked after submission."""
        scheduler.submit_dag(simple_dag)
        
        # DAG executor should track the DAG
        status = scheduler.get_dag_status(str(simple_dag.id))
        assert status["dag_id"] == str(simple_dag.id)


class TestJobExecution:
    """Tests for job execution using run_job()."""

    def test_job_executed_with_run_job(self, scheduler):
        """Test job is executed using run_job()."""
        result_holder = []
        
        def capture_result(x, y):
            res = x + y
            result_holder.append(res)
            return res
        
        job = Job(name="add", func=capture_result, args=(2, 3))
        
        # Use run_job for synchronous execution
        result = scheduler.run_job(job)
        
        assert result.success is True
        assert result.result == 5
        assert job.state == JobState.COMPLETED

    def test_failing_job_marked_failed(self, scheduler):
        """Test failing job transitions to FAILED state."""
        def fail_func():
            raise RuntimeError("Intentional failure")
        
        # Create job with no retries
        job = Job(name="failing", func=fail_func)
        job.retry_policy = RetryPolicy(max_retries=0)
        
        result = scheduler.run_job(job)
        
        assert result.success is False
        assert job.state == JobState.FAILED

    def test_job_exception_captured(self, scheduler):
        """Test job exception is captured."""
        def fail_func():
            raise ValueError("Test error")
        
        job = Job(name="failing", func=fail_func)
        job.retry_policy = RetryPolicy(max_retries=0)
        
        result = scheduler.run_job(job)
        
        assert result.error is not None
        assert "Test error" in result.error


class TestJobCompletionCallback:
    """Tests for job completion callbacks."""

    def test_callback_called_on_success(self, scheduler):
        """Test callback is called when job succeeds."""
        callback = Mock()
        job = Job(name="success", func=lambda: "done")
        
        scheduler.on_job_complete(callback)
        scheduler.run_job(job)
        
        callback.assert_called()

    def test_callback_receives_job(self, scheduler):
        """Test callback receives the completed job."""
        received_job = [None]
        
        def callback(job, result):
            received_job[0] = job
        
        job = Job(name="test", func=lambda: "result")
        scheduler.on_job_complete(callback)
        scheduler.run_job(job)
        
        assert received_job[0] is not None
        assert received_job[0].state == JobState.COMPLETED

    def test_callback_called_on_failure(self, scheduler):
        """Test callback is called when job fails."""
        callback = Mock()
        
        def fail_func():
            raise RuntimeError("Fail")
        
        job = Job(name="failing", func=fail_func)
        job.retry_policy = RetryPolicy(max_retries=0)
        scheduler.on_job_failed(callback)
        scheduler.run_job(job)
        
        callback.assert_called()


class TestGetJobStatus:
    """Tests for job status retrieval."""

    def test_get_job_status_pending(self, scheduler, sample_job):
        """Test getting status of pending job."""
        scheduler.submit(sample_job)
        
        status = scheduler.get_job_status(str(sample_job.id))
        
        assert status in (JobState.PENDING, JobState.SCHEDULED)

    def test_get_job_status_completed(self, scheduler):
        """Test getting status of completed job."""
        job = Job(name="quick", func=lambda: "done")
        scheduler.submit(job)
        scheduler.run_job(job)
        
        status = scheduler.get_job_status(str(job.id))
        
        assert status == JobState.COMPLETED

    def test_get_job_status_nonexistent(self, scheduler):
        """Test getting status of non-existent job."""
        from job_orchestrator.core.exceptions import JobNotFoundError
        
        with pytest.raises(JobNotFoundError):
            scheduler.get_job_status(str(uuid4()))

    def test_get_job_result(self, scheduler):
        """Test getting job result."""
        job = Job(name="compute", func=lambda: 42)
        scheduler.submit(job)
        scheduler.run_job(job)
        
        stored_job = scheduler.get_job(str(job.id))
        
        assert stored_job.result == 42


class TestCancelJob:
    """Tests for job cancellation."""

    def test_cancel_pending_job(self, scheduler, sample_job):
        """Test cancelling pending job."""
        scheduler.submit(sample_job)
        
        success = scheduler.cancel_job(str(sample_job.id))
        
        assert success is True
        job = scheduler.get_job(str(sample_job.id))
        assert job.state == JobState.CANCELLED

    def test_cancel_completed_job_fails(self, scheduler):
        """Test cancelling completed job fails."""
        job = Job(name="quick", func=lambda: "done")
        scheduler.submit(job)
        scheduler.run_job(job)
        
        success = scheduler.cancel_job(str(job.id))
        
        assert success is False

    def test_cancel_nonexistent_job(self, scheduler):
        """Test cancelling non-existent job."""
        from job_orchestrator.core.exceptions import JobNotFoundError
        
        with pytest.raises(JobNotFoundError):
            scheduler.cancel_job(str(uuid4()))


class TestSchedulerLifecycle:
    """Tests for scheduler start/stop lifecycle."""

    def test_start_scheduler(self, scheduler):
        """Test starting the scheduler."""
        scheduler.start()
        
        assert scheduler.is_running is True
        
        scheduler.stop()

    def test_stop_scheduler(self, scheduler):
        """Test stopping the scheduler."""
        scheduler.start()
        scheduler.stop()
        
        assert scheduler.is_running is False

    def test_stop_without_start(self, scheduler):
        """Test stopping without starting doesn't raise."""
        scheduler.stop()  # Should not raise

    def test_double_start_is_safe(self, scheduler):
        """Test double start is handled gracefully."""
        scheduler.start()
        
        # Should be idempotent - no exception expected
        scheduler.start()
        
        scheduler.stop()

    def test_start_stop_start(self, scheduler):
        """Test restart capability."""
        scheduler.start()
        scheduler.stop()
        scheduler.start()
        
        assert scheduler.is_running is True
        
        scheduler.stop()


class TestSchedulerEvents:
    """Tests for scheduler events and hooks."""

    def test_on_job_complete_hook(self, scheduler):
        """Test on_job_complete hook is called."""
        hook = Mock()
        scheduler.on_job_complete(hook)
        
        job = Job(name="test", func=lambda: "done")
        scheduler.run_job(job)
        
        hook.assert_called()

    def test_on_job_failure_hook(self, scheduler):
        """Test on_job_failure hook is called on failure."""
        hook = Mock()
        scheduler.on_job_failed(hook)
        
        def fail():
            raise RuntimeError("fail")
        
        job = Job(name="failing", func=fail)
        job.retry_policy = RetryPolicy(max_retries=0)
        scheduler.run_job(job)
        
        hook.assert_called()


class TestSchedulerStats:
    """Tests for scheduler statistics."""

    def test_get_stats(self, scheduler):
        """Test getting scheduler statistics."""
        stats = scheduler.get_stats()
        
        assert isinstance(stats, dict)
        assert "jobs_submitted" in stats
        assert "jobs_completed" in stats

    def test_stats_update_on_job(self, scheduler):
        """Test stats update after job execution."""
        initial_stats = scheduler.get_stats()
        initial_completed = initial_stats.get("jobs_completed", 0)
        
        job = Job(name="test", func=lambda: "done")
        scheduler.submit(job)
        scheduler.run_job(job)
        
        final_stats = scheduler.get_stats()
        final_completed = final_stats.get("jobs_completed", 0)
        
        assert final_completed > initial_completed


class TestSchedulerConcurrency:
    """Tests for concurrent scheduler operations."""

    def test_concurrent_submit(self, scheduler):
        """Test concurrent job submission."""
        jobs = [Job(name=f"job_{i}", func=lambda i=i: i) for i in range(100)]
        
        def submit_job(job):
            scheduler.submit(job)
        
        threads = [threading.Thread(target=submit_job, args=(job,)) for job in jobs]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        for job in jobs:
            assert scheduler.get_job(str(job.id)) is not None

    def test_sync_result(self, scheduler):
        """Test synchronous result."""
        def delayed_result():
            return "done"
        
        job = Job(name="sync", func=delayed_result)
        result = scheduler.run_job(job)
        
        assert result.result == "done"
        assert job.state == JobState.COMPLETED
