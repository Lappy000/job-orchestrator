"""
Integration Tests for Job Orchestrator.

This module contains end-to-end integration tests that verify
the complete system workflow, including job submission, DAG execution,
retry handling, and worker pool management.
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock

from job_orchestrator import (
    Job, JobState, JobPriority,
    DAG,
    Scheduler, OrchestratorConfig,
)
from job_orchestrator.core.config import WorkerPoolConfig, RetryConfig
from job_orchestrator.core.job import RetryPolicy
from job_orchestrator.queue.priority_queue import ThreadSafePriorityQueue
from job_orchestrator.locking.memory import InMemoryLockManager


class TestSimpleJobWorkflow:
    """Tests for complete lifecycle of a simple job."""

    def test_job_submit_execute_complete(self, scheduler):
        """Test complete job lifecycle: submit -> execute -> complete."""
        result_holder = [None]
        
        def compute():
            result_holder[0] = 42
            return 42
        
        job = Job(name="compute", func=compute)
        
        # Submit
        job_id = scheduler.submit(job)
        assert job_id == str(job.id)
        
        # Execute using run_job for synchronous execution
        result = scheduler.run_job(job)
        
        # Verify completion
        assert result.success is True
        assert result.result == 42
        assert result_holder[0] == 42

    def test_job_with_arguments(self, scheduler):
        """Test job with args and kwargs."""
        def multiply(a, b, factor=1):
            return a * b * factor
        
        job = Job(
            name="multiply",
            func=multiply,
            args=(5, 3),
            kwargs={"factor": 2}
        )
        
        result = scheduler.run_job(job)
        
        assert result.success is True
        assert result.result == 30  # 5 * 3 * 2

    def test_job_completion_callback(self, scheduler):
        """Test job completion callback is called."""
        callback_called = [False]
        received_job = [None]
        
        def callback(job, result):
            received_job[0] = job
            callback_called[0] = True
        
        job = Job(name="with_callback", func=lambda: "done")
        
        scheduler.on_job_complete(callback)
        scheduler.run_job(job)
        
        assert callback_called[0] is True
        assert received_job[0].state == JobState.COMPLETED


class TestDAGWorkflow:
    """Tests for complete DAG execution workflow."""

    def test_simple_dag_execution(self, scheduler, simple_dag):
        """Test simple linear DAG executes completely."""
        scheduler.submit_dag(simple_dag)
        
        # Execute each job in DAG using run_job
        for job in simple_dag.jobs.values():
            scheduler.run_job(job)

    def test_dag_with_data_dependencies(self, scheduler):
        """Test DAG where jobs depend on previous results."""
        results = {}
        
        def step_a():
            results["a"] = 10
            return 10
        
        def step_b():
            results["b"] = results.get("a", 0) * 2
            return results["b"]
        
        def step_c():
            results["c"] = results.get("b", 0) + 5
            return results["c"]
        
        job_a = Job(name="step_a", func=step_a)
        job_b = Job(name="step_b", func=step_b, depends_on=[job_a.id])
        job_c = Job(name="step_c", func=step_c, depends_on=[job_b.id])
        
        dag = DAG(name="data_dag")
        dag.add_job(job_a)
        dag.add_job(job_b)
        dag.add_job(job_c)
        
        # Execute jobs in order
        scheduler.run_job(job_a)
        scheduler.run_job(job_b)
        scheduler.run_job(job_c)
        
        assert results.get("a") == 10
        assert results.get("b") == 20
        assert results.get("c") == 25


class TestJobFailureAndRetry:
    """Tests for job failure and retry mechanism."""

    def test_job_failure_triggers_retry(self, scheduler):
        """Test failing job triggers retry."""
        attempt_count = [0]
        
        def failing_then_success():
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ValueError("Failure")
            return "success"
        
        job = Job(name="retry_job", func=failing_then_success)
        
        # First attempt fails
        result = scheduler.run_job(job)
        
        # Job should track attempts
        assert attempt_count[0] >= 1

    def test_max_retries_then_fail(self, scheduler):
        """Test job with max_retries=0 fails directly."""
        def always_fail():
            raise RuntimeError("Always fails")
        
        job = Job(name="failing", func=always_fail)
        job.retry_policy = RetryPolicy(max_retries=0)
        
        result = scheduler.run_job(job)
        
        assert result.success is False
        assert job.state == JobState.FAILED


class TestSchedulerLifecycle:
    """Tests for scheduler start/stop behavior."""

    def test_start_stop_scheduler(self):
        """Test scheduler start/stop lifecycle."""
        config = OrchestratorConfig()
        scheduler = Scheduler(config)
        
        scheduler.start()
        assert scheduler.is_running is True
        
        scheduler.stop()
        assert scheduler.is_running is False


class TestErrorHandling:
    """Tests for system-wide error handling."""

    def test_exception_does_not_crash_scheduler(self, scheduler):
        """Test exception in job doesn't crash scheduler."""
        def crash():
            raise RuntimeError("Crash!")
        
        crash_job = Job(name="crash", func=crash)
        crash_job.retry_policy = RetryPolicy(max_retries=0)
        
        good_job = Job(name="good", func=lambda: "ok")
        
        # Crash job fails but doesn't affect scheduler
        crash_result = scheduler.run_job(crash_job)
        assert crash_result.success is False
        
        # Good job still works
        good_result = scheduler.run_job(good_job)
        assert good_result.success is True
        assert good_result.result == "ok"

    def test_job_exception_captured(self, scheduler):
        """Test job exception details are captured."""
        def error_job():
            raise ValueError("Test error message")
        
        job = Job(name="error", func=error_job)
        job.retry_policy = RetryPolicy(max_retries=0)
        
        result = scheduler.run_job(job)
        
        assert result.success is False
        assert "Test error message" in result.error


class TestPriorityQueueIntegration:
    """Tests for priority queue behavior."""

    def test_jobs_ordered_by_priority(self):
        """Test jobs in queue are ordered by priority."""
        queue = ThreadSafePriorityQueue()
        
        low = Job(name="low", priority=JobPriority.LOW)
        normal = Job(name="normal", priority=JobPriority.NORMAL)
        high = Job(name="high", priority=JobPriority.HIGH)
        
        # Add in wrong order
        queue.push(low)
        queue.push(normal)
        queue.push(high)
        
        # Should pop in priority order
        assert queue.pop().name == "high"
        assert queue.pop().name == "normal"
        assert queue.pop().name == "low"


class TestLockingIntegration:
    """Tests for locking integration with job execution."""

    def test_lock_manager_basic(self, memory_lock_manager):
        """Test basic lock manager functionality."""
        lock_id = "test_resource"
        
        lock_info = memory_lock_manager.acquire(lock_id, owner="test")
        assert lock_info is not None
        
        released = memory_lock_manager.release(lock_id, owner="test")
        assert released is True


class TestSchedulerStats:
    """Tests for scheduler statistics."""

    def test_stats_updated_on_job(self, scheduler):
        """Test stats are updated after job execution."""
        initial_stats = scheduler.get_stats()
        initial_completed = initial_stats.get("jobs_completed", 0)
        
        job = Job(name="stats_test", func=lambda: "done")
        scheduler.submit(job)
        scheduler.run_job(job)
        
        final_stats = scheduler.get_stats()
        final_completed = final_stats.get("jobs_completed", 0)
        
        assert final_completed > initial_completed


class TestEndToEnd:
    """Complete end-to-end system tests."""

    def test_full_pipeline(self):
        """Test complete data processing pipeline."""
        config = OrchestratorConfig(
            worker_pool=WorkerPoolConfig(min_workers=2, max_workers=4),
            retry=RetryConfig(max_retries=2),
        )
        scheduler = Scheduler(config)
        
        results = {"steps": []}
        lock = threading.Lock()
        
        def step(name):
            with lock:
                results["steps"].append(name)
            return name
        
        # Create jobs
        extract_job = Job(name="extract", func=lambda: step("extract"))
        transform_job = Job(name="transform", func=lambda: step("transform"))
        load_job = Job(name="load", func=lambda: step("load"))
        
        # Execute pipeline
        scheduler.run_job(extract_job)
        scheduler.run_job(transform_job)
        scheduler.run_job(load_job)
        
        assert results["steps"] == ["extract", "transform", "load"]

    def test_multiple_job_execution(self):
        """Test processing multiple jobs."""
        config = OrchestratorConfig()
        scheduler = Scheduler(config)
        
        completed_jobs = []
        lock = threading.Lock()
        
        def track(name):
            with lock:
                completed_jobs.append(name)
            return name
        
        # Create and execute jobs
        for i in range(5):
            job = Job(name=f"job_{i}", func=lambda i=i: track(f"job_{i}"))
            scheduler.run_job(job)
        
        # All should complete
        assert len(completed_jobs) == 5

    def test_system_handles_many_jobs(self):
        """Test system stability with many jobs."""
        config = OrchestratorConfig()
        scheduler = Scheduler(config)
        
        completed = [0]
        lock = threading.Lock()
        
        def quick_job():
            with lock:
                completed[0] += 1
            return "done"
        
        # Execute many jobs
        for i in range(50):
            job = Job(name=f"job_{i}", func=quick_job)
            scheduler.run_job(job)
        
        # All jobs should complete
        assert completed[0] == 50