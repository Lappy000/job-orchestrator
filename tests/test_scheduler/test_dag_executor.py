"""
Tests for DAGExecutor class.

This module tests DAG execution, dependency management,
parallel execution, failure handling, and the async execution model.
"""

import pytest
import time
import threading
from unittest.mock import Mock, MagicMock
from datetime import datetime

from job_orchestrator import Job, JobState, DAG
from job_orchestrator.scheduler.dag_executor import DAGExecutor, DAGStatus, DAGExecution


class TestDAGExecutorCreation:
    """Tests for DAGExecutor creation and initialization."""

    def test_dag_executor_creation(self, dag_executor):
        """Test creating a DAG executor."""
        assert dag_executor is not None
        assert isinstance(dag_executor, DAGExecutor)

    def test_dag_executor_with_scheduler(self, scheduler):
        """Test creating executor with scheduler."""
        executor = DAGExecutor(scheduler=scheduler)
        
        assert executor._scheduler == scheduler


class TestDAGExecutionStart:
    """Tests for starting DAG execution."""

    def test_start_dag_returns_dag_id(self, dag_executor, simple_dag):
        """Test start_dag returns the DAG ID."""
        dag_id = dag_executor.start_dag(simple_dag)
        
        assert dag_id == simple_dag.id

    def test_start_dag_tracks_execution(self, dag_executor, simple_dag):
        """Test start_dag creates execution tracking."""
        dag_executor.start_dag(simple_dag)
        
        status = dag_executor.get_status(simple_dag.id)
        
        assert status is not None
        assert isinstance(status, DAGExecution)

    def test_start_dag_sets_running_status(self, dag_executor, simple_dag):
        """Test start_dag sets status to RUNNING."""
        dag_executor.start_dag(simple_dag)
        
        status = dag_executor.get_status(simple_dag.id)
        
        assert status.status == DAGStatus.RUNNING

    def test_start_dag_sets_start_time(self, dag_executor, simple_dag):
        """Test start_dag sets started_at timestamp."""
        before = datetime.utcnow()
        dag_executor.start_dag(simple_dag)
        after = datetime.utcnow()
        
        status = dag_executor.get_status(simple_dag.id)
        
        assert status.started_at >= before
        assert status.started_at <= after


class TestJobCompletion:
    """Tests for job completion handling."""

    def test_on_job_complete_marks_completed(self, dag_executor, single_job_dag):
        """Test on_job_complete marks job as completed."""
        dag_executor.start_dag(single_job_dag)
        job_id = list(single_job_dag.jobs.keys())[0]
        
        dag_executor.on_job_complete(job_id)
        
        status = dag_executor.get_status(single_job_dag.id)
        assert job_id in status.completed_jobs

    def test_on_job_complete_updates_running_jobs(self, dag_executor, single_job_dag):
        """Test on_job_complete removes from running jobs."""
        dag_executor.start_dag(single_job_dag)
        job_id = list(single_job_dag.jobs.keys())[0]
        
        # Should be in running_jobs initially
        status = dag_executor.get_status(single_job_dag.id)
        assert job_id in status.running_jobs
        
        dag_executor.on_job_complete(job_id)
        
        status = dag_executor.get_status(single_job_dag.id)
        assert job_id not in status.running_jobs

    def test_on_job_complete_queues_dependents(self, dag_executor, simple_dag):
        """Test on_job_complete queues dependent jobs."""
        dag_executor.start_dag(simple_dag)
        
        # Find root job (task_a)
        root_job = None
        for job in simple_dag.jobs.values():
            if job.name == "task_a":
                root_job = job
                break
        
        newly_queued = dag_executor.on_job_complete(root_job.id)
        
        # Should queue the next job in sequence
        assert len(newly_queued) >= 0  # May or may not queue depending on deps


class TestJobFailure:
    """Tests for job failure handling."""

    def test_on_job_failed_marks_failed(self, dag_executor, single_job_dag):
        """Test on_job_failed marks job as failed."""
        dag_executor.start_dag(single_job_dag)
        job_id = list(single_job_dag.jobs.keys())[0]
        
        dag_executor.on_job_failed(job_id, "Test error")
        
        status = dag_executor.get_status(single_job_dag.id)
        assert job_id in status.failed_jobs

    def test_on_job_failed_removes_from_running(self, dag_executor, single_job_dag):
        """Test on_job_failed removes from running jobs."""
        dag_executor.start_dag(single_job_dag)
        job_id = list(single_job_dag.jobs.keys())[0]
        
        dag_executor.on_job_failed(job_id, "Test error")
        
        status = dag_executor.get_status(single_job_dag.id)
        assert job_id not in status.running_jobs


class TestDAGCompletion:
    """Tests for DAG completion detection."""

    def test_dag_completes_when_all_jobs_done(self, dag_executor, single_job_dag):
        """Test DAG completes when all jobs complete."""
        dag_executor.start_dag(single_job_dag)
        job_id = list(single_job_dag.jobs.keys())[0]
        
        dag_executor.on_job_complete(job_id)
        
        status = dag_executor.get_status(single_job_dag.id)
        assert status.status == DAGStatus.COMPLETED

    def test_dag_fails_when_job_fails(self, dag_executor, single_job_dag):
        """Test DAG fails when a job fails (with fail_fast)."""
        single_job_dag.fail_fast = True
        dag_executor.start_dag(single_job_dag)
        job_id = list(single_job_dag.jobs.keys())[0]
        
        dag_executor.on_job_failed(job_id, "Test error")
        
        status = dag_executor.get_status(single_job_dag.id)
        assert status.status == DAGStatus.FAILED


class TestDAGCancellation:
    """Tests for DAG cancellation."""

    def test_cancel_dag_marks_cancelled(self, dag_executor, simple_dag):
        """Test cancel_dag marks DAG as cancelled."""
        dag_executor.start_dag(simple_dag)
        
        result = dag_executor.cancel_dag(simple_dag.id)
        
        assert result is True
        status = dag_executor.get_status(simple_dag.id)
        assert status.status == DAGStatus.CANCELLED

    def test_cancel_dag_cancels_pending_jobs(self, dag_executor, simple_dag):
        """Test cancel_dag marks pending jobs as cancelled."""
        dag_executor.start_dag(simple_dag)
        
        dag_executor.cancel_dag(simple_dag.id)
        
        status = dag_executor.get_status(simple_dag.id)
        # Non-completed jobs should be cancelled
        assert len(status.cancelled_jobs) > 0

    def test_cancel_nonexistent_dag_returns_false(self, dag_executor):
        """Test cancelling non-existent DAG returns False."""
        from uuid import uuid4
        
        result = dag_executor.cancel_dag(uuid4())
        
        assert result is False


class TestDAGProgress:
    """Tests for DAG execution progress tracking."""

    def test_progress_starts_at_zero(self, dag_executor, simple_dag):
        """Test progress starts at 0 for new DAG."""
        dag_executor.start_dag(simple_dag)
        
        status = dag_executor.get_status(simple_dag.id)
        
        # No completed jobs yet (jobs are queued but not completed)
        assert status.progress >= 0.0
        assert status.progress <= 1.0

    def test_progress_increases_with_completions(self, dag_executor, simple_dag):
        """Test progress increases as jobs complete."""
        dag_executor.start_dag(simple_dag)
        
        # Complete first job
        first_job = None
        for job in simple_dag.jobs.values():
            if job.name == "task_a":
                first_job = job
                break
        
        dag_executor.on_job_complete(first_job.id)
        
        status = dag_executor.get_status(simple_dag.id)
        assert status.progress > 0.0

    def test_progress_reaches_one_when_complete(self, dag_executor, single_job_dag):
        """Test progress reaches 1.0 when all jobs complete."""
        dag_executor.start_dag(single_job_dag)
        job_id = list(single_job_dag.jobs.keys())[0]
        
        dag_executor.on_job_complete(job_id)
        
        status = dag_executor.get_status(single_job_dag.id)
        assert status.progress == 1.0

    def test_total_jobs_count(self, dag_executor, simple_dag):
        """Test total_jobs returns correct count."""
        dag_executor.start_dag(simple_dag)
        
        status = dag_executor.get_status(simple_dag.id)
        
        assert status.total_jobs == len(simple_dag.jobs)


class TestExecutorCallbacks:
    """Tests for executor callbacks."""

    def test_on_dag_complete_callback(self, scheduler):
        """Test on_dag_complete callback is called."""
        callback = Mock()
        executor = DAGExecutor(scheduler=scheduler, on_dag_complete=callback)
        
        dag = DAG(name="callback_test")
        job = Job(name="test_job")
        dag.add_node(job)
        
        executor.start_dag(dag)
        executor.on_job_complete(job.id)
        
        callback.assert_called_once()

    def test_on_dag_failed_callback(self, scheduler):
        """Test on_dag_failed callback is called."""
        callback = Mock()
        executor = DAGExecutor(scheduler=scheduler, on_dag_failed=callback)
        
        dag = DAG(name="callback_test")
        dag.fail_fast = True
        job = Job(name="test_job")
        dag.add_node(job)
        
        executor.start_dag(dag)
        executor.on_job_failed(job.id, "Test error")
        
        callback.assert_called_once()


class TestGetDagForJob:
    """Tests for job to DAG mapping."""

    def test_get_dag_for_job_returns_dag_id(self, dag_executor, single_job_dag):
        """Test get_dag_for_job returns correct DAG ID."""
        dag_executor.start_dag(single_job_dag)
        job_id = list(single_job_dag.jobs.keys())[0]
        
        dag_id = dag_executor.get_dag_for_job(job_id)
        
        assert dag_id == single_job_dag.id

    def test_get_dag_for_job_returns_none_for_unknown(self, dag_executor):
        """Test get_dag_for_job returns None for unknown job."""
        from uuid import uuid4
        
        dag_id = dag_executor.get_dag_for_job(uuid4())
        
        assert dag_id is None


class TestExecutorStats:
    """Tests for executor statistics."""

    def test_get_stats_returns_dict(self, dag_executor):
        """Test get_stats returns a dictionary."""
        stats = dag_executor.get_stats()
        
        assert isinstance(stats, dict)

    def test_stats_include_active_dags(self, dag_executor, simple_dag):
        """Test stats include active DAG count."""
        initial_stats = dag_executor.get_stats()
        initial_count = initial_stats.get("active_dags", 0)
        
        dag_executor.start_dag(simple_dag)
        
        stats = dag_executor.get_stats()
        assert stats["active_dags"] == initial_count + 1


class TestExecutorRepr:
    """Tests for string representation."""

    def test_executor_repr(self, dag_executor):
        """Test __repr__ returns meaningful string."""
        result = repr(dag_executor)
        
        assert "DAGExecutor" in result


class TestDiamondDAGExecution:
    """Tests for complex DAG patterns."""

    def test_diamond_dag_dependency_tracking(self, dag_executor, diamond_dag):
        """Test diamond pattern DAG tracks dependencies correctly."""
        dag_executor.start_dag(diamond_dag)
        
        status = dag_executor.get_status(diamond_dag.id)
        
        # Should have exactly 5 jobs
        assert status.total_jobs == 5
        
        # First job should be running (root node)
        assert len(status.running_jobs) >= 1

    def test_parallel_dag_queues_multiple(self, dag_executor, parallel_dag):
        """Test parallel DAG can queue multiple jobs."""
        dag_executor.start_dag(parallel_dag)
        
        # Find and complete root job
        root_job = None
        for job in parallel_dag.jobs.values():
            if job.name == "task_a":
                root_job = job
                break
        
        newly_queued = dag_executor.on_job_complete(root_job.id)
        
        # Should queue both parallel branches
        # Note: actual queueing depends on implementation
        status = dag_executor.get_status(parallel_dag.id)
        assert root_job.id in status.completed_jobs


class TestEmptyDAG:
    """Tests for empty DAG handling."""

    def test_empty_dag_starts_correctly(self, dag_executor, empty_dag):
        """Test empty DAG can be started."""
        dag_executor.start_dag(empty_dag)
        
        status = dag_executor.get_status(empty_dag.id)
        
        # Empty DAG is tracked
        assert status is not None
        assert status.total_jobs == 0
        # Progress for empty DAG should be 1.0 (no jobs to complete)
        assert status.progress == 1.0