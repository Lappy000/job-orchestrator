"""
Tests for JobStore class.

This module tests job storage, retrieval, querying, and
state management in the job store.
"""

import pytest
import threading
from uuid import uuid4
from datetime import datetime, timedelta

from job_orchestrator import Job, JobState, JobPriority
from job_orchestrator.scheduler.job_store import JobStore


class TestJobStoreCreation:
    """Tests for JobStore creation and initialization."""

    def test_job_store_creation(self, job_store):
        """Test creating a job store."""
        assert job_store is not None
        assert isinstance(job_store, JobStore)

    def test_job_store_initially_empty(self, job_store):
        """Test job store is initially empty."""
        assert len(job_store) == 0
        assert job_store.is_empty is True


class TestJobStoreOperations:
    """Tests for basic job store operations."""

    def test_add_job(self, job_store, sample_job):
        """Test adding a job to the store."""
        job_store.add(sample_job)
        
        assert len(job_store) == 1
        assert sample_job.id in job_store

    def test_get_job(self, job_store, sample_job):
        """Test retrieving a job from the store."""
        job_store.add(sample_job)
        
        retrieved = job_store.get(sample_job.id)
        
        assert retrieved is not None
        assert retrieved.id == sample_job.id
        assert retrieved.name == sample_job.name

    def test_get_nonexistent_job(self, job_store):
        """Test getting non-existent job returns None."""
        retrieved = job_store.get(uuid4())
        
        assert retrieved is None

    def test_update_job(self, job_store, sample_job):
        """Test updating a job in the store."""
        job_store.add(sample_job)
        sample_job.state = JobState.RUNNING
        
        job_store.update(sample_job)
        
        retrieved = job_store.get(sample_job.id)
        assert retrieved.state == JobState.RUNNING

    def test_remove_job(self, job_store, sample_job):
        """Test removing a job from the store."""
        job_store.add(sample_job)
        
        removed = job_store.remove(sample_job.id)
        
        assert removed is True
        assert sample_job.id not in job_store

    def test_remove_nonexistent_job(self, job_store):
        """Test removing non-existent job returns False."""
        removed = job_store.remove(uuid4())
        
        assert removed is False

    def test_contains_check(self, job_store, sample_job):
        """Test __contains__ method."""
        job_store.add(sample_job)
        
        assert sample_job.id in job_store
        assert uuid4() not in job_store


class TestJobStoreQueries:
    """Tests for querying jobs from the store."""

    def test_get_all_jobs(self, job_store):
        """Test getting all jobs."""
        jobs = [Job(name=f"job_{i}") for i in range(5)]
        for job in jobs:
            job_store.add(job)
        
        all_jobs = job_store.get_all()
        
        assert len(all_jobs) == 5

    def test_get_jobs_by_state(self, job_store):
        """Test getting jobs by state."""
        pending_jobs = [Job(name=f"pending_{i}") for i in range(3)]
        running_jobs = [Job(name=f"running_{i}") for i in range(2)]
        
        for job in pending_jobs:
            job.state = JobState.PENDING
            job_store.add(job)
        
        for job in running_jobs:
            job.state = JobState.RUNNING
            job_store.add(job)
        
        pending = job_store.get_by_state(JobState.PENDING)
        running = job_store.get_by_state(JobState.RUNNING)
        
        assert len(pending) == 3
        assert len(running) == 2

    def test_get_jobs_by_priority(self, job_store):
        """Test getting jobs by priority."""
        high_jobs = [Job(name=f"high_{i}", priority=JobPriority.HIGH) for i in range(2)]
        low_jobs = [Job(name=f"low_{i}", priority=JobPriority.LOW) for i in range(3)]
        
        for job in high_jobs + low_jobs:
            job_store.add(job)
        
        high = job_store.get_by_priority(JobPriority.HIGH)
        low = job_store.get_by_priority(JobPriority.LOW)
        
        assert len(high) == 2
        assert len(low) == 3

    def test_get_pending_jobs(self, job_store):
        """Test getting pending jobs."""
        pending = Job(name="pending")
        pending.state = JobState.PENDING
        
        completed = Job(name="completed")
        completed.state = JobState.COMPLETED
        
        job_store.add(pending)
        job_store.add(completed)
        
        pending_jobs = job_store.get_pending()
        
        assert len(pending_jobs) == 1
        assert pending_jobs[0].name == "pending"

    def test_get_running_jobs(self, job_store):
        """Test getting running jobs."""
        running = Job(name="running")
        running.state = JobState.RUNNING
        
        job_store.add(running)
        
        running_jobs = job_store.get_running()
        
        assert len(running_jobs) == 1

    def test_get_completed_jobs(self, job_store):
        """Test getting completed jobs."""
        completed = Job(name="completed")
        completed.state = JobState.COMPLETED
        
        job_store.add(completed)
        
        completed_jobs = job_store.get_completed()
        
        assert len(completed_jobs) == 1

    def test_get_failed_jobs(self, job_store):
        """Test getting failed jobs."""
        failed = Job(name="failed")
        failed.state = JobState.FAILED
        
        job_store.add(failed)
        
        failed_jobs = job_store.get_failed()
        
        assert len(failed_jobs) == 1


class TestJobStoreScheduledJobs:
    """Tests for scheduled jobs in the store."""

    def test_get_scheduled_jobs(self, job_store):
        """Test getting scheduled jobs."""
        future = datetime.utcnow() + timedelta(hours=1)
        scheduled = Job(name="scheduled", scheduled_at=future)
        immediate = Job(name="immediate")
        
        job_store.add(scheduled)
        job_store.add(immediate)
        
        scheduled_jobs = job_store.get_scheduled()
        
        assert len(scheduled_jobs) == 1
        assert scheduled_jobs[0].name == "scheduled"

    def test_get_ready_scheduled_jobs(self, job_store):
        """Test getting scheduled jobs that are ready."""
        past = datetime.utcnow() - timedelta(seconds=1)
        future = datetime.utcnow() + timedelta(hours=1)
        
        ready_job = Job(name="ready", scheduled_at=past)
        not_ready_job = Job(name="not_ready", scheduled_at=future)
        
        job_store.add(ready_job)
        job_store.add(not_ready_job)
        
        ready = job_store.get_ready_scheduled()
        
        assert len(ready) == 1
        assert ready[0].name == "ready"

    def test_get_next_scheduled_time(self, job_store):
        """Test getting next scheduled job time."""
        now = datetime.utcnow()
        job1 = Job(name="job1", scheduled_at=now + timedelta(minutes=30))
        job2 = Job(name="job2", scheduled_at=now + timedelta(minutes=10))
        job3 = Job(name="job3", scheduled_at=now + timedelta(minutes=20))
        
        for job in [job1, job2, job3]:
            job_store.add(job)
        
        next_time = job_store.get_next_scheduled_time()
        
        expected = now + timedelta(minutes=10)
        assert abs((next_time - expected).total_seconds()) < 2


class TestJobStoreStateTransitions:
    """Tests for job state transitions in the store."""

    def test_transition_to_running(self, job_store, sample_job):
        """Test transitioning job to running state."""
        job_store.add(sample_job)
        
        job_store.mark_running(sample_job.id)
        
        job = job_store.get(sample_job.id)
        assert job.state == JobState.RUNNING
        assert job.started_at is not None

    def test_transition_to_completed(self, job_store, sample_job):
        """Test transitioning job to completed state."""
        sample_job.state = JobState.RUNNING
        job_store.add(sample_job)
        
        job_store.mark_completed(sample_job.id, result="success")
        
        job = job_store.get(sample_job.id)
        assert job.state == JobState.COMPLETED
        assert job.result == "success"
        assert job.completed_at is not None

    def test_transition_to_failed(self, job_store, sample_job):
        """Test transitioning job to failed state."""
        sample_job.state = JobState.RUNNING
        job_store.add(sample_job)
        
        error = Exception("test error")
        job_store.mark_failed(sample_job.id, error=error)
        
        job = job_store.get(sample_job.id)
        assert job.state == JobState.FAILED
        assert job.error is not None

    def test_transition_to_cancelled(self, job_store, sample_job):
        """Test transitioning job to cancelled state."""
        job_store.add(sample_job)
        
        job_store.mark_cancelled(sample_job.id)
        
        job = job_store.get(sample_job.id)
        assert job.state == JobState.CANCELLED


class TestJobStoreStats:
    """Tests for job store statistics."""

    def test_get_stats(self, job_store):
        """Test getting store statistics."""
        stats = job_store.get_stats()
        
        assert isinstance(stats, dict)
        assert "total" in stats

    def test_stats_count_pending(self, job_store):
        """Test stats count pending jobs."""
        for i in range(5):
            job = Job(name=f"job_{i}")
            job.state = JobState.PENDING
            job_store.add(job)
        
        stats = job_store.get_stats()
        
        assert stats.get("pending", 0) == 5 or stats.get("PENDING", 0) == 5

    def test_count_by_state(self, job_store):
        """Test counting jobs by state."""
        states = [JobState.PENDING, JobState.PENDING, JobState.RUNNING, JobState.COMPLETED]
        
        for i, state in enumerate(states):
            job = Job(name=f"job_{i}")
            job.state = state
            job_store.add(job)
        
        pending_count = job_store.count_by_state(JobState.PENDING)
        running_count = job_store.count_by_state(JobState.RUNNING)
        
        assert pending_count == 2
        assert running_count == 1


class TestJobStoreCleanup:
    """Tests for job store cleanup operations."""

    def test_clear_all(self, job_store):
        """Test clearing all jobs."""
        for i in range(10):
            job_store.add(Job(name=f"job_{i}"))
        
        job_store.clear()
        
        assert len(job_store) == 0

    def test_cleanup_completed(self, job_store):
        """Test cleaning up completed jobs."""
        completed = Job(name="completed")
        completed.state = JobState.COMPLETED
        
        pending = Job(name="pending")
        pending.state = JobState.PENDING
        
        job_store.add(completed)
        job_store.add(pending)
        
        job_store.cleanup_completed()
        
        assert len(job_store) == 1
        assert job_store.get(pending.id) is not None

    def test_cleanup_old_jobs(self, job_store):
        """Test cleaning up old jobs."""
        old_job = Job(name="old")
        old_job.state = JobState.COMPLETED
        old_job.completed_at = datetime.utcnow() - timedelta(days=30)
        
        recent_job = Job(name="recent")
        recent_job.state = JobState.COMPLETED
        recent_job.completed_at = datetime.utcnow() - timedelta(hours=1)
        
        job_store.add(old_job)
        job_store.add(recent_job)
        
        job_store.cleanup_older_than(timedelta(days=7))
        
        assert job_store.get(recent_job.id) is not None
        assert job_store.get(old_job.id) is None


class TestJobStoreThreadSafety:
    """Tests for thread-safe operations."""

    def test_concurrent_add(self, job_store):
        """Test concurrent job additions."""
        jobs = [Job(name=f"job_{i}") for i in range(100)]
        
        def add_job(job):
            job_store.add(job)
        
        threads = [threading.Thread(target=add_job, args=(job,)) for job in jobs]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(job_store) == 100

    def test_concurrent_get(self, job_store, sample_job):
        """Test concurrent job retrieval."""
        job_store.add(sample_job)
        results = []
        lock = threading.Lock()
        
        def get_job():
            job = job_store.get(sample_job.id)
            with lock:
                results.append(job)
        
        threads = [threading.Thread(target=get_job) for _ in range(50)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(results) == 50
        assert all(r.id == sample_job.id for r in results)

    def test_concurrent_update(self, job_store, sample_job):
        """Test concurrent job updates."""
        job_store.add(sample_job)
        
        def update_job(state):
            sample_job.state = state
            job_store.update(sample_job)
        
        states = [JobState.RUNNING, JobState.COMPLETED, JobState.RUNNING] * 10
        threads = [threading.Thread(target=update_job, args=(state,)) for state in states]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should complete without errors
        assert sample_job.id in job_store


class TestJobStoreIteration:
    """Tests for job store iteration."""

    def test_iterate_over_jobs(self, job_store):
        """Test iterating over jobs in store."""
        jobs = [Job(name=f"job_{i}") for i in range(5)]
        for job in jobs:
            job_store.add(job)
        
        iterated = list(job_store)
        
        assert len(iterated) == 5

    def test_len(self, job_store):
        """Test __len__ method."""
        for i in range(7):
            job_store.add(Job(name=f"job_{i}"))
        
        assert len(job_store) == 7


class TestJobStoreRepr:
    """Tests for string representation."""

    def test_str(self, job_store):
        """Test string representation."""
        job_store.add(Job(name="test"))
        
        result = str(job_store)
        
        assert len(result) > 0

    def test_repr(self, job_store):
        """Test repr."""
        result = repr(job_store)
        
        assert "JobStore" in result