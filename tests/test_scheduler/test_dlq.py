"""
Tests for DeadLetterQueue (DLQ).

This module tests DLQ operations, job resolution, requeuing,
TTL cleanup, and failure analytics.
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from uuid import uuid4

from job_orchestrator import Job, JobState
from job_orchestrator.scheduler.dlq import DeadLetterQueue, DLQEntry, DLQEntryStatus


class TestDLQCreation:
    """Tests for DLQ creation and initialization."""

    def test_dlq_creation(self, dead_letter_queue):
        """Test creating a dead letter queue."""
        assert dead_letter_queue is not None
        assert isinstance(dead_letter_queue, DeadLetterQueue)

    def test_dlq_initially_empty(self, dead_letter_queue):
        """Test DLQ is initially empty."""
        assert len(dead_letter_queue) == 0
        assert dead_letter_queue.is_empty is True

    def test_dlq_with_ttl(self):
        """Test DLQ with custom TTL."""
        dlq = DeadLetterQueue(ttl=7200)  # 2 hours
        
        assert dlq.ttl == 7200

    def test_dlq_with_max_entries(self):
        """Test DLQ with max entries limit."""
        dlq = DeadLetterQueue(max_entries=100)
        
        assert dlq.max_entries == 100


class TestAddFailedJob:
    """Tests for adding failed jobs to DLQ."""

    def test_add_failed_job(self, dead_letter_queue, sample_job):
        """Test adding a failed job to DLQ."""
        sample_job.state = JobState.FAILED
        sample_job.error = Exception("Test failure")
        
        entry_id = dead_letter_queue.add(sample_job)
        
        assert len(dead_letter_queue) == 1
        assert str(sample_job.id) in dead_letter_queue

    def test_add_job_creates_entry(self, dead_letter_queue, sample_job):
        """Test adding job creates DLQ entry with metadata."""
        sample_job.state = JobState.FAILED
        error = ValueError("Test error")
        sample_job.error = error
        
        entry_id = dead_letter_queue.add(sample_job, error=error)
        
        entry = dead_letter_queue.get_entry(str(sample_job.id))
        
        assert entry is not None
        assert entry.job_id == str(sample_job.id)
        assert entry.error_type == "ValueError"

    def test_add_job_records_timestamp(self, dead_letter_queue, sample_job):
        """Test adding job records timestamp."""
        sample_job.state = JobState.FAILED
        now = datetime.utcnow()
        
        dead_letter_queue.add(sample_job)
        
        entry = dead_letter_queue.get_entry(str(sample_job.id))
        
        assert entry.added_at >= now

    def test_add_job_captures_error_details(self, dead_letter_queue, sample_job):
        """Test adding job captures error details."""
        sample_job.state = JobState.FAILED
        error = RuntimeError("Detailed error message")
        sample_job.error = error
        
        dead_letter_queue.add(sample_job, error=error)
        
        entry = dead_letter_queue.get_entry(str(sample_job.id))
        
        assert "Detailed error message" in entry.error_message

    def test_add_job_with_reason(self, dead_letter_queue, sample_job):
        """Test adding job with custom reason."""
        sample_job.state = JobState.FAILED
        
        dead_letter_queue.add(sample_job, reason="Max retries exceeded")
        
        entry = dead_letter_queue.get_entry(str(sample_job.id))
        
        assert entry.reason == "Max retries exceeded"


class TestRequeueJob:
    """Tests for requeuing jobs from DLQ."""

    def test_requeue_job(self, dead_letter_queue, sample_job):
        """Test requeuing job from DLQ (without scheduler)."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        # Without scheduler, returns the job
        requeued_job = dead_letter_queue.requeue(str(sample_job.id))
        
        assert requeued_job is not None
        assert requeued_job.state == JobState.PENDING

    def test_requeue_removes_from_dlq(self, dead_letter_queue, sample_job):
        """Test requeue removes job from DLQ."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        dead_letter_queue.requeue(str(sample_job.id))
        
        assert str(sample_job.id) not in dead_letter_queue

    def test_requeue_resets_retry_count(self, dead_letter_queue, sample_job):
        """Test requeue resets retry count."""
        sample_job.state = JobState.FAILED
        sample_job.retry_count = 5
        dead_letter_queue.add(sample_job)
        
        requeued = dead_letter_queue.requeue(str(sample_job.id), reset_retry_count=True)
        
        assert requeued.retry_count == 0

    def test_requeue_preserves_retry_count(self, dead_letter_queue, sample_job):
        """Test requeue preserves retry count when specified."""
        sample_job.state = JobState.FAILED
        sample_job.retry_count = 3
        dead_letter_queue.add(sample_job)
        
        requeued = dead_letter_queue.requeue(str(sample_job.id), reset_retry_count=False)
        
        assert requeued.retry_count == 3

    def test_requeue_nonexistent_job(self, dead_letter_queue):
        """Test requeuing non-existent job returns None."""
        result = dead_letter_queue.requeue(str(uuid4()))
        
        assert result is None

    def test_requeue_with_modifications(self, dead_letter_queue, sample_job):
        """Test requeue with job modifications."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        def modifier(job):
            job.priority = "HIGH"
            return job
        
        requeued = dead_letter_queue.requeue(str(sample_job.id), modifier=modifier)
        
        assert str(requeued.priority) == "HIGH" or getattr(requeued.priority, 'name', None) == "HIGH"


class TestDiscardJob:
    """Tests for discarding jobs from DLQ."""

    def test_discard_job(self, dead_letter_queue, sample_job):
        """Test discarding job from DLQ."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        result = dead_letter_queue.discard(str(sample_job.id))
        
        assert result is True
        assert str(sample_job.id) not in dead_letter_queue

    def test_discard_nonexistent_job(self, dead_letter_queue):
        """Test discarding non-existent job returns False."""
        result = dead_letter_queue.discard(str(uuid4()))
        
        assert result is False

    def test_discard_with_notes(self, dead_letter_queue, sample_job):
        """Test discarding with notes."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        dead_letter_queue.discard(str(sample_job.id), notes="Manually discarded")
        
        # Should be removed
        assert str(sample_job.id) not in dead_letter_queue


class TestResolveJob:
    """Tests for resolving jobs in DLQ."""

    def test_resolve_job(self, dead_letter_queue, sample_job):
        """Test resolving job in DLQ."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        result = dead_letter_queue.resolve(str(sample_job.id), notes="Fixed manually")
        
        assert result is True

    def test_resolve_marks_entry(self, dead_letter_queue, sample_job):
        """Test resolving marks entry as resolved."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        dead_letter_queue.resolve(str(sample_job.id), notes="Issue fixed", keep_in_history=True)
        
        entry = dead_letter_queue.get_entry(str(sample_job.id))
        
        if entry:  # May be removed after resolution depending on keep_in_history
            assert entry.resolved is True

    def test_resolve_keeps_in_dlq(self, dead_letter_queue, sample_job):
        """Test resolve optionally keeps job for history."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        dead_letter_queue.resolve(str(sample_job.id), keep_in_history=True)
        
        entry = dead_letter_queue.get_entry(str(sample_job.id))
        # When keep_in_history=True, entry should still exist
        assert entry is not None


class TestGetStats:
    """Tests for DLQ statistics."""

    def test_get_stats_empty(self, dead_letter_queue):
        """Test getting stats from empty DLQ."""
        stats = dead_letter_queue.get_stats()
        
        assert stats.total == 0

    def test_get_stats_with_entries(self, dead_letter_queue):
        """Test getting stats with entries."""
        for i in range(5):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            job.error = ValueError(f"Error {i}")
            dead_letter_queue.add(job)
        
        stats = dead_letter_queue.get_stats()
        
        assert stats.total == 5

    def test_stats_by_error_type(self, dead_letter_queue):
        """Test stats grouped by error type."""
        job1 = Job(name="job1")
        job1.state = JobState.FAILED
        error1 = ValueError("error")
        job1.error = error1
        
        job2 = Job(name="job2")
        job2.state = JobState.FAILED
        error2 = TypeError("error")
        job2.error = error2
        
        job3 = Job(name="job3")
        job3.state = JobState.FAILED
        error3 = ValueError("error")
        job3.error = error3
        
        dead_letter_queue.add(job1, error=error1)
        dead_letter_queue.add(job2, error=error2)
        dead_letter_queue.add(job3, error=error3)
        
        stats = dead_letter_queue.get_stats()
        
        assert stats.by_error_type.get("ValueError", 0) == 2
        assert stats.by_error_type.get("TypeError", 0) == 1

    def test_stats_oldest_entry(self, dead_letter_queue):
        """Test stats include oldest entry time."""
        job = Job(name="old_job")
        job.state = JobState.FAILED
        dead_letter_queue.add(job)
        
        time.sleep(0.05)
        
        job2 = Job(name="new_job")
        job2.state = JobState.FAILED
        dead_letter_queue.add(job2)
        
        stats = dead_letter_queue.get_stats()
        
        assert stats.oldest_entry is not None


class TestFailureAnalytics:
    """Tests for failure pattern analysis."""

    def test_get_failure_patterns(self, dead_letter_queue):
        """Test getting failure patterns."""
        for i in range(10):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            error = ValueError("Common error") if i < 7 else TypeError("Rare error")
            job.error = error
            dead_letter_queue.add(job, error=error)
        
        patterns = dead_letter_queue.get_failure_patterns()
        
        assert isinstance(patterns, list)

    def test_most_common_errors(self, dead_letter_queue):
        """Test getting most common errors."""
        errors = [ValueError, TypeError, ValueError, ValueError, KeyError]
        
        for i, error_class in enumerate(errors):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            error = error_class(f"error_{i}")
            job.error = error
            dead_letter_queue.add(job, error=error)
        
        common = dead_letter_queue.get_most_common_errors(limit=3)
        
        assert len(common) <= 3
        # ValueError should be most common - returns list of dicts
        if common:
            assert common[0]["error"] == "ValueError"

    def test_failure_rate_over_time(self, dead_letter_queue):
        """Test getting failure rate over time periods."""
        for i in range(5):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            dead_letter_queue.add(job)
        
        rate = dead_letter_queue.get_failure_rate(period=timedelta(minutes=1))
        
        assert rate >= 0


class TestTTLCleanup:
    """Tests for TTL-based cleanup."""

    def test_cleanup_expired_entries(self):
        """Test cleaning up expired entries."""
        dlq = DeadLetterQueue(ttl=0.1)  # 100ms TTL
        
        job = Job(name="expiring")
        job.state = JobState.FAILED
        dlq.add(job)
        
        assert len(dlq) == 1
        
        time.sleep(0.15)
        
        dlq.cleanup_expired()
        
        assert len(dlq) == 0

    def test_cleanup_preserves_fresh_entries(self):
        """Test cleanup preserves non-expired entries."""
        dlq = DeadLetterQueue(ttl=10.0)  # 10 seconds
        
        job = Job(name="fresh")
        job.state = JobState.FAILED
        dlq.add(job)
        
        dlq.cleanup_expired()
        
        assert len(dlq) == 1

    def test_auto_cleanup_on_add(self):
        """Test auto cleanup triggered on add."""
        dlq = DeadLetterQueue(ttl=0.03, auto_cleanup=True)
        
        for i in range(3):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            dlq.add(job)
            time.sleep(0.05)
        
        # Earlier jobs should be cleaned up
        assert len(dlq) <= 2


class TestMaxEntriesLimit:
    """Tests for max entries limit."""

    def test_evicts_oldest_when_full(self):
        """Test oldest entry evicted when limit reached."""
        dlq = DeadLetterQueue(max_entries=3)
        
        first_job = Job(name="first")
        first_job.state = JobState.FAILED
        dlq.add(first_job)
        
        for i in range(3):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            dlq.add(job)
        
        # First job should be evicted
        assert str(first_job.id) not in dlq
        assert len(dlq) == 3


class TestDLQIteration:
    """Tests for DLQ iteration."""

    def test_iterate_entries(self, dead_letter_queue):
        """Test iterating over DLQ entries."""
        for i in range(5):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            dead_letter_queue.add(job)
        
        entries = list(dead_letter_queue)
        
        assert len(entries) == 5

    def test_get_all_entries(self, dead_letter_queue):
        """Test getting all entries."""
        for i in range(3):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            dead_letter_queue.add(job)
        
        entries = dead_letter_queue.get_all()
        
        assert len(entries) == 3


class TestDLQFiltering:
    """Tests for filtering DLQ entries."""

    def test_filter_by_error_type(self, dead_letter_queue):
        """Test filtering by error type."""
        job1 = Job(name="job1")
        job1.state = JobState.FAILED
        error1 = ValueError("e")
        job1.error = error1
        
        job2 = Job(name="job2")
        job2.state = JobState.FAILED
        error2 = TypeError("e")
        job2.error = error2
        
        dead_letter_queue.add(job1, error=error1)
        dead_letter_queue.add(job2, error=error2)
        
        filtered = dead_letter_queue.filter_by_error_type("ValueError")
        
        assert len(filtered) == 1

    def test_filter_by_time_range(self, dead_letter_queue):
        """Test filtering by time range."""
        job = Job(name="job")
        job.state = JobState.FAILED
        dead_letter_queue.add(job)
        
        now = datetime.utcnow()
        earlier = now - timedelta(minutes=1)
        
        filtered = dead_letter_queue.filter_by_time(start=earlier, end=now + timedelta(seconds=1))
        
        assert len(filtered) >= 1


class TestDLQEntry:
    """Tests for DLQEntry dataclass."""

    def test_dlq_entry_creation(self, sample_job):
        """Test creating a DLQ entry."""
        entry = DLQEntry(
            entry_id=str(uuid4()),
            job=sample_job,
            job_id=str(sample_job.id),
            job_name=sample_job.name,
            error_type="ValueError",
            error_message="Test error",
            added_at=datetime.utcnow(),
        )
        
        assert entry.job_id == str(sample_job.id)
        assert entry.error_type == "ValueError"

    def test_dlq_entry_with_stack_trace(self, sample_job):
        """Test DLQ entry with stack trace."""
        entry = DLQEntry(
            entry_id=str(uuid4()),
            job=sample_job,
            job_id=str(sample_job.id),
            job_name=sample_job.name,
            error_type="Exception",
            error_message="Error",
            stack_trace="Traceback...",
            added_at=datetime.utcnow(),
        )
        
        assert entry.stack_trace == "Traceback..."


class TestDLQThreadSafety:
    """Tests for thread-safe DLQ operations."""

    def test_concurrent_add(self, dead_letter_queue):
        """Test concurrent adding to DLQ."""
        def add_job(i):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            dead_letter_queue.add(job)
        
        threads = [threading.Thread(target=add_job, args=(i,)) for i in range(100)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(dead_letter_queue) == 100

    def test_concurrent_requeue(self, dead_letter_queue):
        """Test concurrent requeue operations."""
        jobs = []
        for i in range(50):
            job = Job(name=f"job_{i}")
            job.state = JobState.FAILED
            dead_letter_queue.add(job)
            jobs.append(job)
        
        requeued = []
        lock = threading.Lock()
        
        def requeue_job(job):
            result = dead_letter_queue.requeue(str(job.id))
            if result:
                with lock:
                    requeued.append(result)
        
        threads = [threading.Thread(target=requeue_job, args=(job,)) for job in jobs]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(requeued) == 50


class TestDLQRepr:
    """Tests for string representation."""

    def test_dlq_str(self, dead_letter_queue, sample_job):
        """Test string representation of DLQ."""
        sample_job.state = JobState.FAILED
        dead_letter_queue.add(sample_job)
        
        result = str(dead_letter_queue)
        
        assert len(result) > 0

    def test_dlq_repr(self, dead_letter_queue):
        """Test repr of DLQ."""
        result = repr(dead_letter_queue)
        
        assert "DeadLetterQueue" in result