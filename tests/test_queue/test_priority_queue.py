"""
Tests for ThreadSafePriorityQueue.

This module tests the priority queue implementation, including priority ordering,
thread safety, scheduled jobs, lazy deletion, and blocking operations.
"""

import pytest
import threading
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from job_orchestrator import Job, JobPriority
from job_orchestrator.queue.priority_queue import ThreadSafePriorityQueue


class TestPriorityQueueCreation:
    """Tests for priority queue creation and initialization."""

    def test_queue_creation_empty(self, empty_priority_queue):
        """Test creating an empty priority queue."""
        assert empty_priority_queue is not None
        assert len(empty_priority_queue) == 0
        assert empty_priority_queue.empty() is True

    def test_queue_with_max_size(self):
        """Test creating queue with max size."""
        queue = ThreadSafePriorityQueue(maxsize=10)
        
        assert queue.maxsize == 10

    def test_queue_default_unlimited_size(self):
        """Test default queue has unlimited size."""
        queue = ThreadSafePriorityQueue()
        
        assert queue.maxsize == 0 or queue.maxsize is None


class TestPushPopOrder:
    """Tests for priority-ordered push and pop operations."""

    def test_push_single_job(self, empty_priority_queue, sample_job):
        """Test pushing a single job."""
        empty_priority_queue.push(sample_job)
        
        assert len(empty_priority_queue) == 1
        assert empty_priority_queue.empty() is False

    def test_pop_single_job(self, empty_priority_queue, sample_job):
        """Test popping a single job."""
        empty_priority_queue.push(sample_job)
        popped = empty_priority_queue.pop()
        
        assert popped == sample_job
        assert empty_priority_queue.empty() is True

    def test_priority_ordering_high_first(self, empty_priority_queue):
        """Test higher priority jobs are popped first."""
        low_job = Job(name="low", priority=JobPriority.LOW)
        high_job = Job(name="high", priority=JobPriority.HIGH)
        normal_job = Job(name="normal", priority=JobPriority.NORMAL)
        
        # Push in non-priority order
        empty_priority_queue.push(low_job)
        empty_priority_queue.push(normal_job)
        empty_priority_queue.push(high_job)
        
        # Pop should return in priority order
        assert empty_priority_queue.pop() == high_job
        assert empty_priority_queue.pop() == normal_job
        assert empty_priority_queue.pop() == low_job

    def test_critical_priority_first(self, empty_priority_queue):
        """Test CRITICAL priority jobs are popped before HIGH."""
        critical_job = Job(name="critical", priority=JobPriority.CRITICAL)
        high_job = Job(name="high", priority=JobPriority.HIGH)
        
        empty_priority_queue.push(high_job)
        empty_priority_queue.push(critical_job)
        
        assert empty_priority_queue.pop() == critical_job
        assert empty_priority_queue.pop() == high_job

    def test_background_priority_last(self, empty_priority_queue):
        """Test BACKGROUND priority jobs are popped last."""
        background_job = Job(name="background", priority=JobPriority.BACKGROUND)
        low_job = Job(name="low", priority=JobPriority.LOW)
        
        empty_priority_queue.push(background_job)
        empty_priority_queue.push(low_job)
        
        assert empty_priority_queue.pop() == low_job
        assert empty_priority_queue.pop() == background_job

    def test_same_priority_fifo(self, empty_priority_queue):
        """Test same priority jobs follow FIFO order."""
        job1 = Job(name="first", priority=JobPriority.NORMAL)
        job2 = Job(name="second", priority=JobPriority.NORMAL)
        job3 = Job(name="third", priority=JobPriority.NORMAL)
        
        empty_priority_queue.push(job1)
        empty_priority_queue.push(job2)
        empty_priority_queue.push(job3)
        
        assert empty_priority_queue.pop() == job1
        assert empty_priority_queue.pop() == job2
        assert empty_priority_queue.pop() == job3

    def test_all_priority_levels(self, empty_priority_queue):
        """Test all priority levels in correct order."""
        jobs = [
            Job(name="background", priority=JobPriority.BACKGROUND),
            Job(name="low", priority=JobPriority.LOW),
            Job(name="normal", priority=JobPriority.NORMAL),
            Job(name="high", priority=JobPriority.HIGH),
            Job(name="critical", priority=JobPriority.CRITICAL),
        ]
        
        # Push in reverse priority order
        for job in jobs:
            empty_priority_queue.push(job)
        
        # Should pop in priority order
        results = [empty_priority_queue.pop() for _ in range(5)]
        
        assert results[0].name == "critical"
        assert results[1].name == "high"
        assert results[2].name == "normal"
        assert results[3].name == "low"
        assert results[4].name == "background"


class TestThreadSafety:
    """Tests for thread-safe concurrent operations."""

    def test_concurrent_push(self, empty_priority_queue):
        """Test concurrent push operations."""
        jobs = [Job(name=f"job_{i}") for i in range(100)]
        
        def push_job(job):
            empty_priority_queue.push(job)
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(push_job, jobs)
        
        assert len(empty_priority_queue) == 100

    def test_concurrent_pop(self, empty_priority_queue):
        """Test concurrent pop operations."""
        jobs = [Job(name=f"job_{i}") for i in range(100)]
        for job in jobs:
            empty_priority_queue.push(job)
        
        results = []
        lock = threading.Lock()
        
        def pop_job():
            try:
                job = empty_priority_queue.pop(timeout=0.1)
                with lock:
                    results.append(job)
            except Exception:
                pass
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(pop_job) for _ in range(100)]
            for f in futures:
                f.result()
        
        assert len(results) == 100
        assert len(empty_priority_queue) == 0

    def test_concurrent_push_pop(self, empty_priority_queue):
        """Test concurrent push and pop operations."""
        push_count = [0]
        pop_count = [0]
        lock = threading.Lock()
        
        def pusher():
            for i in range(50):
                job = Job(name=f"job_{i}")
                empty_priority_queue.push(job)
                with lock:
                    push_count[0] += 1
        
        def popper():
            for _ in range(50):
                try:
                    empty_priority_queue.pop(timeout=1.0)
                    with lock:
                        pop_count[0] += 1
                except Exception:
                    pass
        
        threads = [
            threading.Thread(target=pusher),
            threading.Thread(target=popper),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All pushed items should be popped
        assert push_count[0] == 50
        assert pop_count[0] == 50

    def test_no_race_conditions(self, empty_priority_queue):
        """Test for absence of race conditions."""
        errors = []
        lock = threading.Lock()
        
        def worker():
            try:
                for _ in range(100):
                    job = Job(name="test")
                    empty_priority_queue.push(job)
                    empty_priority_queue.pop(timeout=1.0)
            except Exception as e:
                with lock:
                    errors.append(e)
        
        threads = [threading.Thread(target=worker) for _ in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0


class TestScheduledJobs:
    """Tests for scheduled job handling."""

    def test_push_scheduled_job(self, empty_priority_queue):
        """Test pushing a scheduled job."""
        future_time = datetime.utcnow() + timedelta(seconds=10)
        job = Job(name="scheduled", scheduled_at=future_time)
        
        empty_priority_queue.push(job)
        
        assert len(empty_priority_queue) == 1

    def test_scheduled_job_not_ready(self, empty_priority_queue):
        """Test scheduled job not returned before scheduled time."""
        future_time = datetime.utcnow() + timedelta(seconds=10)
        job = Job(name="scheduled", scheduled_at=future_time)
        
        empty_priority_queue.push(job)
        
        # Should not return the job yet (with short timeout)
        result = empty_priority_queue.pop_if_ready(timeout=0.1)
        
        assert result is None

    def test_scheduled_job_ready_after_time(self, empty_priority_queue):
        """Test scheduled job returned after scheduled time."""
        past_time = datetime.utcnow() - timedelta(seconds=1)
        job = Job(name="ready", scheduled_at=past_time)
        
        empty_priority_queue.push(job)
        
        result = empty_priority_queue.pop_if_ready(timeout=0.1)
        
        assert result == job

    def test_immediate_job_before_scheduled(self, empty_priority_queue):
        """Test immediate job returned before scheduled job."""
        future_time = datetime.utcnow() + timedelta(seconds=10)
        scheduled_job = Job(name="scheduled", scheduled_at=future_time, priority=JobPriority.HIGH)
        immediate_job = Job(name="immediate", priority=JobPriority.LOW)
        
        empty_priority_queue.push(scheduled_job)
        empty_priority_queue.push(immediate_job)
        
        result = empty_priority_queue.pop_if_ready(timeout=0.1)
        
        assert result == immediate_job

    def test_get_next_scheduled_time(self, empty_priority_queue):
        """Test getting next scheduled time."""
        now = datetime.utcnow()
        job1 = Job(name="job1", scheduled_at=now + timedelta(seconds=30))
        job2 = Job(name="job2", scheduled_at=now + timedelta(seconds=10))
        job3 = Job(name="job3", scheduled_at=now + timedelta(seconds=20))
        
        for job in [job1, job2, job3]:
            empty_priority_queue.push(job)
        
        next_time = empty_priority_queue.get_next_scheduled_time()
        
        # Should be closest to now (job2's scheduled time)
        assert next_time is not None
        assert abs((next_time - (now + timedelta(seconds=10))).total_seconds()) < 1


class TestLazyDeletion:
    """Tests for lazy deletion mechanism."""

    def test_delete_item(self, empty_priority_queue, sample_job):
        """Test deleting an item."""
        empty_priority_queue.push(sample_job)
        empty_priority_queue.delete(sample_job.id)
        
        # Item still in queue but marked deleted (lazy deletion)
        # Pop should skip deleted items
        result = empty_priority_queue.pop(timeout=0.1)
        
        assert result is None

    def test_delete_nonexistent_item(self, empty_priority_queue):
        """Test deleting non-existent item doesn't raise."""
        from uuid import uuid4
        
        # Should not raise
        empty_priority_queue.delete(uuid4())

    def test_deleted_item_not_returned(self, empty_priority_queue):
        """Test deleted items are never returned."""
        job1 = Job(name="job1")
        job2 = Job(name="job2")
        
        empty_priority_queue.push(job1)
        empty_priority_queue.push(job2)
        empty_priority_queue.delete(job1.id)
        
        result = empty_priority_queue.pop()
        
        assert result == job2

    def test_delete_maintains_order(self, empty_priority_queue):
        """Test deletion maintains order of remaining items."""
        jobs = [Job(name=f"job_{i}") for i in range(5)]
        
        for job in jobs:
            empty_priority_queue.push(job)
        
        # Delete middle job
        empty_priority_queue.delete(jobs[2].id)
        
        results = []
        while not empty_priority_queue.empty():
            job = empty_priority_queue.pop(timeout=0.1)
            if job:
                results.append(job)
        
        assert len(results) == 4
        assert jobs[2] not in results


class TestBlockingPop:
    """Tests for blocking pop with timeout."""

    def test_pop_with_timeout_empty_queue(self, empty_priority_queue):
        """Test pop with timeout on empty queue."""
        start = time.time()
        result = empty_priority_queue.pop(timeout=0.2)
        elapsed = time.time() - start
        
        assert result is None
        assert elapsed >= 0.2

    def test_pop_with_timeout_returns_item(self, empty_priority_queue):
        """Test pop with timeout returns item when pushed."""
        result = [None]
        
        def pusher():
            time.sleep(0.1)
            empty_priority_queue.push(Job(name="delayed"))
        
        thread = threading.Thread(target=pusher)
        thread.start()
        
        result[0] = empty_priority_queue.pop(timeout=1.0)
        thread.join()
        
        assert result[0] is not None
        assert result[0].name == "delayed"

    def test_pop_without_timeout_blocks(self, empty_priority_queue):
        """Test pop without timeout blocks indefinitely."""
        result = [None]
        popped = threading.Event()
        
        def pusher():
            time.sleep(0.1)
            empty_priority_queue.push(Job(name="blocking"))
        
        def popper():
            result[0] = empty_priority_queue.pop(timeout=2.0)
            popped.set()
        
        push_thread = threading.Thread(target=pusher)
        pop_thread = threading.Thread(target=popper)
        
        pop_thread.start()
        push_thread.start()
        
        popped.wait(timeout=2.0)
        push_thread.join()
        pop_thread.join()
        
        assert result[0] is not None

    def test_pop_zero_timeout_non_blocking(self, empty_priority_queue):
        """Test pop with zero timeout is non-blocking."""
        start = time.time()
        result = empty_priority_queue.pop(timeout=0)
        elapsed = time.time() - start
        
        assert result is None
        assert elapsed < 0.1


class TestQueueOperations:
    """Tests for additional queue operations."""

    def test_peek(self, empty_priority_queue, sample_job):
        """Test peeking without removing."""
        empty_priority_queue.push(sample_job)
        
        peeked = empty_priority_queue.peek()
        
        assert peeked == sample_job
        assert len(empty_priority_queue) == 1  # Still in queue

    def test_peek_empty_queue(self, empty_priority_queue):
        """Test peeking empty queue returns None."""
        assert empty_priority_queue.peek() is None

    def test_clear(self, empty_priority_queue):
        """Test clearing the queue."""
        for i in range(5):
            empty_priority_queue.push(Job(name=f"job_{i}"))
        
        empty_priority_queue.clear()
        
        assert empty_priority_queue.empty() is True
        assert len(empty_priority_queue) == 0

    def test_contains(self, empty_priority_queue, sample_job):
        """Test checking if job is in queue."""
        empty_priority_queue.push(sample_job)
        
        assert sample_job.id in empty_priority_queue

    def test_not_contains(self, empty_priority_queue):
        """Test checking for non-existent job."""
        from uuid import uuid4
        
        assert uuid4() not in empty_priority_queue

    def test_qsize(self, empty_priority_queue):
        """Test getting queue size."""
        for i in range(5):
            empty_priority_queue.push(Job(name=f"job_{i}"))
        
        assert empty_priority_queue.qsize() == 5

    def test_full_with_maxsize(self):
        """Test full() with max size."""
        queue = ThreadSafePriorityQueue(maxsize=2)
        
        assert queue.full() is False
        
        queue.push(Job(name="job1"))
        queue.push(Job(name="job2"))
        
        assert queue.full() is True


class TestQueueIterator:
    """Tests for queue iteration."""

    def test_iterate_queue(self, empty_priority_queue):
        """Test iterating over queue items."""
        jobs = [Job(name=f"job_{i}") for i in range(5)]
        
        for job in jobs:
            empty_priority_queue.push(job)
        
        # Iteration should not remove items
        items = list(empty_priority_queue)
        
        assert len(items) == 5
        assert len(empty_priority_queue) == 5


class TestQueueRepr:
    """Tests for string representation."""

    def test_queue_str(self, empty_priority_queue):
        """Test string representation of queue."""
        empty_priority_queue.push(Job(name="test"))
        
        result = str(empty_priority_queue)
        
        assert len(result) > 0

    def test_queue_repr(self, empty_priority_queue):
        """Test repr of queue."""
        result = repr(empty_priority_queue)
        
        assert "ThreadSafePriorityQueue" in result or "PriorityQueue" in result


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_push_none_raises(self, empty_priority_queue):
        """Test pushing None raises exception."""
        with pytest.raises((TypeError, ValueError)):
            empty_priority_queue.push(None)

    def test_large_number_of_items(self, empty_priority_queue):
        """Test queue with large number of items."""
        count = 10000
        
        for i in range(count):
            empty_priority_queue.push(Job(name=f"job_{i}"))
        
        assert len(empty_priority_queue) == count
        
        # Pop all items
        for _ in range(count):
            job = empty_priority_queue.pop(timeout=0.01)
            assert job is not None
        
        assert empty_priority_queue.empty() is True

    def test_rapid_push_pop(self, empty_priority_queue):
        """Test rapid alternating push/pop."""
        for i in range(1000):
            empty_priority_queue.push(Job(name=f"job_{i}"))
            job = empty_priority_queue.pop(timeout=0.01)
            assert job.name == f"job_{i}"
        
        assert empty_priority_queue.empty() is True