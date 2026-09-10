"""Regressions for replaced queue entries being returned more than once."""

from job_orchestrator import Job, JobPriority, ThreadSafePriorityQueue


def test_priority_update_does_not_duplicate_job():
    queue = ThreadSafePriorityQueue()
    job = Job(name="charge-once", priority=JobPriority.NORMAL)
    queue.push(job)
    assert queue.update_priority(job.id, JobPriority.HIGH)
    assert queue.pop_nowait() is job
    assert queue.pop_nowait() is None
    assert len(queue) == 0


def test_rescheduling_discards_the_old_ready_entry():
    from datetime import datetime, timedelta, timezone

    queue = ThreadSafePriorityQueue()
    job = Job(name="rescheduled")
    queue.push(job)
    assert queue.reschedule(job.id, datetime.now(timezone.utc) + timedelta(hours=1))
    assert queue.pop_nowait() is None
    assert queue.peek() is job
    assert queue.get(job.id) is job


def test_remove_after_multiple_replacements_does_not_resurrect_job():
    queue = ThreadSafePriorityQueue()
    job = Job(name="removed")
    for _ in range(3):
        assert queue.push(job)
    assert queue.remove(job.id)
    assert queue.pop_nowait() is None
    assert queue.peek() is None
    assert len(queue) == 0


def test_pop_nowait_does_not_wait_for_future_job():
    from datetime import datetime, timedelta, timezone

    queue = ThreadSafePriorityQueue()
    job = Job(name="later", scheduled_at=datetime.now(timezone.utc) + timedelta(seconds=1))
    queue.push(job)
    assert queue.pop_nowait() is None
    assert queue.get(job.id) is job


def test_ready_low_priority_job_wakes_before_future_high_priority_job():
    from datetime import datetime, timedelta, timezone

    queue = ThreadSafePriorityQueue()
    now = datetime.now(timezone.utc)
    later = Job(name="later", priority=JobPriority.HIGH, scheduled_at=now + timedelta(seconds=2))
    sooner = Job(name="sooner", priority=JobPriority.LOW, scheduled_at=now + timedelta(seconds=0.02))
    queue.push(later)
    queue.push(sooner)
    assert queue.pop(timeout=0.3) is sooner


def test_peek_ready_sees_lower_priority_ready_job_without_consuming():
    from datetime import datetime, timedelta, timezone

    queue = ThreadSafePriorityQueue()
    later = Job(priority=JobPriority.HIGH, scheduled_at=datetime.now(timezone.utc) + timedelta(hours=1))
    ready = Job(priority=JobPriority.LOW)
    queue.push(later)
    queue.push(ready)
    assert queue.peek_ready() is ready
    assert len(queue) == 2


def test_replacing_existing_job_is_allowed_at_capacity():
    queue = ThreadSafePriorityQueue(max_size=1)
    job = Job(name="original")
    replacement = Job(id=job.id, name="replacement")
    assert queue.push(job)
    assert queue.push(replacement)
    assert queue.pop_nowait() is replacement
    assert queue.pop_nowait() is None
