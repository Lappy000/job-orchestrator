"""Synchronous public-boundary regressions; no workers or external services."""

import pytest

from job_orchestrator import (
    InvalidStateTransitionError,
    Job,
    JobState,
    OrchestratorConfig,
    RetryPolicy,
    Scheduler,
    StateMachine,
)


@pytest.fixture
def local_scheduler():
    config = OrchestratorConfig()
    config.dlq.auto_cleanup_days = 0
    scheduler = Scheduler(config)
    scheduler.start()
    try:
        yield scheduler
    finally:
        scheduler.stop()


@pytest.mark.parametrize("state", [
    JobState.CANCELLED, JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT,
])
def test_run_job_rejects_invalid_execution_state_before_side_effects(local_scheduler, state):
    calls = []

    def task():
        calls.append("executed")
        if state == JobState.FAILED:
            raise ValueError("permanent")
        return "original result"

    job = Job(func=task, retry_policy=RetryPolicy(max_retries=0))
    job_id = local_scheduler.submit(job)
    assert local_scheduler.get_next_job(timeout=0.05) is job
    if state == JobState.CANCELLED:
        assert local_scheduler.cancel_job(job_id)
    elif state == JobState.TIMEOUT:
        machine = StateMachine()
        machine.transition(job, JobState.RUNNING)
        machine.transition(job, JobState.TIMEOUT)
    else:
        local_scheduler.run_job(job)
    calls.clear()
    before = job.to_dict()
    stats = local_scheduler.get_stats()

    with pytest.raises(InvalidStateTransitionError):
        local_scheduler.run_job(job)

    assert calls == []
    assert job.to_dict() == before
    assert local_scheduler.get_stats() == stats
    assert local_scheduler.list_jobs(state=state) == [job]


def test_run_job_preserves_typed_retry_policy_through_success(local_scheduler):
    attempts = []
    completed = []
    failed = []

    class TransientError(ValueError):
        pass

    def task():
        attempts.append(job.attempt)
        assert local_scheduler.list_jobs(state=JobState.RUNNING) == [job]
        if job.attempt < 2:
            raise TransientError("try again")
        return {"answer": 42}

    job = Job(func=task, retry_policy=RetryPolicy(
        max_retries=2, base_delay=0, jitter=False, retry_on=(ValueError,),
    ))
    job_id = local_scheduler.submit(job)
    local_scheduler.on_job_complete(lambda job, result: completed.append(result))
    local_scheduler.on_job_failed(lambda job, result: failed.append(result))
    for attempt in range(2):
        assert local_scheduler.get_next_job(timeout=0.05) is job
        result = local_scheduler.run_job(job)
        assert result.success is False
        assert result.error == "try again"
        assert "TransientError" in result.traceback
        assert local_scheduler.get_job_status(job_id) == JobState.SCHEDULED
        assert job.attempt == attempt + 1
        assert job.error is None and job.traceback is None
        assert local_scheduler.list_jobs(state=JobState.SCHEDULED) == [job]
        assert local_scheduler.list_jobs(state=JobState.RUNNING) == []
        assert local_scheduler.list_jobs(state=JobState.FAILED) == []
        assert local_scheduler.get_dlq_entries() == []
        assert completed == [] and failed == []

    assert local_scheduler.get_next_job(timeout=0.05) is job
    result = local_scheduler.run_job(job)
    assert result.success is True
    assert result.result == {"answer": 42}
    assert local_scheduler.get_job(job_id).result == result.result
    assert local_scheduler.list_jobs(state=JobState.COMPLETED) == [job]
    assert local_scheduler.list_jobs(state=JobState.SCHEDULED) == []
    assert attempts == [0, 1, 2]
    assert completed == [result] and failed == []
    assert local_scheduler.get_next_job(timeout=0) is None
    stats = local_scheduler.get_stats()
    assert stats["jobs_retried"] == 2
    assert stats["jobs_completed"] == 1
    assert stats["jobs_failed"] == 0


@pytest.mark.parametrize("error,max_retries,expected_retries", [
    (ValueError("exhausted"), 1, 1),
    (TypeError("not retryable"), 3, 0),
])
def test_run_job_permanent_failure_enters_dlq(
    local_scheduler, error, max_retries, expected_retries,
):
    calls = []
    failures = []
    entries = []

    def task():
        calls.append(job.attempt)
        raise error

    job = Job(func=task, retry_policy=RetryPolicy(
        max_retries=max_retries, base_delay=0, jitter=False, retry_on=(ValueError,),
    ))
    job_id = local_scheduler.submit(job)
    local_scheduler.on_job_failed(lambda job, result: failures.append(result))
    local_scheduler.on_dlq_entry_added(entries.append)
    for _ in range(expected_retries + 1):
        assert local_scheduler.get_next_job(timeout=0.05) is job
        result = local_scheduler.run_job(job)
        assert result.success is False
        assert result.exception is error

    assert len(calls) == expected_retries + 1
    assert job.attempt == expected_retries
    assert local_scheduler.get_job_status(job_id) == JobState.FAILED
    assert local_scheduler.list_jobs(state=JobState.FAILED) == [job]
    assert local_scheduler.list_jobs(state=JobState.RUNNING) == []
    assert local_scheduler.list_jobs(state=JobState.SCHEDULED) == []
    assert local_scheduler.get_next_job(timeout=0) is None
    assert failures == [result]
    entry, = local_scheduler.get_dlq_entries()
    assert entries == [entry]
    assert entry.job_id == job_id
    assert entry.error_type == type(error).__name__
    assert entry.error_message == str(error)
    assert entry.retry_count == expected_retries
    assert entry.stack_trace == result.traceback == job.traceback
    assert job.completed_at == result.completed_at
    stats = local_scheduler.get_stats()
    assert stats["jobs_retried"] == expected_retries
    assert stats["jobs_failed"] == stats["jobs_sent_to_dlq"] == 1


def test_fail_job_owns_retry_transitions_and_records_attempts(local_scheduler, monkeypatch):
    transitions = []
    transition = StateMachine.transition

    def record(machine, job, target, **kwargs):
        previous = job.state
        transition(machine, job, target, **kwargs)
        transitions.append((previous, target))

    monkeypatch.setattr(StateMachine, "transition", record)
    job = Job(retry_policy=RetryPolicy(
        max_retries=1, base_delay=0, jitter=False, retry_on=(ValueError,),
    ))
    job_id = local_scheduler.submit(job)
    for attempt in range(2):
        assert local_scheduler.get_next_job(timeout=0.05) is job
        StateMachine().transition(job, JobState.RUNNING)
        try:
            raise ValueError("worker failure")
        except ValueError as error:
            local_scheduler.fail_job(job_id, error)
        if attempt == 0:
            assert job.attempt == 1
            assert local_scheduler.list_jobs(state=JobState.SCHEDULED) == [job]
            assert local_scheduler.list_jobs(state=JobState.RUNNING) == []
            assert job.error is None and job.traceback is None
            assert local_scheduler.get_dlq_entries() == []

    assert transitions == [
        (JobState.PENDING, JobState.SCHEDULED),
        (JobState.SCHEDULED, JobState.RUNNING),
        (JobState.RUNNING, JobState.RETRYING),
        (JobState.RETRYING, JobState.SCHEDULED),
        (JobState.SCHEDULED, JobState.RUNNING),
        (JobState.RUNNING, JobState.FAILED),
    ]
    assert local_scheduler.list_jobs(state=JobState.FAILED) == [job]
    assert local_scheduler.list_jobs(state=JobState.SCHEDULED) == []
    assert local_scheduler.get_next_job(timeout=0) is None
    entry, = local_scheduler.get_dlq_entries()
    assert entry.error_type == "ValueError"
    assert entry.retry_count == job.attempt == 1
    assert "ValueError: worker failure" in entry.stack_trace
    assert local_scheduler.get_stats()["jobs_sent_to_dlq"] == 1


def test_pending_direct_execution_persists_in_initially_empty_store(local_scheduler):
    seen_states = []

    def task():
        seen_states.append(job.state)
        return "direct result"

    job = Job(func=task)
    assert local_scheduler.list_jobs() == []
    result = local_scheduler.run_job(job)
    assert result.success is True
    assert seen_states == [JobState.RUNNING]
    assert job.state == JobState.COMPLETED
    assert local_scheduler.get_job(str(job.id)) is job
    assert local_scheduler.list_jobs(state=JobState.COMPLETED) == [job]
    assert local_scheduler.get_stats()["job_store"]["by_state"] == {"completed": 1}


def test_cancellation_between_retries_prevents_next_attempt(local_scheduler):
    calls = []

    def task():
        calls.append(job.attempt)
        raise ValueError("retry later")

    job = Job(func=task, retry_policy=RetryPolicy(base_delay=0, jitter=False))
    job_id = local_scheduler.submit(job)
    assert local_scheduler.get_next_job(timeout=0.05) is job
    assert local_scheduler.run_job(job).success is False
    assert job.attempt == 1
    assert local_scheduler.cancel_job(job_id) is True
    assert local_scheduler.get_next_job(timeout=0) is None
    with pytest.raises(InvalidStateTransitionError):
        local_scheduler.run_job(job)
    assert calls == [0]
    assert job.attempt == 1
    assert local_scheduler.list_jobs(state=JobState.CANCELLED) == [job]
    assert local_scheduler.get_dlq_entries() == []
