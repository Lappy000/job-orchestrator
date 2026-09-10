"""Contract tests for the shipped Python API and CLI, not the placeholder api package."""

from datetime import datetime, timedelta

import pytest

from job_orchestrator import (
    Job,
    JobAlreadyExistsError,
    JobPriority,
    JobState,
    OrchestratorConfig,
    QueueFullError,
    Scheduler,
    __version__,
)
from job_orchestrator.cli.main import cli


@pytest.fixture
def public_scheduler():
    config = OrchestratorConfig()
    config.dlq.auto_cleanup_days = 0
    scheduler = Scheduler(config)
    try:
        yield scheduler
    finally:
        scheduler.start()
        scheduler.stop()


@pytest.mark.parametrize("value", [0, False, None, {"answer": 42}])
def test_submit_execute_returns_and_persists_result(public_scheduler, value):
    job = Job(name="result", func=lambda supplied: supplied, args=(value,))
    job_id = public_scheduler.submit(job)
    assert job_id == str(job.id)
    assert public_scheduler.get_job_status(job_id) == JobState.SCHEDULED
    public_scheduler.start()
    assert public_scheduler.get_next_job(timeout=0.05) is job

    result = public_scheduler.run_job(job)

    assert result.success is True
    assert result.result == value
    assert result.job_id == job.id
    assert public_scheduler.get_job(job_id).result == value
    assert public_scheduler.list_jobs(state=JobState.COMPLETED) == [job]
    assert public_scheduler.get_next_job(timeout=0) is None


def test_readiness_respects_lifecycle_and_future_schedule(public_scheduler):
    future = Job(scheduled_at=datetime.utcnow() + timedelta(days=1))
    ready = Job()
    public_scheduler.submit(future)
    public_scheduler.submit(ready)
    assert public_scheduler.is_running is False
    assert public_scheduler.get_next_job(timeout=0) is None
    public_scheduler.start()
    assert public_scheduler.is_running is True
    assert public_scheduler.get_next_job(timeout=0.05) is ready
    assert public_scheduler.get_next_job(timeout=0) is None
    assert public_scheduler.get_job_status(str(future.id)) == JobState.SCHEDULED
    public_scheduler.stop()
    assert public_scheduler.is_running is False


def test_public_defaults_are_independent():
    first, second = Job(), Job()
    assert first.state == JobState.PENDING
    assert first.priority == JobPriority.NORMAL
    assert first.attempt == 0
    assert first.args == () and first.kwargs == {}
    assert first.retry_policy.max_retries == 3
    first.kwargs["changed"] = True
    first.retry_policy.max_retries = 0
    assert second.kwargs == {}
    assert second.retry_policy.max_retries == 3
    config = OrchestratorConfig()
    assert config.queue.max_size is None
    assert config.retry.max_retries == 3


def test_valid_identifier_lookup_and_duplicate_rejection(public_scheduler):
    job = Job(name="valid_input")
    job_id = public_scheduler.submit(job)
    assert public_scheduler.get_job(job_id) is job
    with pytest.raises(JobAlreadyExistsError):
        public_scheduler.submit(job)
    assert public_scheduler.list_jobs() == [job]
    assert public_scheduler.get_stats()["jobs_submitted"] == 1


@pytest.mark.parametrize("identifier,error", [
    ("", ValueError), (None, TypeError), ("invalid!", ValueError),
])
def test_invalid_identifiers_are_rejected(public_scheduler, identifier, error):
    before = public_scheduler.get_stats()
    with pytest.raises(error):
        public_scheduler.get_job_status(identifier)
    assert public_scheduler.get_stats() == before


def test_queue_capacity_rejection_does_not_store_job():
    config = OrchestratorConfig()
    config.queue.max_size = 1
    config.dlq.auto_cleanup_days = 0
    scheduler = Scheduler(config)
    original, rejected = Job(), Job()
    scheduler.submit(original)
    with pytest.raises(QueueFullError):
        scheduler.submit(rejected)
    assert rejected.state == JobState.PENDING
    assert scheduler.get_job(str(rejected.id)) is None
    assert scheduler.list_jobs() == [original]


def test_cancel_removes_ready_job_without_execution(public_scheduler):
    calls = []
    job = Job(func=lambda: calls.append("executed"))
    job_id = public_scheduler.submit(job)
    assert public_scheduler.cancel_job(job_id) is True
    assert public_scheduler.cancel_job(job_id) is False
    public_scheduler.start()
    assert public_scheduler.get_next_job(timeout=0) is None
    assert public_scheduler.list_jobs(state=JobState.CANCELLED) == [job]
    assert job.completed_at is not None
    assert calls == []


def test_cli_version_and_help(capsys):
    assert cli(["version"]) == 0
    assert f"job-orchestrator {__version__}" in capsys.readouterr().out
    assert cli([]) == 0
    assert "job-orch" in capsys.readouterr().out


def test_cli_rejects_unknown_command(capsys):
    with pytest.raises(SystemExit) as exc:
        cli(["not-a-command"])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err
