"""Both exported retry policies must honor their documented maximum delay."""

import random

import pytest

from job_orchestrator import RetryPolicy as PublicRetryPolicy
from job_orchestrator.scheduler.retry import RetryPolicy as SchedulerRetryPolicy


@pytest.mark.parametrize("policy", [
    PublicRetryPolicy(base_delay=1, max_delay=10, jitter=True),
    SchedulerRetryPolicy(initial_delay=1, max_delay=10, jitter=True),
])
def test_positive_jitter_cannot_exceed_max_delay(monkeypatch, policy):
    monkeypatch.setattr(random, "random", lambda: 0.999)
    assert policy.calculate_delay(10) == 10


@pytest.mark.parametrize("policy_type,delay_key", [
    (PublicRetryPolicy, "base_delay"), (SchedulerRetryPolicy, "initial_delay"),
])
@pytest.mark.parametrize("sample,attempt,expected", [
    (0.0, 0, 1.0), (0.5, 0, 2.0), (0.999, 0, 2.998),
    (0.0, 10, 5.0), (0.5, 10, 10.0),
])
def test_jitter_retains_lower_and_uncapped_delays(
    monkeypatch, policy_type, delay_key, sample, attempt, expected,
):
    monkeypatch.setattr(random, "random", lambda: sample)
    policy = policy_type(**{delay_key: 2, "max_delay": 10, "jitter": True})
    assert policy.calculate_delay(attempt) == pytest.approx(expected)
