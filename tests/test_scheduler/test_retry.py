"""
Tests for retry mechanism.

This module tests RetryPolicy, RetryHandler, exponential backoff,
jitter, and retry scheduling.
"""

import pytest
import time
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from job_orchestrator import Job, JobState
from job_orchestrator.scheduler.retry import RetryPolicy, RetryHandler
from job_orchestrator.core.config import RetryConfig


class TestRetryPolicyCreation:
    """Tests for RetryPolicy creation and initialization."""

    def test_retry_policy_default(self, default_retry_policy):
        """Test creating retry policy with defaults."""
        assert default_retry_policy is not None
        assert default_retry_policy.max_retries >= 0

    def test_retry_policy_custom_values(self):
        """Test creating retry policy with custom values."""
        policy = RetryPolicy(
            max_retries=5,
            initial_delay=2.0,
            max_delay=120.0,
            exponential_base=3.0,
            jitter=False,  # Actual API uses boolean jitter
        )
        
        assert policy.max_retries == 5
        assert policy.initial_delay == 2.0
        assert policy.max_delay == 120.0
        assert policy.exponential_base == 3.0
        assert policy.jitter is False

    def test_retry_policy_from_dict(self):
        """Test creating retry policy from dict."""
        data = {
            "max_retries": 5,
            "initial_delay": 2.0,
            "max_delay": 120.0,
        }
        policy = RetryPolicy.from_dict(data)
        
        assert policy.max_retries == 5
        assert policy.initial_delay == 2.0

    def test_retry_policy_disabled(self):
        """Test creating disabled retry policy (0 retries)."""
        policy = RetryPolicy(max_retries=0)
        
        # With 0 max_retries, should_retry always returns False
        assert policy.should_retry(0) is False


class TestExponentialBackoff:
    """Tests for exponential backoff calculation."""

    def test_backoff_first_retry(self, default_retry_policy):
        """Test delay for first retry."""
        delay = default_retry_policy.calculate_delay(retry_count=0)
        
        # First retry should be close to initial_delay (accounting for jitter)
        assert delay >= default_retry_policy.initial_delay * 0.5
        assert delay <= default_retry_policy.initial_delay * 1.5

    def test_backoff_increases_exponentially(self):
        """Test delay increases exponentially with retry count."""
        policy = RetryPolicy(
            initial_delay=1.0,
            exponential_base=2.0,
            jitter=False,  # No jitter for predictable test
            max_delay=1000.0,
        )
        
        delay_0 = policy.calculate_delay(0)  # 1 * 2^0 = 1
        delay_1 = policy.calculate_delay(1)  # 1 * 2^1 = 2
        delay_2 = policy.calculate_delay(2)  # 1 * 2^2 = 4
        delay_3 = policy.calculate_delay(3)  # 1 * 2^3 = 8
        
        assert delay_0 == pytest.approx(1.0)
        assert delay_1 == pytest.approx(2.0)
        assert delay_2 == pytest.approx(4.0)
        assert delay_3 == pytest.approx(8.0)

    def test_backoff_with_base_3(self):
        """Test exponential backoff with base 3."""
        policy = RetryPolicy(
            initial_delay=1.0,
            exponential_base=3.0,
            jitter=False,
            max_delay=1000.0,
        )
        
        delay_0 = policy.calculate_delay(0)  # 1 * 3^0 = 1
        delay_1 = policy.calculate_delay(1)  # 1 * 3^1 = 3
        delay_2 = policy.calculate_delay(2)  # 1 * 3^2 = 9
        
        assert delay_0 == pytest.approx(1.0)
        assert delay_1 == pytest.approx(3.0)
        assert delay_2 == pytest.approx(9.0)


class TestMaxDelayCap:
    """Tests for max delay cap."""

    def test_delay_capped_at_max(self):
        """Test delay never exceeds max_delay."""
        policy = RetryPolicy(
            initial_delay=1.0,
            exponential_base=2.0,
            max_delay=10.0,
            jitter=False,
        )
        
        # At retry 10, would be 1024 without cap
        delay = policy.calculate_delay(10)
        
        assert delay <= 10.0

    def test_delay_equals_max_at_high_retries(self):
        """Test delay equals max at high retry counts."""
        policy = RetryPolicy(
            initial_delay=1.0,
            exponential_base=2.0,
            max_delay=60.0,
            jitter=False,
        )
        
        # At retry 6, would be 64 > 60
        delay = policy.calculate_delay(6)
        
        assert delay == pytest.approx(60.0)


class TestJitter:
    """Tests for jitter application."""

    def test_jitter_applied(self):
        """Test jitter is applied to delay."""
        policy = RetryPolicy(
            initial_delay=10.0,
            exponential_base=2.0,
            max_delay=1000.0,
            jitter=True,
        )
        
        # Get multiple delays and check they vary
        delays = [policy.calculate_delay(0) for _ in range(100)]
        
        # With jitter, we should see variation
        min_delay = min(delays)
        max_delay = max(delays)
        
        # Delays should vary within jitter range (0.5 to 1.5 factor)
        assert min_delay >= 10.0 * 0.5  # -50% jitter
        assert max_delay <= 10.0 * 1.5  # +50% jitter
        assert max_delay > min_delay  # Should have variation

    def test_jitter_disabled_means_no_variation(self):
        """Test disabled jitter produces consistent delays."""
        policy = RetryPolicy(
            initial_delay=5.0,
            exponential_base=2.0,
            jitter=False,
        )
        
        delays = [policy.calculate_delay(0) for _ in range(10)]
        
        assert all(d == delays[0] for d in delays)

    def test_jitter_bounds(self):
        """Test jitter stays within bounds."""
        policy = RetryPolicy(
            initial_delay=100.0,
            jitter=True,
            max_delay=1000.0,
        )
        
        for _ in range(100):
            delay = policy.calculate_delay(0)
            # Should be within ±50% (jitter factor 0.5 to 1.5)
            assert delay >= 50.0
            assert delay <= 150.0


class TestRetryHandlerCreation:
    """Tests for RetryHandler creation."""

    def test_retry_handler_creation(self, retry_handler):
        """Test creating a retry handler."""
        assert retry_handler is not None
        assert isinstance(retry_handler, RetryHandler)

    def test_retry_handler_with_policy(self, default_retry_policy):
        """Test creating handler with specific policy."""
        handler = RetryHandler(default_policy=default_retry_policy)
        
        assert handler.default_policy == default_retry_policy


class TestScheduleRetry:
    """Tests for scheduling retries."""

    def test_schedule_retry_increments_attempt(self, retry_handler, sample_job, scheduler):
        """Test scheduling retry increments attempt count."""
        initial_attempt = sample_job.attempt
        sample_job.state = JobState.FAILED
        
        retry_handler.schedule_retry(sample_job, scheduler)
        
        assert sample_job.attempt == initial_attempt + 1

    def test_schedule_retry_changes_state(self, retry_handler, sample_job, scheduler):
        """Test scheduling retry changes state from FAILED."""
        sample_job.state = JobState.FAILED
        
        retry_handler.schedule_retry(sample_job, scheduler)
        
        # After submit, state may be SCHEDULED or RETRYING depending on scheduler
        assert sample_job.state != JobState.FAILED

    def test_schedule_retry_sets_scheduled_time(self, retry_handler, sample_job, scheduler):
        """Test scheduling retry sets future scheduled time."""
        sample_job.state = JobState.FAILED
        now = datetime.utcnow()
        
        retry_handler.schedule_retry(sample_job, scheduler)
        
        assert sample_job.scheduled_at >= now

    def test_schedule_retry_returns_true(self, retry_handler, sample_job, scheduler):
        """Test schedule_retry returns True on success."""
        sample_job.state = JobState.FAILED
        
        result = retry_handler.schedule_retry(sample_job, scheduler)
        
        assert result is True


class TestMaxRetriesExceeded:
    """Tests for max retries exceeded handling."""

    def test_schedule_retry_fails_at_max(self, retry_handler, sample_job, scheduler):
        """Test schedule_retry returns False after max retries."""
        sample_job.state = JobState.FAILED
        sample_job.attempt = retry_handler.default_policy.max_retries
        
        result = retry_handler.schedule_retry(sample_job, scheduler)
        
        assert result is False

    def test_should_retry_respects_max(self, retry_handler, sample_job):
        """Test should_retry respects max retries."""
        sample_job.state = JobState.FAILED
        sample_job.attempt = 0
        
        assert retry_handler.should_retry(sample_job) is True
        
        sample_job.attempt = retry_handler.default_policy.max_retries
        
        assert retry_handler.should_retry(sample_job) is False


class TestExceptionTypeFiltering:
    """Tests for exception type filtering."""

    def test_retry_on_specific_exceptions(self):
        """Test retry only on specific exception types."""
        policy = RetryPolicy(
            max_retries=3,
            retry_on=(ValueError, TypeError),  # Tuple, not list
        )
        
        # Policy should_retry should check exception type
        assert policy.should_retry(0, ValueError("test")) is True
        assert policy.should_retry(0, KeyError("test")) is False

    def test_retry_on_all_exceptions_by_default(self, retry_handler, sample_job):
        """Test retry on all exceptions by default."""
        sample_job.state = JobState.FAILED
        sample_job.attempt = 0
        
        assert retry_handler.should_retry(sample_job) is True


class TestRetryWithBackoff:
    """Tests for retry with backoff."""

    def test_retry_delay_increases(self, retry_handler, sample_job):
        """Test retry delay increases with each attempt."""
        sample_job.state = JobState.FAILED
        
        delays = []
        for i in range(4):
            sample_job.attempt = i
            delay = retry_handler.get_retry_delay(sample_job)
            delays.append(delay)
        
        # Each delay should be greater than or equal to previous
        # (accounting for jitter)
        for i in range(1, len(delays)):
            assert delays[i] >= delays[i-1] * 0.3  # Allow for jitter

    def test_first_retry_immediate_option(self):
        """Test option for immediate first retry."""
        policy = RetryPolicy(
            max_retries=3,
            initial_delay=0.0,  # Immediate first retry
            jitter=False,
        )
        
        delay = policy.calculate_delay(0)
        
        assert delay == 0.0


class TestRetryPolicyString:
    """Tests for retry policy string representation."""

    def test_retry_policy_to_dict(self, default_retry_policy):
        """Test serializing retry policy to dict."""
        result = default_retry_policy.to_dict()
        
        assert isinstance(result, dict)
        assert "max_retries" in result

    def test_retry_policy_repr(self, default_retry_policy):
        """Test repr of retry policy."""
        result = repr(default_retry_policy)
        
        assert len(result) > 0


class TestRetryEdgeCases:
    """Tests for edge cases in retry logic."""

    def test_remaining_retries(self, retry_handler, sample_job):
        """Test getting remaining retries."""
        sample_job.attempt = 0
        remaining = retry_handler.get_remaining_retries(sample_job)
        
        assert remaining == retry_handler.default_policy.max_retries
        
        sample_job.attempt = retry_handler.default_policy.max_retries
        remaining = retry_handler.get_remaining_retries(sample_job)
        
        assert remaining == 0

    def test_prepare_for_retry(self, retry_handler, sample_job):
        """Test preparing job for retry without submitting."""
        sample_job.state = JobState.FAILED
        initial_attempt = sample_job.attempt
        
        delay, scheduled_at = retry_handler.prepare_for_retry(sample_job)
        
        assert delay > 0 or sample_job.attempt > initial_attempt
        assert scheduled_at > datetime.utcnow() - timedelta(seconds=1)

    def test_get_policy_for_job(self, retry_handler, sample_job):
        """Test getting policy for specific job."""
        policy = retry_handler.get_policy_for_job(sample_job)
        
        assert policy is not None
        assert policy.max_retries >= 0

    def test_create_retry_policy_factory(self, retry_handler):
        """Test factory method for creating retry policies."""
        policy = retry_handler.create_retry_policy(
            max_retries=10,
            initial_delay=5.0,
        )
        
        assert policy.max_retries == 10
        assert policy.initial_delay == 5.0


class TestPreDefinedPolicies:
    """Tests for pre-defined retry policies."""

    def test_aggressive_retry_policy(self):
        """Test aggressive retry policy exists."""
        from job_orchestrator.scheduler.retry import AGGRESSIVE_RETRY
        
        assert AGGRESSIVE_RETRY.max_retries > 5
        assert AGGRESSIVE_RETRY.initial_delay < 1.0

    def test_conservative_retry_policy(self):
        """Test conservative retry policy exists."""
        from job_orchestrator.scheduler.retry import CONSERVATIVE_RETRY
        
        assert CONSERVATIVE_RETRY.max_retries <= 5
        assert CONSERVATIVE_RETRY.initial_delay >= 1.0

    def test_no_retry_policy(self):
        """Test no-retry policy exists."""
        from job_orchestrator.scheduler.retry import NO_RETRY
        
        assert NO_RETRY.max_retries == 0

    def test_linear_backoff_policy(self):
        """Test linear backoff policy exists."""
        from job_orchestrator.scheduler.retry import LINEAR_BACKOFF
        
        assert LINEAR_BACKOFF.exponential_base == 1.0