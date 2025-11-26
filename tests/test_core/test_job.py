"""
Tests for the Job model and related classes.

This module tests the Job dataclass, JobState, JobPriority enums,
and RetryPolicy configuration.
"""

import pytest
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from job_orchestrator import Job, JobState, JobPriority, RetryPolicy


class TestJobCreation:
    """Tests for basic job creation and initialization."""

    def test_job_creation_minimal(self):
        """Test creating a job with minimal arguments."""
        job = Job(name="test_job")
        
        assert job.name == "test_job"
        assert isinstance(job.id, UUID)
        assert job.state == JobState.PENDING
        assert job.priority == JobPriority.NORMAL
        assert job.attempt == 0
        assert job.func is None
        assert job.args == ()
        assert job.kwargs == {}

    def test_job_creation_with_function(self, sample_job):
        """Test creating a job with a function."""
        assert sample_job.name == "test_job"
        assert sample_job.func is not None
        assert sample_job.args == (1, 2)

    def test_job_creation_with_priority(self, high_priority_job):
        """Test creating a job with specific priority."""
        assert high_priority_job.priority == JobPriority.HIGH

    def test_job_creation_with_scheduled_time(self, scheduled_job):
        """Test creating a job with scheduled execution time."""
        assert scheduled_job.scheduled_at is not None
        assert scheduled_job.scheduled_at > datetime.utcnow()

    def test_job_creation_with_timeout(self, job_with_timeout):
        """Test creating a job with a timeout."""
        assert job_with_timeout.timeout == 0.1

    def test_job_creation_with_metadata(self, job_with_metadata):
        """Test creating a job with tags and metadata."""
        assert job_with_metadata.tags == {"environment": "test", "team": "backend"}
        assert job_with_metadata.metadata == {"source": "pytest", "version": "1.0"}

    def test_job_has_unique_id(self):
        """Test that each job has a unique ID."""
        jobs = [Job(name="test") for _ in range(10)]
        ids = [job.id for job in jobs]
        assert len(set(ids)) == 10

    def test_job_created_at_is_set(self):
        """Test that created_at timestamp is automatically set."""
        before = datetime.utcnow()
        job = Job(name="test")
        after = datetime.utcnow()
        
        assert before <= job.created_at <= after


class TestJobSerialization:
    """Tests for job serialization and deserialization."""

    def test_job_to_dict(self, sample_job):
        """Test serializing a job to dictionary."""
        data = sample_job.to_dict()
        
        assert data["name"] == "test_job"
        assert data["id"] == str(sample_job.id)
        assert data["state"] == "pending"
        assert data["priority"] == JobPriority.NORMAL.value
        assert data["args"] == [1, 2]
        assert data["attempt"] == 0

    def test_job_from_dict(self, sample_job):
        """Test deserializing a job from dictionary."""
        data = sample_job.to_dict()
        restored = Job.from_dict(data)
        
        assert restored.id == sample_job.id
        assert restored.name == sample_job.name
        assert restored.state == sample_job.state
        assert restored.priority == sample_job.priority
        assert restored.args == sample_job.args

    def test_job_roundtrip_serialization(self, job_with_metadata):
        """Test that serialization and deserialization preserve all fields."""
        original = job_with_metadata
        data = original.to_dict()
        restored = Job.from_dict(data)
        
        assert restored.id == original.id
        assert restored.name == original.name
        assert restored.tags == original.tags
        assert restored.metadata == original.metadata

    def test_job_serialize_with_scheduled_at(self, scheduled_job):
        """Test serializing a job with scheduled_at timestamp."""
        data = scheduled_job.to_dict()
        restored = Job.from_dict(data)
        
        # Check timestamp is preserved (within 1 second tolerance)
        assert abs(
            (restored.scheduled_at - scheduled_job.scheduled_at).total_seconds()
        ) < 1

    def test_job_serialize_with_depends_on(self):
        """Test serializing a job with dependencies."""
        dep_ids = [uuid4(), uuid4()]
        job = Job(name="dependent_job", depends_on=dep_ids)
        
        data = job.to_dict()
        restored = Job.from_dict(data)
        
        assert restored.depends_on == dep_ids

    def test_job_serialize_result(self):
        """Test serializing a job with a result."""
        job = Job(name="completed_job")
        job.result = {"status": "success", "count": 42}
        
        data = job.to_dict()
        restored = Job.from_dict(data)
        
        assert restored.result == {"status": "success", "count": 42}


class TestJobPriorityOrdering:
    """Tests for job priority-based ordering."""

    def test_job_comparison_by_priority(self):
        """Test that jobs are ordered by priority."""
        critical = Job(name="critical", priority=JobPriority.CRITICAL)
        high = Job(name="high", priority=JobPriority.HIGH)
        normal = Job(name="normal", priority=JobPriority.NORMAL)
        low = Job(name="low", priority=JobPriority.LOW)
        background = Job(name="background", priority=JobPriority.BACKGROUND)
        
        assert critical < high
        assert high < normal
        assert normal < low
        assert low < background

    def test_job_comparison_same_priority_by_time(self):
        """Test that jobs with same priority are ordered by scheduled time."""
        earlier = Job(
            name="earlier",
            priority=JobPriority.NORMAL,
            scheduled_at=datetime.utcnow()
        )
        later = Job(
            name="later",
            priority=JobPriority.NORMAL,
            scheduled_at=datetime.utcnow() + timedelta(hours=1)
        )
        
        assert earlier < later

    def test_job_sorting(self):
        """Test sorting a list of jobs by priority."""
        jobs = [
            Job(name="low", priority=JobPriority.LOW),
            Job(name="critical", priority=JobPriority.CRITICAL),
            Job(name="normal", priority=JobPriority.NORMAL),
            Job(name="high", priority=JobPriority.HIGH),
        ]
        
        sorted_jobs = sorted(jobs)
        priorities = [j.priority for j in sorted_jobs]
        
        assert priorities == [
            JobPriority.CRITICAL,
            JobPriority.HIGH,
            JobPriority.NORMAL,
            JobPriority.LOW,
        ]

    def test_job_priority_values(self):
        """Test that priority enum values are correctly ordered."""
        assert JobPriority.CRITICAL.value < JobPriority.HIGH.value
        assert JobPriority.HIGH.value < JobPriority.NORMAL.value
        assert JobPriority.NORMAL.value < JobPriority.LOW.value
        assert JobPriority.LOW.value < JobPriority.BACKGROUND.value


class TestJobStateProperties:
    """Tests for job state-related properties."""

    def test_job_is_terminal_completed(self):
        """Test is_terminal property for COMPLETED state."""
        job = Job(name="test")
        job.state = JobState.COMPLETED
        
        assert job.is_terminal is True
        assert job.is_active is False

    def test_job_is_terminal_failed(self):
        """Test is_terminal property for FAILED state."""
        job = Job(name="test")
        job.state = JobState.FAILED
        
        assert job.is_terminal is True
        assert job.is_active is False

    def test_job_is_terminal_cancelled(self):
        """Test is_terminal property for CANCELLED state."""
        job = Job(name="test")
        job.state = JobState.CANCELLED
        
        assert job.is_terminal is True
        assert job.is_active is False

    def test_job_is_active_pending(self):
        """Test is_active property for PENDING state."""
        job = Job(name="test")
        job.state = JobState.PENDING
        
        assert job.is_active is True
        assert job.is_terminal is False

    def test_job_is_active_running(self):
        """Test is_active property for RUNNING state."""
        job = Job(name="test")
        job.state = JobState.RUNNING
        
        assert job.is_active is True
        assert job.is_terminal is False

    def test_job_is_active_retrying(self):
        """Test is_active property for RETRYING state."""
        job = Job(name="test")
        job.state = JobState.RETRYING
        
        assert job.is_active is True
        assert job.is_terminal is False

    def test_job_can_retry_with_attempts_remaining(self):
        """Test can_retry property when retries are remaining."""
        job = Job(
            name="test",
            retry_policy=RetryPolicy(max_retries=3)
        )
        job.attempt = 0
        
        assert job.can_retry is True

    def test_job_can_retry_exhausted(self):
        """Test can_retry property when retries are exhausted."""
        job = Job(
            name="test",
            retry_policy=RetryPolicy(max_retries=3)
        )
        job.attempt = 3
        
        assert job.can_retry is False

    def test_job_can_retry_no_retries_configured(self):
        """Test can_retry property when no retries are configured."""
        job = Job(
            name="test",
            retry_policy=RetryPolicy(max_retries=0)
        )
        job.attempt = 0
        
        assert job.can_retry is False


class TestJobExecutionTime:
    """Tests for job execution time calculation."""

    def test_execution_time_not_started(self):
        """Test execution_time is None when job hasn't started."""
        job = Job(name="test")
        
        assert job.execution_time is None

    def test_execution_time_running(self):
        """Test execution_time for a running job."""
        job = Job(name="test")
        job.started_at = datetime.utcnow() - timedelta(seconds=10)
        
        execution_time = job.execution_time
        assert execution_time is not None
        assert 9.5 < execution_time < 10.5  # Allow some tolerance

    def test_execution_time_completed(self):
        """Test execution_time for a completed job."""
        job = Job(name="test")
        job.started_at = datetime.utcnow() - timedelta(seconds=30)
        job.completed_at = datetime.utcnow() - timedelta(seconds=10)
        
        execution_time = job.execution_time
        assert execution_time is not None
        assert 19.5 < execution_time < 20.5  # Should be ~20 seconds


class TestJobEquality:
    """Tests for job equality and hashing."""

    def test_job_equality_same_id(self):
        """Test that jobs with the same ID are equal."""
        id = uuid4()
        job1 = Job(name="job1")
        job1.id = id
        job2 = Job(name="job2")
        job2.id = id
        
        assert job1 == job2

    def test_job_equality_different_id(self):
        """Test that jobs with different IDs are not equal."""
        job1 = Job(name="test")
        job2 = Job(name="test")
        
        assert job1 != job2

    def test_job_hash(self):
        """Test that job hash is based on ID."""
        job = Job(name="test")
        
        assert hash(job) == hash(job.id)

    def test_job_in_set(self):
        """Test that jobs can be added to a set."""
        job1 = Job(name="job1")
        job2 = Job(name="job2")
        job_set = {job1, job2, job1}  # job1 added twice
        
        assert len(job_set) == 2


class TestJobCopy:
    """Tests for job copy functionality."""

    def test_job_copy_creates_new_id(self):
        """Test that copying a job creates a new ID."""
        original = Job(name="original", priority=JobPriority.HIGH)
        copy = original.copy()
        
        assert copy.id != original.id

    def test_job_copy_preserves_config(self):
        """Test that copying preserves job configuration."""
        original = Job(
            name="original",
            priority=JobPriority.HIGH,
            timeout=30.0,
            tags={"env": "test"},
            retry_policy=RetryPolicy(max_retries=5)
        )
        
        copy = original.copy()
        
        assert copy.name == original.name
        assert copy.priority == original.priority
        assert copy.timeout == original.timeout
        assert copy.tags == original.tags
        assert copy.retry_policy.max_retries == original.retry_policy.max_retries

    def test_job_copy_resets_state(self):
        """Test that copying resets job state to PENDING."""
        original = Job(name="original")
        original.state = JobState.COMPLETED
        original.started_at = datetime.utcnow()
        original.completed_at = datetime.utcnow()
        
        copy = original.copy()
        
        assert copy.state == JobState.PENDING
        # Note: copy starts fresh, started_at/completed_at are not copied


class TestRetryPolicy:
    """Tests for RetryPolicy functionality."""

    def test_retry_policy_default_values(self):
        """Test default retry policy values."""
        policy = RetryPolicy()
        
        assert policy.max_retries == 3
        assert policy.base_delay == 1.0
        assert policy.max_delay == 300.0
        assert policy.exponential_base == 2.0
        assert policy.jitter is True

    def test_retry_policy_calculate_delay_without_jitter(self):
        """Test delay calculation without jitter."""
        policy = RetryPolicy(
            base_delay=1.0,
            exponential_base=2.0,
            jitter=False
        )
        
        assert policy.calculate_delay(0) == 1.0   # 1 * 2^0 = 1
        assert policy.calculate_delay(1) == 2.0   # 1 * 2^1 = 2
        assert policy.calculate_delay(2) == 4.0   # 1 * 2^2 = 4
        assert policy.calculate_delay(3) == 8.0   # 1 * 2^3 = 8

    def test_retry_policy_calculate_delay_with_jitter(self):
        """Test delay calculation with jitter adds randomness."""
        policy = RetryPolicy(base_delay=1.0, jitter=True)
        
        delays = [policy.calculate_delay(1) for _ in range(10)]
        
        # With jitter, delays should vary
        assert len(set(delays)) > 1

    def test_retry_policy_max_delay_cap(self):
        """Test that delay never exceeds max_delay."""
        policy = RetryPolicy(
            base_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            jitter=False
        )
        
        # At attempt 10, delay would be 1024 but should be capped
        assert policy.calculate_delay(10) == 10.0

    def test_retry_policy_should_retry_on_exception_type(self):
        """Test should_retry filters by exception type."""
        policy = RetryPolicy(retry_on=(ValueError, TypeError))
        
        assert policy.should_retry(ValueError("test")) is True
        assert policy.should_retry(TypeError("test")) is True
        assert policy.should_retry(KeyError("test")) is False

    def test_retry_policy_serialization(self):
        """Test retry policy serialization."""
        policy = RetryPolicy(
            max_retries=5,
            base_delay=2.0,
            max_delay=100.0
        )
        
        data = policy.to_dict()
        restored = RetryPolicy.from_dict(data)
        
        assert restored.max_retries == policy.max_retries
        assert restored.base_delay == policy.base_delay
        assert restored.max_delay == policy.max_delay


class TestJobStateEnum:
    """Tests for JobState enum."""

    def test_all_states_exist(self):
        """Test that all expected states exist."""
        states = {s.value for s in JobState}
        
        expected = {
            "pending", "scheduled", "running", "completed",
            "failed", "retrying", "cancelled", "timeout"
        }
        
        assert states == expected

    def test_state_values(self):
        """Test that state values are correct strings."""
        assert JobState.PENDING.value == "pending"
        assert JobState.COMPLETED.value == "completed"
        assert JobState.FAILED.value == "failed"


class TestJobPriorityEnum:
    """Tests for JobPriority enum."""

    def test_all_priorities_exist(self):
        """Test that all expected priorities exist."""
        priorities = {p.name for p in JobPriority}
        
        expected = {"CRITICAL", "HIGH", "NORMAL", "LOW", "BACKGROUND"}
        
        assert priorities == expected

    def test_priority_numeric_values(self):
        """Test that priority values are correctly ordered numerically."""
        assert JobPriority.CRITICAL.value == 0
        assert JobPriority.HIGH.value == 1
        assert JobPriority.NORMAL.value == 2
        assert JobPriority.LOW.value == 3
        assert JobPriority.BACKGROUND.value == 4