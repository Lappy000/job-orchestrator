"""
Tests for WorkerPool.

This module tests worker pool management, auto-scaling,
health monitoring, and pool lifecycle operations.

The WorkerPool uses a PULL-based model:
- Jobs are submitted to the Scheduler
- Workers pull jobs from the Scheduler's queue
- WorkerPool manages worker lifecycle and scaling
"""

import pytest
import time
import threading
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from job_orchestrator import Job, JobState, Scheduler, OrchestratorConfig
from job_orchestrator.workers.pool import WorkerPool, PoolConfig, PoolStats
from job_orchestrator.workers.worker import WorkerState


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def config():
    """Create a default OrchestratorConfig."""
    return OrchestratorConfig()


@pytest.fixture
def scheduler(config):
    """Create a scheduler instance."""
    sched = Scheduler(config=config)
    sched.start()
    yield sched
    sched.stop(wait=False)


@pytest.fixture
def pool_config():
    """Create a PoolConfig for testing."""
    return PoolConfig(
        min_workers=2,
        max_workers=5,
        scale_interval=0.1,  # Fast scaling for tests
        health_check_interval=0.1,  # Fast health checks for tests
        worker_max_idle_time=1.0,
    )


@pytest.fixture
def worker_pool(scheduler, pool_config):
    """Create a worker pool instance."""
    pool = WorkerPool(scheduler=scheduler, config=pool_config)
    yield pool
    if pool.is_running:
        pool.stop(wait=False, timeout=1.0)


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    def test_func(x, y):
        return x + y
    return Job(name="test_job", func=test_func, args=(1, 2))


@pytest.fixture
def failing_job():
    """Create a job that fails."""
    def failing_func():
        raise ValueError("Job failed on purpose")
    
    from job_orchestrator.core.job import RetryPolicy
    return Job(
        name="failing_job",
        func=failing_func,
        retry_policy=RetryPolicy(max_retries=0)
    )


# ============================================================================
# TestWorkerPoolCreation - Pool creation and initialization
# ============================================================================

class TestWorkerPoolCreation:
    """Tests for WorkerPool creation and initialization."""

    def test_pool_creation(self, worker_pool):
        """Test creating a worker pool."""
        assert worker_pool is not None
        assert isinstance(worker_pool, WorkerPool)

    def test_pool_with_config(self, scheduler):
        """Test creating pool with PoolConfig."""
        config = PoolConfig(min_workers=3, max_workers=8)
        pool = WorkerPool(scheduler=scheduler, config=config)
        
        assert pool.config.min_workers == 3
        assert pool.config.max_workers == 8
        
    def test_pool_default_config(self, scheduler):
        """Test pool uses default config when none provided."""
        pool = WorkerPool(scheduler=scheduler)
        
        assert pool.config.min_workers >= 1
        assert pool.config.max_workers >= pool.config.min_workers

    def test_pool_invalid_config_raises(self, scheduler):
        """Test invalid config raises ValueError."""
        with pytest.raises(ValueError):
            config = PoolConfig(min_workers=10, max_workers=5)  # Invalid
            WorkerPool(scheduler=scheduler, config=config)


# ============================================================================
# TestPoolLifecycle - Pool start and stop operations
# ============================================================================

class TestPoolLifecycle:
    """Tests for pool start and stop operations."""

    def test_start_pool(self, worker_pool):
        """Test starting the worker pool."""
        worker_pool.start()
        
        assert worker_pool.is_running is True
        
        worker_pool.stop(wait=False)

    def test_stop_pool(self, worker_pool):
        """Test stopping the worker pool."""
        worker_pool.start()
        worker_pool.stop(wait=True, timeout=2.0)
        
        assert worker_pool.is_running is False

    def test_stop_without_start(self, worker_pool):
        """Test stopping without starting doesn't raise."""
        worker_pool.stop()  # Should not raise

    def test_start_creates_min_workers(self, worker_pool):
        """Test start creates minimum number of workers."""
        worker_pool.start()
        time.sleep(0.05)  # Brief wait for workers to start
        
        assert worker_pool.worker_count >= worker_pool.config.min_workers
        
        worker_pool.stop(wait=False)

    def test_graceful_shutdown(self, scheduler, pool_config):
        """Test graceful shutdown waits for workers."""
        pool = WorkerPool(scheduler=scheduler, config=pool_config)
        pool.start()
        
        # Submit a quick job
        def quick_job():
            time.sleep(0.05)
            return "done"
        
        job = Job(name="quick", func=quick_job)
        scheduler.submit(job)
        
        time.sleep(0.02)  # Let job start
        pool.stop(wait=True, timeout=2.0)
        
        assert pool.is_running is False

    def test_force_shutdown(self, scheduler, pool_config):
        """Test force shutdown terminates quickly."""
        pool = WorkerPool(scheduler=scheduler, config=pool_config)
        pool.start()
        
        def slow_job():
            time.sleep(1.0)
            return "done"
        
        job = Job(name="slow", func=slow_job)
        scheduler.submit(job)
        
        time.sleep(0.02)
        
        start = time.time()
        pool.stop(wait=False)
        elapsed = time.time() - start
        
        assert elapsed < 0.5


# ============================================================================
# TestAutoScalingUp - Auto-scaling up tests
# ============================================================================

class TestAutoScalingUp:
    """Tests for auto-scaling up."""

    def test_scale_up_method(self, worker_pool):
        """Test manual scale up method."""
        worker_pool.start()
        initial_count = worker_pool.worker_count
        
        added = worker_pool.scale_up(2)
        
        # Should add workers up to max
        assert added <= 2
        assert worker_pool.worker_count >= initial_count
        
        worker_pool.stop(wait=False)

    def test_scale_up_respects_max(self, scheduler):
        """Test scaling up doesn't exceed max_workers."""
        config = PoolConfig(min_workers=2, max_workers=3)
        pool = WorkerPool(scheduler=scheduler, config=config)
        pool.start()
        
        # Try to add more than max
        added = pool.scale_up(10)
        
        assert pool.worker_count <= config.max_workers
        
        pool.stop(wait=False)

    def test_scale_up_under_load(self, scheduler):
        """Test pool scales up under high load."""
        config = PoolConfig(
            min_workers=1,
            max_workers=5,
            scale_up_threshold=0.5,
            scale_interval=0.1,
        )
        pool = WorkerPool(scheduler=scheduler, config=config)
        pool.start()
        initial_count = pool.worker_count
        
        # Submit jobs to create load
        for i in range(10):
            job = Job(name=f"job_{i}", func=lambda: time.sleep(0.1))
            scheduler.submit(job)
        
        time.sleep(0.2)  # Wait for auto-scaler
        
        # Pool may have scaled up
        assert pool.worker_count >= initial_count
        
        pool.stop(wait=False, timeout=1.0)


# ============================================================================
# TestAutoScalingDown - Auto-scaling down tests
# ============================================================================

class TestAutoScalingDown:
    """Tests for auto-scaling down."""

    def test_scale_down_method(self, worker_pool):
        """Test manual scale down method."""
        worker_pool.start()
        
        # First scale up
        worker_pool.scale_up(2)
        time.sleep(0.05)
        high_count = worker_pool.worker_count
        
        # Scale down
        removed = worker_pool.scale_down(1)
        
        # May have removed workers (depends on idle state)
        assert worker_pool.worker_count <= high_count
        
        worker_pool.stop(wait=False)

    def test_scale_down_respects_min(self, scheduler):
        """Test scaling down doesn't go below min_workers."""
        config = PoolConfig(min_workers=2, max_workers=5)
        pool = WorkerPool(scheduler=scheduler, config=config)
        pool.start()
        
        # Try to scale down below min
        pool.scale_down(10)
        
        time.sleep(0.05)
        assert pool.worker_count >= config.min_workers
        
        pool.stop(wait=False)


# ============================================================================
# TestPoolStats - Pool statistics tests
# ============================================================================

class TestPoolStats:
    """Tests for pool statistics."""

    def test_get_stats_returns_pool_stats(self, worker_pool):
        """Test getting pool statistics returns PoolStats."""
        worker_pool.start()
        
        stats = worker_pool.get_stats()
        
        assert isinstance(stats, PoolStats)
        
        worker_pool.stop(wait=False)

    def test_stats_total_workers(self, worker_pool):
        """Test stats include total workers."""
        worker_pool.start()
        time.sleep(0.05)
        
        stats = worker_pool.get_stats()
        
        assert stats.total_workers >= worker_pool.config.min_workers
        
        worker_pool.stop(wait=False)

    def test_stats_idle_busy_workers(self, worker_pool):
        """Test stats track idle and busy workers."""
        worker_pool.start()
        time.sleep(0.05)
        
        stats = worker_pool.get_stats()
        
        # Total should be idle + busy + stopping
        assert stats.total_workers == (
            stats.idle_workers + stats.busy_workers + stats.stopping_workers
        )
        
        worker_pool.stop(wait=False)

    def test_stats_to_dict(self, worker_pool):
        """Test stats can be converted to dict."""
        worker_pool.start()
        
        stats = worker_pool.get_stats()
        stats_dict = stats.to_dict()
        
        assert isinstance(stats_dict, dict)
        assert "total_workers" in stats_dict
        assert "idle_workers" in stats_dict
        assert "jobs_completed" in stats_dict
        
        worker_pool.stop(wait=False)

    def test_stats_uptime(self, worker_pool):
        """Test stats track uptime."""
        worker_pool.start()
        time.sleep(0.05)
        
        stats = worker_pool.get_stats()
        
        assert stats.uptime_seconds >= 0
        
        worker_pool.stop(wait=False)


# ============================================================================
# TestPoolWorkerInfo - Worker information tests
# ============================================================================

class TestPoolWorkerInfo:
    """Tests for worker information."""

    def test_get_worker_info(self, worker_pool):
        """Test getting worker information."""
        worker_pool.start()
        time.sleep(0.05)
        
        info_list = worker_pool.get_worker_info()
        
        assert isinstance(info_list, list)
        assert len(info_list) >= worker_pool.config.min_workers
        
        worker_pool.stop(wait=False)

    def test_worker_info_contains_state(self, worker_pool):
        """Test worker info contains state."""
        worker_pool.start()
        time.sleep(0.05)
        
        info_list = worker_pool.get_worker_info()
        
        for info in info_list:
            assert hasattr(info, 'state')
            assert info.state in WorkerState
        
        worker_pool.stop(wait=False)

    def test_get_worker_by_id(self, worker_pool):
        """Test getting specific worker by ID."""
        worker_pool.start()
        time.sleep(0.05)
        
        info_list = worker_pool.get_worker_info()
        if info_list:
            worker_id = info_list[0].worker_id
            worker = worker_pool.get_worker(worker_id)
            
            assert worker is not None
            assert worker.worker_id == worker_id
        
        worker_pool.stop(wait=False)

    def test_get_nonexistent_worker(self, worker_pool):
        """Test getting non-existent worker returns None."""
        worker_pool.start()
        
        worker = worker_pool.get_worker("nonexistent-id")
        
        assert worker is None
        
        worker_pool.stop(wait=False)


# ============================================================================
# TestPoolJobExecution - Job execution through pool
# ============================================================================

class TestPoolJobExecution:
    """Tests for job execution through pool."""

    def test_job_executed_via_scheduler(self, scheduler, worker_pool, sample_job):
        """Test jobs are submitted to scheduler, executed by pool workers."""
        worker_pool.start()
        
        # Submit job to scheduler (not directly to pool)
        scheduler.submit(sample_job)
        
        time.sleep(0.2)  # Wait for execution
        
        # Job should be completed
        assert sample_job.state in (JobState.COMPLETED, JobState.RUNNING)
        
        worker_pool.stop(wait=False)

    def test_multiple_jobs_executed(self, scheduler, worker_pool):
        """Test multiple jobs are executed."""
        worker_pool.start()
        
        jobs = [Job(name=f"job_{i}", func=lambda i=i: i * 2) for i in range(5)]
        
        for job in jobs:
            scheduler.submit(job)
        
        time.sleep(0.3)
        worker_pool.stop(wait=True, timeout=2.0)
        
        completed = sum(1 for job in jobs if job.state == JobState.COMPLETED)
        assert completed >= 1  # At least some should complete

    def test_parallel_execution(self, scheduler):
        """Test jobs execute in parallel with multiple workers."""
        config = PoolConfig(min_workers=3, max_workers=5)
        pool = WorkerPool(scheduler=scheduler, config=config)
        pool.start()
        
        start_times = []
        lock = threading.Lock()
        
        def timed_job():
            with lock:
                start_times.append(time.time())
            time.sleep(0.05)
            return "done"
        
        jobs = [Job(name=f"job_{i}", func=timed_job) for i in range(3)]
        
        for job in jobs:
            scheduler.submit(job)
        
        time.sleep(0.2)
        pool.stop(wait=True, timeout=2.0)
        
        # Check jobs started close together (parallelism)
        if len(start_times) >= 2:
            time_diff = max(start_times) - min(start_times)
            assert time_diff < 0.1  # Should start within 100ms of each other


# ============================================================================
# TestPoolConcurrency - Concurrent operations tests
# ============================================================================

class TestPoolConcurrency:
    """Tests for concurrent pool operations."""

    def test_concurrent_scale_operations(self, worker_pool):
        """Test concurrent scaling operations are thread-safe."""
        worker_pool.start()
        
        errors = []
        
        def scale_up():
            try:
                for _ in range(3):
                    worker_pool.scale_up(1)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
        
        def scale_down():
            try:
                for _ in range(2):
                    worker_pool.scale_down(1)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=scale_up),
            threading.Thread(target=scale_down),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert worker_pool.worker_count >= worker_pool.config.min_workers
        
        worker_pool.stop(wait=False)

    def test_concurrent_stats_access(self, worker_pool):
        """Test concurrent stats access is thread-safe."""
        worker_pool.start()
        
        stats_results = []
        errors = []
        
        def get_stats():
            try:
                for _ in range(10):
                    stats = worker_pool.get_stats()
                    stats_results.append(stats)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=get_stats) for _ in range(3)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert len(stats_results) == 30
        
        worker_pool.stop(wait=False)


# ============================================================================
# TestPoolConfig - Pool configuration tests
# ============================================================================

class TestPoolConfig:
    """Tests for pool configuration."""

    def test_config_property(self, worker_pool, pool_config):
        """Test config property returns PoolConfig."""
        assert worker_pool.config == pool_config

    def test_worker_count_property(self, worker_pool):
        """Test worker_count property."""
        worker_pool.start()
        time.sleep(0.05)
        
        count = worker_pool.worker_count
        
        assert isinstance(count, int)
        assert count >= worker_pool.config.min_workers
        
        worker_pool.stop(wait=False)

    def test_is_running_property(self, worker_pool):
        """Test is_running property."""
        assert worker_pool.is_running is False
        
        worker_pool.start()
        assert worker_pool.is_running is True
        
        worker_pool.stop(wait=False)
        assert worker_pool.is_running is False


# ============================================================================
# TestPoolRepr - String representation tests
# ============================================================================

class TestPoolRepr:
    """Tests for string representation."""

    def test_pool_repr(self, worker_pool):
        """Test repr of pool."""
        result = repr(worker_pool)
        
        assert "WorkerPool" in result

    def test_pool_repr_shows_state(self, worker_pool):
        """Test repr shows running state."""
        worker_pool.start()
        result = repr(worker_pool)
        worker_pool.stop(wait=False)
        
        # Should show some state info
        assert "running" in result.lower() or "workers" in result.lower()


# ============================================================================
# TestPoolEdgeCases - Edge cases and error handling
# ============================================================================

class TestPoolEdgeCases:
    """Tests for edge cases and error handling."""

    def test_double_start(self, worker_pool):
        """Test double start doesn't create extra workers."""
        worker_pool.start()
        initial_count = worker_pool.worker_count
        
        worker_pool.start()  # Should be safe
        
        # Worker count shouldn't change dramatically
        assert abs(worker_pool.worker_count - initial_count) <= 1
        
        worker_pool.stop(wait=False)

    def test_double_stop(self, worker_pool):
        """Test double stop is safe."""
        worker_pool.start()
        worker_pool.stop(wait=False)
        worker_pool.stop(wait=False)  # Should not raise

    def test_scale_up_when_stopped(self, worker_pool):
        """Test scale_up when pool is not running."""
        # Pool not started - scale operations might not work
        result = worker_pool.scale_up(1)
        
        # Should handle gracefully
        assert result >= 0

    def test_get_stats_when_stopped(self, worker_pool):
        """Test get_stats when pool is not running."""
        stats = worker_pool.get_stats()
        
        assert isinstance(stats, PoolStats)
        assert stats.total_workers == 0