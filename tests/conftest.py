"""
Shared test fixtures and utilities for all test modules.

This module provides common fixtures used across all tests, including
scheduler, jobs, DAGs, and configuration objects.
"""

import pytest
from datetime import datetime, timedelta
from uuid import uuid4

from job_orchestrator import (
    Job,
    JobState,
    JobPriority,
    DAG,
    Scheduler,
    OrchestratorConfig,
    ThreadSafePriorityQueue,
)
from job_orchestrator.scheduler.job_store import JobStore
from job_orchestrator.scheduler.dlq import DeadLetterQueue, DLQEntry, DLQEntryStatus, DLQStats
from job_orchestrator.scheduler.retry import RetryHandler, RetryPolicy
from job_orchestrator.scheduler.dag_executor import DAGExecutor
from job_orchestrator.locking import LockManager
from job_orchestrator.locking.memory import InMemoryLockManager
from job_orchestrator.workers import ThreadWorker, WorkerPool
from job_orchestrator.core.config import WorkerPoolConfig, RetryConfig, DeadLetterQueueConfig


@pytest.fixture
def default_config():
    """Create a default orchestrator configuration."""
    return OrchestratorConfig()


@pytest.fixture
def custom_config():
    """Create a custom orchestrator configuration."""
    config = OrchestratorConfig()
    config.worker_pool.min_workers = 1
    config.worker_pool.max_workers = 5
    return config


@pytest.fixture
def scheduler(default_config):
    """Create a scheduler instance."""
    return Scheduler(config=default_config)


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    def test_func(x, y):
        return x + y
    return Job(name="test_job", func=test_func, args=(1, 2))


@pytest.fixture
def scheduled_job():
    """Create a job with scheduled execution time."""
    future = datetime.utcnow() + timedelta(hours=1)
    return Job(name="scheduled_job", scheduled_at=future)


@pytest.fixture
def job_with_timeout():
    """Create a job with a timeout."""
    return Job(name="timeout_job", timeout=0.1)


@pytest.fixture
def job_with_metadata():
    """Create a job with tags and metadata."""
    return Job(
        name="metadata_job",
        tags={"environment": "test", "team": "backend"},
        metadata={"source": "pytest", "version": "1.0"}
    )


@pytest.fixture
def high_priority_job():
    """Create a high priority job."""
    return Job(name="high_priority", priority=JobPriority.HIGH)


@pytest.fixture
def low_priority_job():
    """Create a low priority job."""
    return Job(name="low_priority", priority=JobPriority.LOW)


@pytest.fixture
def failing_job():
    """Create a job that fails."""
    def failing_func():
        raise ValueError("Job failed on purpose")
    
    from job_orchestrator.core.job import RetryPolicy
    return Job(
        name="failing_job",
        func=failing_func,
        retry_policy=RetryPolicy(max_retries=0)  # Don't retry
    )


@pytest.fixture
def simple_dag():
    """Create a simple linear DAG (task_a -> task_b -> task_c)."""
    dag = DAG(name="simple_dag", description="Simple linear DAG")
    
    job_a = Job(name="task_a")
    job_b = Job(name="task_b")
    job_c = Job(name="task_c")
    
    dag.add_node(job_a)
    dag.add_node(job_b)
    dag.add_node(job_c)
    dag.add_edge(job_a.id, job_b.id)
    dag.add_edge(job_b.id, job_c.id)
    
    return dag


@pytest.fixture
def empty_dag():
    """Create an empty DAG with no jobs."""
    return DAG(name="empty_dag", description="Empty DAG fixture")


@pytest.fixture
def single_job_dag():
    """Create a DAG containing a single job."""
    dag = DAG(name="single_job_dag", description="Single job DAG")
    job = Job(name="task_single")
    dag.add_node(job)
    return dag


@pytest.fixture
def empty_priority_queue():
    """Create an empty ThreadSafePriorityQueue instance for tests."""
    return ThreadSafePriorityQueue()


@pytest.fixture
def job_store():
    """Provide a fresh in-memory JobStore for each test."""
    return JobStore()


@pytest.fixture
def parallel_dag():
    """Create a DAG with parallel jobs (task_a -> [task_b, task_c] -> task_d)."""
    dag = DAG(name="parallel_dag", description="DAG with parallel execution")
    
    job_a = Job(name="task_a")
    job_b = Job(name="task_b")
    job_c = Job(name="task_c")
    job_d = Job(name="task_d")
    
    dag.add_node(job_a)
    dag.add_node(job_b)
    dag.add_node(job_c)
    dag.add_node(job_d)
    
    dag.add_edge(job_a.id, job_b.id)
    dag.add_edge(job_a.id, job_c.id)
    dag.add_edge(job_b.id, job_d.id)
    dag.add_edge(job_c.id, job_d.id)
    
    return dag


@pytest.fixture
def diamond_dag():
    """Create a diamond-shaped DAG (task_a -> [task_b, task_c] -> task_d -> task_e)."""
    dag = DAG(name="diamond_dag", description="Diamond-shaped DAG")
    
    job_a = Job(name="task_a")
    job_b = Job(name="task_b")
    job_c = Job(name="task_c")
    job_d = Job(name="task_d")
    job_e = Job(name="task_e")
    
    for job in (job_a, job_b, job_c, job_d, job_e):
        dag.add_node(job)
    
    dag.add_edge(job_a.id, job_b.id)
    dag.add_edge(job_a.id, job_c.id)
    dag.add_edge(job_b.id, job_d.id)
    dag.add_edge(job_c.id, job_d.id)
    dag.add_edge(job_d.id, job_e.id)
    
    return dag


@pytest.fixture
def state_machine():
    """Create a state machine instance."""
    from job_orchestrator.core.state import StateMachine
    return StateMachine()


@pytest.fixture
def dead_letter_queue():
    """Create a dead letter queue instance."""
    return DeadLetterQueue()


@pytest.fixture
def retry_policy():
    """Create a default retry policy."""
    return RetryPolicy()


@pytest.fixture
def default_retry_policy():
    """Create a default retry policy for tests."""
    return RetryPolicy(
        max_retries=3,
        initial_delay=1.0,
        max_delay=300.0,
        exponential_base=2.0,
        jitter=True,
    )


@pytest.fixture
def retry_config():
    """Create a RetryConfig for tests."""
    return RetryConfig(
        max_retries=3,
        base_delay=1.0,
        max_delay=300.0,
        exponential_base=2.0,
        jitter=True,
    )


@pytest.fixture
def retry_handler(retry_policy):
    """Create a retry handler with default policy."""
    return RetryHandler(default_policy=retry_policy)


@pytest.fixture
def dag_executor(scheduler):
    """Create a DAG executor instance."""
    return DAGExecutor(scheduler)


@pytest.fixture
def lock_manager():
    """Create a memory-based lock manager."""
    return LockManager()


@pytest.fixture
def memory_lock_manager():
    """Create an in-memory lock manager."""
    return InMemoryLockManager()


@pytest.fixture
def worker(scheduler):
    """Create a thread worker instance."""
    return ThreadWorker(scheduler=scheduler)


@pytest.fixture
def worker_pool(scheduler):
    """Create a worker pool instance."""
    return WorkerPool(scheduler=scheduler)
