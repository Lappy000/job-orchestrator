# API Reference

Complete API documentation for the Job Orchestrator library.

## Table of Contents

- [Core Classes](#core-classes)
  - [Job](#job)
  - [JobState](#jobstate)
  - [JobPriority](#jobpriority)
  - [RetryPolicy](#retrypolicy)
- [DAG Classes](#dag-classes)
  - [DAG](#dag)
  - [DAGNode](#dagnode)
  - [DAGBuilder](#dagbuilder)
- [Queue Classes](#queue-classes)
  - [ThreadSafePriorityQueue](#threadsafepriorityqueue)
  - [QueueEntry](#queueentry)
- [Scheduler Classes](#scheduler-classes)
  - [Scheduler](#scheduler)
  - [JobStore](#jobstore)
  - [DAGExecutor](#dagexecutor)
- [Worker Classes](#worker-classes)
  - [WorkerPool](#workerpool)
  - [PoolConfig](#poolconfig)
  - [WorkerType](#workertype)
- [Locking Classes](#locking-classes)
  - [LockManager](#lockmanager)
  - [InMemoryLockManager](#inmemorylockmanager)
  - [RedisLockManager](#redislockmanager)
  - [LockInfo](#lockinfo)
- [Dead Letter Queue](#dead-letter-queue)
  - [DeadLetterQueue](#deadletterqueue)
  - [DLQEntry](#dlqentry)
  - [DLQEntryStatus](#dlqentrystatus)
- [Configuration Classes](#configuration-classes)
  - [OrchestratorConfig](#orchestratorconfig)
- [Exceptions](#exceptions)

---

## Core Classes

### Job

The fundamental unit of work in the Job Orchestrator.

```python
from job_orchestrator import Job, JobPriority, RetryPolicy
```

#### Constructor

```python
Job(
    id: UUID = uuid4(),           # Unique identifier
    name: str = "",               # Human-readable name
    description: str = "",        # Optional description
    func: Optional[Callable] = None,  # Function to execute
    func_path: str = "",          # Module path for serialization
    args: tuple = (),             # Positional arguments
    kwargs: Dict[str, Any] = {},  # Keyword arguments
    priority: JobPriority = JobPriority.NORMAL,
    scheduled_at: Optional[datetime] = None,  # When to execute
    timeout: Optional[float] = None,  # Execution timeout (seconds)
    state: JobState = JobState.PENDING,
    depends_on: List[UUID] = [],  # Job IDs this depends on
    retry_policy: RetryPolicy = RetryPolicy(),
    tags: Dict[str, str] = {},    # Metadata tags
    metadata: Dict[str, Any] = {},  # Custom metadata
)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_terminal` | `bool` | True if job is in COMPLETED, FAILED, or CANCELLED state |
| `is_active` | `bool` | True if job is in PENDING, SCHEDULED, RUNNING, or RETRYING state |
| `can_retry` | `bool` | True if job has remaining retry attempts |
| `execution_time` | `Optional[float]` | Execution time in seconds (None if not started/completed) |

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `to_dict()` | `Dict[str, Any]` | Serialize job to dictionary |
| `from_dict(data)` | `Job` | Class method: Deserialize from dictionary |
| `copy()` | `Job` | Create a copy with new ID |

#### Example

```python
from job_orchestrator import Job, JobPriority, RetryPolicy
from datetime import datetime, timedelta

# Create a job with full configuration
job = Job(
    name="send_email",
    func=send_email_function,
    args=("user@example.com",),
    kwargs={"subject": "Hello", "body": "World"},
    priority=JobPriority.HIGH,
    timeout=30.0,
    retry_policy=RetryPolicy(max_retries=3),
    tags={"type": "notification", "user_id": "123"},
)

# Check job state
print(f"Job ID: {job.id}")
print(f"Can retry: {job.can_retry}")
print(f"Is active: {job.is_active}")

# Serialize/deserialize
job_dict = job.to_dict()
restored_job = Job.from_dict(job_dict)
```

---

### JobState

Enum representing job lifecycle states.

```python
from job_orchestrator import JobState
```

#### Values

| Value | Description |
|-------|-------------|
| `PENDING` | Job created but not yet queued |
| `SCHEDULED` | Job in queue waiting for worker |
| `RUNNING` | Job currently being executed |
| `COMPLETED` | Job finished successfully |
| `FAILED` | Job failed after all retries exhausted |
| `RETRYING` | Job failed, waiting for retry |
| `CANCELLED` | Job was manually cancelled |
| `TIMEOUT` | Job exceeded execution timeout |

#### Example

```python
from job_orchestrator import Job, JobState

job = Job(name="example")
print(job.state)  # JobState.PENDING

# Check state
if job.state == JobState.PENDING:
    print("Job is waiting to be processed")
```

---

### JobPriority

Enum representing job priority levels.

```python
from job_orchestrator import JobPriority
```

#### Values

| Value | Numeric | Description |
|-------|---------|-------------|
| `CRITICAL` | 0 | Highest priority - urgent jobs |
| `HIGH` | 1 | High priority jobs |
| `NORMAL` | 2 | Default priority |
| `LOW` | 3 | Low priority jobs |
| `BACKGROUND` | 4 | Lowest priority - background tasks |

#### Example

```python
from job_orchestrator import Job, JobPriority

urgent = Job(name="urgent", priority=JobPriority.CRITICAL)
normal = Job(name="normal", priority=JobPriority.NORMAL)
background = Job(name="background", priority=JobPriority.BACKGROUND)
```

---

### RetryPolicy

Configuration for job retry behavior with exponential backoff.

```python
from job_orchestrator import RetryPolicy
```

#### Constructor

```python
RetryPolicy(
    max_retries: int = 3,           # Maximum retry attempts
    base_delay: float = 1.0,        # Initial delay in seconds
    max_delay: float = 300.0,       # Maximum delay cap in seconds
    exponential_base: float = 2.0,  # Backoff multiplier
    jitter: bool = True,            # Add randomness to delays
    retry_on: tuple = (Exception,), # Exception types to retry on
)
```

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `calculate_delay(attempt)` | `float` | Calculate delay for given attempt number |
| `should_retry(exception)` | `bool` | Check if exception type should trigger retry |
| `to_dict()` | `Dict` | Serialize to dictionary |
| `from_dict(data)` | `RetryPolicy` | Class method: Deserialize |

#### Example

```python
from job_orchestrator import RetryPolicy

# Aggressive retry for flaky operations
aggressive = RetryPolicy(
    max_retries=10,
    base_delay=0.5,
    max_delay=60.0,
    exponential_base=1.5,
    jitter=True,
)

# Conservative retry for expensive operations
conservative = RetryPolicy(
    max_retries=2,
    base_delay=5.0,
    max_delay=300.0,
    exponential_base=3.0,
)

# Calculate delays
print(aggressive.calculate_delay(0))  # ~0.5s (with jitter)
print(aggressive.calculate_delay(1))  # ~0.75s
print(aggressive.calculate_delay(2))  # ~1.125s
```

---

## DAG Classes

### DAG

Directed Acyclic Graph for job dependencies.

```python
from job_orchestrator import DAG
```

#### Constructor

```python
DAG(
    id: UUID = uuid4(),
    name: str = "",
    description: str = "",
    fail_fast: bool = True,       # Stop on first failure
    max_parallel: Optional[int] = None,  # Limit concurrent jobs
    enable_rollback: bool = False,
    rollback_handler: Optional[Callable] = None,
)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_complete` | `bool` | True if all jobs are completed |
| `has_failed` | `bool` | True if any job has failed |
| `progress` | `float` | Completion percentage (0.0 to 1.0) |

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `add_node(job)` | `None` | Add a job without dependencies |
| `add_edge(from_id, to_id)` | `None` | Add dependency edge |
| `add_job(job, depends_on)` | `None` | Add job with dependencies |
| `get_root_nodes()` | `List[UUID]` | Get jobs with no dependencies |
| `get_leaf_nodes()` | `List[UUID]` | Get jobs with no dependents |
| `get_ready_jobs()` | `List[Job]` | Get jobs ready for execution |
| `topological_sort()` | `List[Job]` | Get jobs in execution order |
| `has_cycle()` | `bool` | Check for cycles |
| `validate()` | `bool` | Validate DAG structure |
| `get_execution_plan()` | `List[List[Job]]` | Get parallelizable job levels |

#### Example

```python
from job_orchestrator import DAG, Job

# Create DAG manually
dag = DAG(name="my_pipeline")

job_a = Job(name="extract")
job_b = Job(name="transform")
job_c = Job(name="load")

dag.add_node(job_a)
dag.add_node(job_b)
dag.add_node(job_c)

dag.add_edge(job_a.id, job_b.id)  # transform depends on extract
dag.add_edge(job_b.id, job_c.id)  # load depends on transform

# Validate
dag.validate()

# Get execution order
for job in dag.topological_sort():
    print(f"Execute: {job.name}")

# Get parallelizable levels
for level, jobs in enumerate(dag.get_execution_plan()):
    print(f"Level {level}: {[j.name for j in jobs]}")
```

---

### DAGBuilder

Fluent API for building DAGs.

```python
from job_orchestrator import DAGBuilder
```

#### Constructor

```python
DAGBuilder(name: str, description: str = "")
```

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `add_job(job_or_func, job_id, depends_on, **kwargs)` | `DAGBuilder` | Add a job |
| `with_fail_fast(enabled)` | `DAGBuilder` | Enable/disable fail-fast |
| `with_max_parallel(limit)` | `DAGBuilder` | Set parallel limit |
| `with_rollback(handler)` | `DAGBuilder` | Enable rollback |
| `with_metadata(**kwargs)` | `DAGBuilder` | Add metadata |
| `build()` | `DAG` | Validate and return DAG |
| `build_unchecked()` | `DAG` | Return DAG without validation |

#### Example

```python
from job_orchestrator import DAGBuilder

def extract(): return {"data": [1, 2, 3]}
def transform(data): return [x * 2 for x in data]
def load(data): print(f"Loaded: {data}")
def rollback(): print("Rolling back!")

# Build DAG with fluent API
dag = (DAGBuilder("etl_pipeline", "ETL workflow")
    .add_job(extract, job_id="extract")
    .add_job(transform, job_id="transform", depends_on=["extract"])
    .add_job(load, job_id="load", depends_on=["transform"])
    .with_fail_fast(True)
    .with_max_parallel(2)
    .with_rollback(rollback)
    .with_metadata(owner="data-team", version="1.0")
    .build())
```

---

## Queue Classes

### ThreadSafePriorityQueue

Thread-safe heap-based priority queue with O(log n) operations.

```python
from job_orchestrator import ThreadSafePriorityQueue
```

#### Constructor

```python
ThreadSafePriorityQueue(max_size: Optional[int] = None)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `size` | `int` | Number of items in queue |
| `is_empty` | `bool` | True if queue is empty |
| `is_full` | `bool` | True if queue is at max capacity |

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `push(job)` | `bool` | Add job to queue (False if full) |
| `pop(timeout)` | `Optional[Job]` | Get highest priority ready job |
| `pop_nowait()` | `Optional[Job]` | Non-blocking pop |
| `peek()` | `Optional[Job]` | View top job without removing |
| `peek_ready()` | `Optional[Job]` | View top ready job |
| `remove(job_id)` | `bool` | Remove job by ID |
| `get(job_id)` | `Optional[Job]` | Get job by ID |
| `contains(job_id)` | `bool` | Check if job is in queue |
| `update_priority(job_id, priority)` | `bool` | Change job priority |
| `reschedule(job_id, scheduled_at)` | `bool` | Change scheduled time |
| `clear()` | `int` | Remove all jobs |
| `shutdown()` | `None` | Signal shutdown |
| `get_stats()` | `Dict` | Get queue statistics |

#### Example

```python
from job_orchestrator import ThreadSafePriorityQueue, Job, JobPriority
from datetime import datetime, timedelta
import threading

queue = ThreadSafePriorityQueue(max_size=1000)

# Add jobs with different priorities
queue.push(Job(name="low", priority=JobPriority.LOW))
queue.push(Job(name="high", priority=JobPriority.HIGH))
queue.push(Job(name="critical", priority=JobPriority.CRITICAL))

# Pop returns highest priority first
job = queue.pop(timeout=1.0)  # Returns "critical"

# Schedule for future execution
future_job = Job(
    name="future",
    scheduled_at=datetime.utcnow() + timedelta(minutes=5)
)
queue.push(future_job)

# Consumer thread
def consumer():
    while True:
        job = queue.pop(timeout=5.0)
        if job is None:
            break
        print(f"Processing: {job.name}")

# Get statistics
stats = queue.get_stats()
print(f"Total: {stats['total_size']}, Ready: {stats['ready']}")
```

---

## Scheduler Classes

### Scheduler

Main job scheduler coordinating all components.

```python
from job_orchestrator.scheduler import Scheduler
from job_orchestrator import OrchestratorConfig
```

#### Constructor

```python
Scheduler(config: Optional[OrchestratorConfig] = None)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_running` | `bool` | Check if scheduler is running |
| `retry_handler` | `RetryHandler` | Get retry handler instance |
| `dlq` | `DeadLetterQueue` | Get dead letter queue |

#### Methods

##### Lifecycle

| Method | Returns | Description |
|--------|---------|-------------|
| `start()` | `None` | Start the scheduler |
| `stop(wait, timeout)` | `None` | Stop the scheduler |

##### Job Management

| Method | Returns | Description |
|--------|---------|-------------|
| `submit(job)` | `str` | Submit a job, returns job ID |
| `submit_dag(dag)` | `str` | Submit a DAG, returns DAG ID |
| `get_next_job(timeout)` | `Optional[Job]` | Get next ready job |
| `run_job(job)` | `JobResult` | Execute job synchronously |
| `complete_job(job_id, result)` | `None` | Mark job as completed |
| `fail_job(job_id, error)` | `None` | Mark job as failed |
| `cancel_job(job_id)` | `bool` | Cancel a pending job |

##### Status Queries

| Method | Returns | Description |
|--------|---------|-------------|
| `get_job_status(job_id)` | `JobState` | Get job state |
| `get_job(job_id)` | `Optional[Job]` | Get job by ID |
| `get_dag_status(dag_id)` | `Dict` | Get DAG status |
| `list_jobs(state, limit, offset)` | `List[Job]` | List jobs |
| `list_dags(status, limit, offset)` | `List[DAGExecution]` | List DAGs |
| `get_stats()` | `Dict` | Get scheduler statistics |

##### Dead Letter Queue

| Method | Returns | Description |
|--------|---------|-------------|
| `get_dlq_entries(status, limit, offset)` | `List[DLQEntry]` | List DLQ entries |
| `get_dlq_entry(entry_id)` | `Optional[DLQEntry]` | Get DLQ entry |
| `requeue_dlq_entry(entry_id, reset, by)` | `bool` | Requeue failed job |
| `discard_dlq_entry(entry_id, notes, by)` | `bool` | Discard entry |
| `resolve_dlq_entry(entry_id, notes, by)` | `bool` | Mark as resolved |
| `get_dlq_stats()` | `DLQStats` | Get DLQ statistics |
| `get_dlq_analytics()` | `Dict` | Get failure analytics |

##### Callbacks

| Method | Returns | Description |
|--------|---------|-------------|
| `on_job_complete(callback)` | `None` | Register completion callback |
| `on_job_failed(callback)` | `None` | Register failure callback |
| `on_dlq_entry_added(callback)` | `None` | Register DLQ callback |

#### Example

```python
from job_orchestrator.scheduler import Scheduler
from job_orchestrator import Job, OrchestratorConfig

# Create with configuration
config = OrchestratorConfig.from_dict({
    "retry": {"max_retries": 5},
    "dlq": {"max_size": 5000},
})
scheduler = Scheduler(config)

# Register callbacks
def on_complete(job, result):
    print(f"Job {job.name} completed: {result.result}")

def on_failed(job, result):
    print(f"Job {job.name} failed: {result.error}")

scheduler.on_job_complete(on_complete)
scheduler.on_job_failed(on_failed)

# Start scheduler
scheduler.start()

# Submit jobs
job = Job(name="example", func=lambda: "Hello!")
job_id = scheduler.submit(job)

# Run synchronously
result = scheduler.run_job(job)
print(result.result)

# Get statistics
stats = scheduler.get_stats()
print(f"Completed: {stats['jobs_completed']}")

# Stop scheduler
scheduler.stop(wait=True)
```

---

## Worker Classes

### WorkerPool

Manages workers with dynamic auto-scaling.

```python
from job_orchestrator.workers import WorkerPool, PoolConfig
```

#### Constructor

```python
WorkerPool(
    scheduler: Scheduler,
    config: Optional[PoolConfig] = None,
)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_running` | `bool` | Check if pool is running |
| `worker_count` | `int` | Current number of workers |
| `config` | `PoolConfig` | Pool configuration |

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `start()` | `None` | Start pool with min workers |
| `stop(wait, timeout)` | `None` | Stop all workers |
| `scale_up(count)` | `int` | Add workers (returns actual added) |
| `scale_down(count)` | `int` | Remove idle workers |
| `get_stats()` | `PoolStats` | Get pool statistics |
| `get_worker_info()` | `List[WorkerInfo]` | Get all worker info |
| `get_worker(worker_id)` | `Optional[BaseWorker]` | Get specific worker |

#### Example

```python
from job_orchestrator.scheduler import Scheduler
from job_orchestrator.workers import WorkerPool, PoolConfig, WorkerType

scheduler = Scheduler()
scheduler.start()

# Configure pool
config = PoolConfig(
    min_workers=2,
    max_workers=10,
    worker_type=WorkerType.THREAD,
    scale_up_threshold=0.8,
    scale_down_threshold=0.2,
    scale_interval=10.0,
    worker_max_idle_time=60.0,
)

pool = WorkerPool(scheduler=scheduler, config=config)
pool.start()

# Manual scaling
pool.scale_up(3)    # Add 3 workers
pool.scale_down(1)  # Remove 1 idle worker

# Monitor pool
stats = pool.get_stats()
print(f"Workers: {stats.total_workers}")
print(f"Busy: {stats.busy_workers}")
print(f"Jobs completed: {stats.jobs_completed}")
print(f"Avg job time: {stats.avg_job_time:.2f}s")

pool.stop(wait=True)
scheduler.stop()
```

---

### PoolConfig

Configuration for WorkerPool.

```python
from job_orchestrator.workers import PoolConfig, WorkerType
```

#### Constructor

```python
PoolConfig(
    min_workers: int = 2,
    max_workers: int = 10,
    worker_type: WorkerType = WorkerType.THREAD,
    scale_up_threshold: float = 0.8,
    scale_down_threshold: float = 0.2,
    health_check_interval: float = 5.0,
    worker_max_idle_time: float = 60.0,
    scale_interval: float = 10.0,
    worker_heartbeat_timeout: float = 30.0,
)
```

---

### WorkerType

Enum for worker execution model.

```python
from job_orchestrator.workers import WorkerType
```

| Value | Description |
|-------|-------------|
| `THREAD` | Thread-based (good for I/O bound) |
| `PROCESS` | Process-based (good for CPU bound) |
| `ASYNC` | Async-based (good for async/await) |

---

## Locking Classes

### LockManager

Abstract base class for distributed locking.

```python
from job_orchestrator.locking import LockManager
```

#### Abstract Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `acquire(resource, owner, timeout, ttl, metadata)` | `Optional[LockInfo]` | Acquire lock |
| `release(resource, owner)` | `bool` | Release lock |
| `extend(resource, owner, ttl)` | `bool` | Extend lock TTL |
| `is_locked(resource)` | `bool` | Check if locked |
| `get_lock_info(resource)` | `Optional[LockInfo]` | Get lock details |

#### Context Manager

```python
with lock_manager.lock(resource, owner, timeout, ttl, metadata) as lock_info:
    # Do work with exclusive access
    pass
# Lock automatically released
```

---

### InMemoryLockManager

Thread-safe in-memory lock manager for single-node deployments.

```python
from job_orchestrator.locking import InMemoryLockManager
```

#### Example

```python
from job_orchestrator.locking import InMemoryLockManager

lock_manager = InMemoryLockManager()

# Context manager (recommended)
with lock_manager.lock("resource:123", owner="worker-1", ttl=30.0) as lock:
    print(f"Got lock: {lock.lock_id}")
    # Do exclusive work

# Manual acquire/release
lock = lock_manager.acquire("resource:456", owner="worker-1", ttl=30.0)
if lock:
    try:
        # Do work
        lock_manager.extend("resource:456", "worker-1", ttl=60.0)
    finally:
        lock_manager.release("resource:456", "worker-1")
```

---

### RedisLockManager

Distributed lock manager using Redis.

```python
from job_orchestrator.locking import RedisLockManager
```

#### Constructor

```python
RedisLockManager(
    redis_client=None,      # Single Redis client
    redis_urls=None,        # Multiple URLs for Redlock
    key_prefix: str = "job_orchestrator:lock:",
)
```

#### Example

```python
import redis
from job_orchestrator.locking import RedisLockManager

# Single Redis instance
client = redis.Redis(host='localhost', port=6379)
lock_manager = RedisLockManager(redis_client=client)

# Multiple instances (Redlock algorithm)
lock_manager = RedisLockManager(redis_urls=[
    "redis://host1:6379",
    "redis://host2:6379",
    "redis://host3:6379",
])

with lock_manager.lock("distributed:resource", ttl=30.0) as lock:
    # Safe across multiple nodes
    pass
```

---

### LockInfo

Information about an acquired lock.

```python
from job_orchestrator.locking import LockInfo
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `lock_id` | `str` | Unique lock identifier |
| `resource` | `str` | Locked resource name |
| `owner` | `str` | Lock owner identifier |
| `acquired_at` | `datetime` | When lock was acquired |
| `expires_at` | `Optional[datetime]` | When lock expires |
| `is_expired` | `bool` | Check if expired |
| `remaining_ttl` | `Optional[float]` | Remaining TTL in seconds |

---

## Dead Letter Queue

### DeadLetterQueue

Storage for jobs that exhausted all retries.

```python
from job_orchestrator.scheduler import DeadLetterQueue, DLQEntryStatus
```

#### Constructor

```python
DeadLetterQueue(
    max_size: int = 10000,
    ttl_days: int = 30,
    cleanup_interval: float = 3600.0,
)
```

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `add(job, error, traceback)` | `str` | Add failed job, returns entry ID |
| `get(entry_id)` | `Optional[DLQEntry]` | Get entry by ID |
| `get_by_job_id(job_id)` | `Optional[DLQEntry]` | Get by job ID |
| `get_all(status, limit, offset)` | `List[DLQEntry]` | List entries |
| `requeue(entry_id, scheduler, reset, by)` | `bool` | Retry job |
| `discard(entry_id, notes, by)` | `bool` | Discard entry |
| `resolve(entry_id, notes, by)` | `bool` | Mark resolved |
| `remove(entry_id)` | `bool` | Delete entry |
| `get_stats()` | `DLQStats` | Get statistics |
| `get_failure_analytics()` | `Dict` | Get analytics |
| `on_entry_added(callback)` | `None` | Register callback |
| `shutdown()` | `None` | Stop cleanup thread |

---

### DLQEntry

Entry in the dead letter queue.

```python
from job_orchestrator.scheduler import DLQEntry
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `entry_id` | `str` | Unique entry ID |
| `job` | `Job` | The failed job |
| `original_error` | `str` | Error message |
| `error_traceback` | `Optional[str]` | Full traceback |
| `failed_at` | `datetime` | When added to DLQ |
| `retry_history` | `List[Dict]` | Previous attempts |
| `status` | `DLQEntryStatus` | Current status |
| `notes` | `str` | Administrative notes |
| `age` | `timedelta` | Time since failure |
| `error_type` | `str` | Extracted error type |

---

### DLQEntryStatus

Enum for DLQ entry status.

```python
from job_orchestrator.scheduler import DLQEntryStatus
```

| Value | Description |
|-------|-------------|
| `PENDING` | Awaiting manual review |
| `REQUEUED` | Sent back for retry |
| `DISCARDED` | Permanently removed |
| `RESOLVED` | Fixed externally |

---

## Configuration Classes

### OrchestratorConfig

Main configuration class aggregating all settings.

```python
from job_orchestrator import OrchestratorConfig
```

#### Loading Methods

```python
# From dictionary
config = OrchestratorConfig.from_dict({
    "worker_pool": {...},
    "retry": {...},
    "storage": {...},
})

# From YAML file
config = OrchestratorConfig.from_yaml("config.yaml")

# From TOML file
config = OrchestratorConfig.from_toml("config.toml")

# From environment variables
config = OrchestratorConfig.from_env()
```

#### Nested Configurations

| Config | Description |
|--------|-------------|
| `WorkerPoolConfig` | Worker pool settings |
| `QueueConfig` | Queue settings |
| `RetryConfig` | Default retry policy |
| `DeadLetterQueueConfig` | DLQ settings |
| `StorageConfig` | Storage backend |
| `LockConfig` | Locking settings |
| `LoggingConfig` | Logging settings |
| `MetricsConfig` | Metrics settings |

See [Configuration Guide](configuration.md) for details.

---

## Exceptions

All exceptions inherit from `JobOrchestratorError`.

```python
from job_orchestrator import (
    JobOrchestratorError,
    JobNotFoundError,
    JobAlreadyExistsError,
    InvalidStateTransitionError,
    CyclicDependencyError,
    DAGValidationError,
    LockAcquisitionError,
    JobTimeoutError,
    JobFailedError,
    JobCancelledError,
    QueueFullError,
    WorkerPoolError,
    StorageError,
    SerializationError,
    ConfigurationError,
)
```

| Exception | Description |
|-----------|-------------|
| `JobNotFoundError` | Job not found by ID |
| `JobAlreadyExistsError` | Duplicate job ID |
| `InvalidStateTransitionError` | Invalid state change |
| `CyclicDependencyError` | Cycle in DAG |
| `DAGValidationError` | Invalid DAG structure |
| `LockAcquisitionError` | Failed to acquire lock |
| `JobTimeoutError` | Job exceeded timeout |
| `JobFailedError` | Job execution failed |
| `JobCancelledError` | Job was cancelled |
| `QueueFullError` | Queue at capacity |
| `WorkerPoolError` | Worker pool error |
| `StorageError` | Storage backend error |
| `SerializationError` | Serialization failed |
| `ConfigurationError` | Invalid configuration |

#### Example

```python
from job_orchestrator import (
    JobNotFoundError,
    QueueFullError,
    LockAcquisitionError,
)
from job_orchestrator.scheduler import Scheduler

scheduler = Scheduler()
scheduler.start()

try:
    status = scheduler.get_job_status("nonexistent-id")
except JobNotFoundError as e:
    print(f"Job not found: {e.job_id}")

try:
    # Queue with max_size=1
    scheduler.submit(job1)
    scheduler.submit(job2)  # Might fail
except QueueFullError as e:
    print(f"Queue full: {e.queue_size}/{e.max_size}")

try:
    with lock_manager.lock("resource", timeout=1.0) as lock:
        pass
except LockAcquisitionError as e:
    print(f"Could not acquire lock: {e.lock_name}")