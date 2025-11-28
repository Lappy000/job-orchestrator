# Job Orchestrator

A high-performance, lightweight background job orchestrator with DAG support.
A modern alternative to Celery/Airflow for Python applications.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Lightweight:** Core functionality uses only Python stdlib (threading, multiprocessing, queue)
- **DAG Support:** Build complex workflows with job dependencies
- **Priority Queue:** Heap-based O(log n) priority queue implementation
- **Automatic Retries:** Configurable retry policies with exponential backoff and jitter
- **Dynamic Scaling:** Auto-scaling worker pool based on load
- **Job Dependencies:** Chain jobs and execute in parallel when possible
- **Dead Letter Queue:** Failed jobs are preserved for debugging and reprocessing
- **Distributed Locking:** Thread-safe locks with Redis support for multi-node deployments
- **Observable:** Built-in statistics, logging, and state introspection
- **Extensible:** Optional Redis/PostgreSQL backends for persistence

## Installation

```bash
# Basic installation (stdlib only)
pip install job-orchestrator

# With Redis backend
pip install job-orchestrator[redis]

# With PostgreSQL backend
pip install job-orchestrator[postgresql]

# With all backends
pip install job-orchestrator[all]

# Development installation
pip install job-orchestrator[dev]
```

## Quick Start

### Simple Job Submission

```python
from job_orchestrator import Job, JobPriority, ThreadSafePriorityQueue
from job_orchestrator.scheduler import Scheduler

# Create a job
def process_data(data: dict) -> dict:
    # Your processing logic here
    return {"processed": True, "data": data}

# Create scheduler and start it
scheduler = Scheduler()
scheduler.start()

# Create and submit a job
job = Job(
    name="process_data",
    func=process_data,
    args=({"input": "value"},),
    priority=JobPriority.HIGH
)
job_id = scheduler.submit(job)

# Run the job (for synchronous execution)
result = scheduler.run_job(job)
print(f"Result: {result.result}")

# Or get job status (for async execution with workers)
status = scheduler.get_job_status(job_id)
print(f"Status: {status}")

# Stop the scheduler
scheduler.stop()
```

### DAG Workflows (Dependency Graphs)

```python
from job_orchestrator import DAGBuilder, Job
from job_orchestrator.scheduler import Scheduler

# Define task functions
def extract_data():
    return {"raw_data": [1, 2, 3, 4, 5]}

def transform_data(data):
    return {"transformed": [x * 2 for x in data["raw_data"]]}

def load_data(data):
    print(f"Loading: {data}")
    return {"loaded": True, "count": len(data["transformed"])}

# Build a DAG workflow
dag = (DAGBuilder("etl_pipeline", description="Extract, Transform, Load pipeline")
    .add_job(extract_data, job_id="extract")
    .add_job(transform_data, job_id="transform", depends_on=["extract"])
    .add_job(load_data, job_id="load", depends_on=["transform"])
    .with_fail_fast(True)  # Stop on first failure
    .with_max_parallel(2)   # Limit concurrent execution
    .build())

# Submit the DAG
scheduler = Scheduler()
scheduler.start()

dag_id = scheduler.submit_dag(dag)
status = scheduler.get_dag_status(dag_id)
print(f"DAG Status: {status}")

scheduler.stop()
```

### Custom Retry Policy

```python
from job_orchestrator import Job, RetryPolicy, JobPriority
from job_orchestrator.scheduler import Scheduler

# Define a flaky task that might fail
def flaky_api_call():
    import random
    if random.random() < 0.7:  # 70% failure rate
        raise ConnectionError("API unavailable")
    return {"status": "success"}

# Create custom retry policy
retry_policy = RetryPolicy(
    max_retries=5,
    base_delay=1.0,        # Start with 1 second delay
    max_delay=60.0,        # Cap at 60 seconds
    exponential_base=2.0,  # Double delay each retry
    jitter=True            # Add randomness to prevent thundering herd
)

# Create job with retry policy
job = Job(
    name="flaky_api_call",
    func=flaky_api_call,
    retry_policy=retry_policy,
    priority=JobPriority.HIGH
)

scheduler = Scheduler()
scheduler.start()
scheduler.submit(job)
scheduler.stop()
```

### Using the Priority Queue Directly

```python
from job_orchestrator import Job, JobPriority, ThreadSafePriorityQueue

# Create a thread-safe priority queue
queue = ThreadSafePriorityQueue(max_size=1000)

# Create jobs with different priorities
high_priority = Job(name="urgent_task", priority=JobPriority.CRITICAL)
normal_priority = Job(name="regular_task", priority=JobPriority.NORMAL)
low_priority = Job(name="background_task", priority=JobPriority.BACKGROUND)

# Add to queue (any order)
queue.push(low_priority)
queue.push(high_priority)
queue.push(normal_priority)

# Pop returns highest priority first
job1 = queue.pop(timeout=1.0)  # Returns "urgent_task"
job2 = queue.pop(timeout=1.0)  # Returns "regular_task"
job3 = queue.pop(timeout=1.0)  # Returns "background_task"

print(f"Order: {job1.name}, {job2.name}, {job3.name}")
```

### Working with the Dead Letter Queue

```python
from job_orchestrator.scheduler import Scheduler, DLQEntryStatus

scheduler = Scheduler()
scheduler.start()

# ... submit and run jobs ...

# Get failed jobs from DLQ
dlq_entries = scheduler.get_dlq_entries(status=DLQEntryStatus.PENDING)

for entry in dlq_entries:
    print(f"Failed job: {entry.job.name}")
    print(f"Error: {entry.original_error}")
    print(f"Retry attempts: {len(entry.retry_history)}")
    
    # Option 1: Requeue for another attempt
    scheduler.requeue_dlq_entry(entry.entry_id, reset_retry_count=True)
    
    # Option 2: Mark as resolved (fixed externally)
    # scheduler.resolve_dlq_entry(entry.entry_id, notes="Fixed manually")
    
    # Option 3: Discard permanently
    # scheduler.discard_dlq_entry(entry.entry_id, notes="Known issue")

# Get DLQ statistics
stats = scheduler.get_dlq_stats()
print(f"Pending failures: {stats.pending_count}")
print(f"Common errors: {stats.common_errors}")

scheduler.stop()
```

### Distributed Locking

```python
from job_orchestrator.locking import InMemoryLockManager, RedisLockManager

# For single-node deployments
lock_manager = InMemoryLockManager()

# For multi-node deployments with Redis
# import redis
# client = redis.Redis(host='localhost', port=6379)
# lock_manager = RedisLockManager(redis_client=client)

# Using context manager (recommended)
with lock_manager.lock("shared_resource:123", owner="worker-1", ttl=30.0) as lock_info:
    print(f"Acquired lock: {lock_info.lock_id}")
    # Do work with exclusive access to the resource
    pass
# Lock is automatically released

# Manual acquire/release
lock_info = lock_manager.acquire("resource:456", owner="worker-1", timeout=5.0, ttl=30.0)
if lock_info:
    try:
        # Do work
        pass
    finally:
        lock_manager.release("resource:456", owner="worker-1")
```

### Worker Pool with Auto-Scaling

```python
from job_orchestrator.scheduler import Scheduler
from job_orchestrator.workers import WorkerPool, PoolConfig, WorkerType

scheduler = Scheduler()
scheduler.start()

# Configure worker pool
config = PoolConfig(
    min_workers=2,                  # Always keep at least 2 workers
    max_workers=10,                 # Scale up to 10 workers max
    worker_type=WorkerType.THREAD,  # Use threads (or PROCESS for CPU-bound)
    scale_up_threshold=0.8,         # Scale up when 80% busy
    scale_down_threshold=0.2,       # Scale down when 20% busy
    scale_interval=10.0,            # Check every 10 seconds
)

# Create and start pool
pool = WorkerPool(scheduler=scheduler, config=config)
pool.start()

# Workers automatically process jobs from the scheduler's queue
# and scale based on load

# Get pool statistics
stats = pool.get_stats()
print(f"Workers: {stats.total_workers} (busy: {stats.busy_workers})")
print(f"Jobs completed: {stats.jobs_completed}")
print(f"Average job time: {stats.avg_job_time:.2f}s")

# Stop pool gracefully
pool.stop(wait=True, timeout=30.0)
scheduler.stop()
```

## Configuration

### Using Python Configuration

```python
from job_orchestrator import OrchestratorConfig, WorkerPoolConfig, RetryConfig

config = OrchestratorConfig.from_dict({
    "worker_pool": {
        "min_workers": 4,
        "max_workers": 20,
        "worker_type": "thread",
        "scale_up_threshold": 0.8,
        "scale_down_threshold": 0.2,
    },
    "retry": {
        "max_retries": 3,
        "base_delay": 1.0,
        "max_delay": 300.0,
        "exponential_base": 2.0,
        "jitter": True,
    },
    "dlq": {
        "enabled": True,
        "max_size": 10000,
        "auto_cleanup_days": 7,
    },
    "storage": {
        "backend": "memory",  # or "redis", "postgresql"
    },
    "job_timeout": 3600.0,  # 1 hour default
})

scheduler = Scheduler(config)
```

### Using Environment Variables

```bash
export JOB_ORCH_MIN_WORKERS=4
export JOB_ORCH_MAX_WORKERS=20
export JOB_ORCH_WORKER_TYPE=thread
export JOB_ORCH_STORAGE_BACKEND=redis
export JOB_ORCH_REDIS_URL=redis://localhost:6379/0
export JOB_ORCH_MAX_RETRIES=3
export JOB_ORCH_JOB_TIMEOUT=3600
```

```python
from job_orchestrator import OrchestratorConfig

config = OrchestratorConfig.from_env()
scheduler = Scheduler(config)
```

### Using YAML Configuration

```yaml
# job_orchestrator.yaml
worker_pool:
  min_workers: 4
  max_workers: 20
  worker_type: thread
  scale_up_threshold: 0.8
  scale_down_threshold: 0.2

retry:
  max_retries: 3
  base_delay: 1.0
  max_delay: 300.0
  exponential_base: 2.0
  jitter: true

dlq:
  enabled: true
  max_size: 10000
  auto_cleanup_days: 7

storage:
  backend: redis
  redis_url: redis://localhost:6379/0

logging:
  level: INFO

job_timeout: 3600.0
```

```python
from job_orchestrator import OrchestratorConfig

config = OrchestratorConfig.from_yaml("job_orchestrator.yaml")
scheduler = Scheduler(config)
```

## Job States

Jobs progress through the following lifecycle states:

```
PENDING ──────► SCHEDULED ──────► RUNNING ──────► COMPLETED
    │              │                  │
    │              │                  ├──► FAILED (to DLQ)
    │              │                  │
    │              │                  └──► RETRYING ──► SCHEDULED
    │              │
    └──────────────┴──────────────► CANCELLED
```

| State | Description |
|-------|-------------|
| `PENDING` | Job created but not yet queued |
| `SCHEDULED` | Job is in queue waiting for a worker |
| `RUNNING` | Job is being executed by a worker |
| `COMPLETED` | Job finished successfully |
| `FAILED` | Job failed after exhausting retries (in DLQ) |
| `RETRYING` | Job failed and waiting for retry |
| `CANCELLED` | Job was manually cancelled |
| `TIMEOUT` | Job exceeded execution timeout |

## Priority Levels

Jobs can be assigned priority levels that determine execution order:

| Priority | Value | Use Case |
|----------|-------|----------|
| `CRITICAL` | 0 | Urgent tasks requiring immediate processing |
| `HIGH` | 1 | Important tasks with higher precedence |
| `NORMAL` | 2 | Default priority for regular tasks |
| `LOW` | 3 | Tasks that can wait |
| `BACKGROUND` | 4 | Non-urgent background processing |

## Documentation

For detailed documentation, see:

- [Architecture Guide](ARCHITECTURE.md) - System design and component details
- [API Reference](docs/api.md) - Complete API documentation
- [Configuration Guide](docs/configuration.md) - Configuration options
- [Deployment Guide](docs/deployment.md) - Production deployment
- [Examples](examples/) - Practical code examples
## Real-World Test Scenarios

The project includes comprehensive test suites demonstrating real-world use cases:

### 🛒 E-commerce Order Processing
Test an online store handling orders with payment processing, inventory management, and fulfillment:
```bash
pytest tests/test_real_world/test_ecommerce_order_processing.py -v
```
**Scenarios tested:**
- Order creation and validation
- Payment processing with retry logic
- Inventory reservation with distributed locking
- Black Friday load simulation (100 concurrent orders)
- Subscription renewals
- Multi-order fulfillment

### 📊 Data Pipeline (ETL)
Test a data engineering team's ETL workflows:
```bash
pytest tests/test_real_world/test_data_pipeline.py -v
```
**Scenarios tested:**
- Multi-source data extraction (database, API, files)
- Data transformation and cleaning
- Data quality validation with error reporting
- Incremental loading with checkpoints
- Daily sales aggregation
- Customer analytics and segmentation

### 🔧 Microservices Orchestration
Test a DevOps team coordinating microservices deployment and workflows:
```bash
pytest tests/test_real_world/test_microservices_orchestration.py -v
```
**Scenarios tested:**
- Service registration and health checks
- Blue-green deployment pattern
- Canary deployment (gradual rollout)
- Saga pattern with compensation (rollback)
- Event-driven service coordination
- Circuit breaker pattern

Run all real-world tests:
```bash
pytest tests/test_real_world/ -v
```


## Development

```bash
# Clone the repository
git clone https://github.com/example/job-orchestrator.git
cd job-orchestrator

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=job_orchestrator --cov-report=html

# Run type checking
mypy src/

# Format code
black src/ tests/
ruff check src/ tests/ --fix
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Comparison with Alternatives

| Feature | Job Orchestrator | Celery | Airflow |
|---------|-----------------|--------|---------|
| Zero Dependencies | ✅ | ❌ | ❌ |
| DAG Support | ✅ | Limited | ✅ |
| Priority Queue | ✅ Built-in | ❌ | ❌ |
| Auto-scaling | ✅ | Manual | ❌ |
| Dead Letter Queue | ✅ | ❌ | ❌ |
| Lightweight | ✅ <1MB | Heavy | Heavy |
| Learning Curve | Low | Medium | High |

## Support

- 📖 [Documentation](https://github.com/example/job-orchestrator#readme)
- 🐛 [Issue Tracker](https://github.com/example/job-orchestrator/issues)
- 💬 [Discussions](https://github.com/example/job-orchestrator/discussions)