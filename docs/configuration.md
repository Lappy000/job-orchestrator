# Configuration Guide

Complete guide to configuring the Job Orchestrator.

## Table of Contents

- [Overview](#overview)
- [Loading Configuration](#loading-configuration)
- [Configuration Reference](#configuration-reference)
  - [OrchestratorConfig](#orchestratorconfig)
  - [WorkerPoolConfig](#workerpoolconfig)
  - [QueueConfig](#queueconfig)
  - [RetryConfig](#retryconfig)
  - [DeadLetterQueueConfig](#deadletterqueueconfig)
  - [StorageConfig](#storageconfig)
  - [LockConfig](#lockconfig)
  - [LoggingConfig](#loggingconfig)
  - [MetricsConfig](#metricsconfig)
- [Environment Variables](#environment-variables)
- [Configuration Files](#configuration-files)
- [Best Practices](#best-practices)

---

## Overview

Job Orchestrator uses a hierarchical configuration system that supports multiple sources:

1. **Programmatic** - Python dataclasses with validation
2. **YAML files** - Human-readable configuration files
3. **TOML files** - Python-native configuration format
4. **Environment variables** - For containerized deployments
5. **Defaults** - Sensible defaults for quick start

Configuration precedence (highest to lowest):
1. Explicit programmatic values
2. Environment variables (when using `from_env()`)
3. Configuration file values
4. Default values

---

## Loading Configuration

### Programmatic Configuration

```python
from job_orchestrator import OrchestratorConfig
from job_orchestrator.core.config import (
    WorkerPoolConfig,
    QueueConfig,
    RetryConfig,
    DeadLetterQueueConfig,
    StorageConfig,
    LockConfig,
)
from job_orchestrator.workers import WorkerType

# Full programmatic configuration
config = OrchestratorConfig(
    worker_pool=WorkerPoolConfig(
        min_workers=4,
        max_workers=16,
        worker_type=WorkerType.THREAD,
    ),
    queue=QueueConfig(
        max_size=50000,
    ),
    retry=RetryConfig(
        max_retries=5,
        base_delay=2.0,
        max_delay=600.0,
    ),
    dlq=DeadLetterQueueConfig(
        max_size=10000,
        ttl_days=30,
    ),
    storage=StorageConfig(
        backend="memory",
    ),
    locking=LockConfig(
        backend="memory",
        default_ttl=60.0,
    ),
)
```

### From Dictionary

```python
config = OrchestratorConfig.from_dict({
    "worker_pool": {
        "min_workers": 4,
        "max_workers": 16,
        "worker_type": "thread",
    },
    "queue": {
        "max_size": 50000,
    },
    "retry": {
        "max_retries": 5,
        "base_delay": 2.0,
    },
})
```

### From YAML File

```python
config = OrchestratorConfig.from_yaml("config.yaml")
```

### From TOML File

```python
config = OrchestratorConfig.from_toml("config.toml")
```

### From Environment Variables

```python
config = OrchestratorConfig.from_env()
```

### Merging Configurations

```python
# Load base config, override with environment
base_config = OrchestratorConfig.from_yaml("config.yaml")
env_overrides = OrchestratorConfig.from_env()
config = base_config.merge(env_overrides)
```

---

## Configuration Reference

### OrchestratorConfig

The root configuration class containing all subsystems.

```python
@dataclass
class OrchestratorConfig:
    worker_pool: WorkerPoolConfig = field(default_factory=WorkerPoolConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    dlq: DeadLetterQueueConfig = field(default_factory=DeadLetterQueueConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    locking: LockConfig = field(default_factory=LockConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
```

---

### WorkerPoolConfig

Controls worker pool behavior and scaling.

```python
@dataclass
class WorkerPoolConfig:
    min_workers: int = 2          # Minimum worker count
    max_workers: int = 10         # Maximum worker count
    worker_type: WorkerType = WorkerType.THREAD  # thread, process, async
    scale_up_threshold: float = 0.8    # Utilization to trigger scale up
    scale_down_threshold: float = 0.2  # Utilization to trigger scale down
    health_check_interval: float = 5.0  # Seconds between health checks
    worker_max_idle_time: float = 60.0  # Idle time before scale down
    scale_interval: float = 10.0        # Seconds between scaling decisions
    worker_heartbeat_timeout: float = 30.0  # Timeout for worker heartbeat
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_workers` | int | 2 | Always maintain at least this many workers |
| `max_workers` | int | 10 | Never exceed this many workers |
| `worker_type` | WorkerType | THREAD | Execution model (THREAD, PROCESS, ASYNC) |
| `scale_up_threshold` | float | 0.8 | Scale up when utilization exceeds this |
| `scale_down_threshold` | float | 0.2 | Scale down when utilization drops below |
| `health_check_interval` | float | 5.0 | Seconds between worker health checks |
| `worker_max_idle_time` | float | 60.0 | Remove idle workers after this time |
| `scale_interval` | float | 10.0 | Minimum time between scaling operations |
| `worker_heartbeat_timeout` | float | 30.0 | Mark worker unhealthy after this time |

**Validation Rules:**
- `min_workers` must be ≥ 1
- `max_workers` must be ≥ `min_workers`
- `scale_up_threshold` must be > `scale_down_threshold`
- All thresholds must be between 0.0 and 1.0

**Worker Type Selection:**

| Type | Best For | Notes |
|------|----------|-------|
| `THREAD` | I/O-bound tasks | Shared memory, GIL limitations |
| `PROCESS` | CPU-bound tasks | Process isolation, serialization overhead |
| `ASYNC` | High concurrency | Best for async/await code |

---

### QueueConfig

Controls the priority queue behavior.

```python
@dataclass
class QueueConfig:
    max_size: Optional[int] = None  # None = unlimited
    default_priority: int = 2        # JobPriority.NORMAL
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_size` | Optional[int] | None | Maximum queue size (None = unlimited) |
| `default_priority` | int | 2 | Default priority for jobs without explicit priority |

**Validation Rules:**
- `max_size` must be > 0 if specified
- `default_priority` must be between 0-4

---

### RetryConfig

Default retry policy for jobs without explicit policy.

```python
@dataclass
class RetryConfig:
    max_retries: int = 3           # Maximum retry attempts
    base_delay: float = 1.0        # Initial delay seconds
    max_delay: float = 300.0       # Maximum delay cap
    exponential_base: float = 2.0  # Backoff multiplier
    jitter: bool = True            # Add randomness
    retry_on_timeout: bool = True  # Retry timeout errors
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_retries` | int | 3 | Total retry attempts (0 = no retries) |
| `base_delay` | float | 1.0 | First retry delay in seconds |
| `max_delay` | float | 300.0 | Maximum delay (5 minutes) |
| `exponential_base` | float | 2.0 | Multiply delay by this each attempt |
| `jitter` | bool | True | Add ±25% randomness to delays |
| `retry_on_timeout` | bool | True | Retry jobs that exceed timeout |

**Example Delay Calculation** (base=1.0, exponential_base=2.0):
- Attempt 1: 1.0s
- Attempt 2: 2.0s
- Attempt 3: 4.0s
- Attempt 4: 8.0s
- ...capped at max_delay

**Validation Rules:**
- `max_retries` must be ≥ 0
- `base_delay` must be > 0
- `max_delay` must be ≥ `base_delay`
- `exponential_base` must be ≥ 1.0

---

### DeadLetterQueueConfig

Controls the dead letter queue for failed jobs.

```python
@dataclass
class DeadLetterQueueConfig:
    max_size: int = 10000          # Maximum DLQ entries
    ttl_days: int = 30             # Entry retention days
    cleanup_interval: float = 3600.0  # Cleanup check interval
    enable_analytics: bool = True   # Track failure analytics
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_size` | int | 10000 | Maximum entries before oldest removed |
| `ttl_days` | int | 30 | Days to keep entries |
| `cleanup_interval` | float | 3600.0 | Seconds between cleanup runs |
| `enable_analytics` | bool | True | Enable failure pattern analytics |

**Validation Rules:**
- `max_size` must be > 0
- `ttl_days` must be > 0
- `cleanup_interval` must be > 0

---

### StorageConfig

Configures the storage backend for job persistence.

```python
@dataclass
class StorageConfig:
    backend: str = "memory"        # memory, sqlite, redis, postgresql
    connection_string: str = ""    # Database connection string
    table_prefix: str = "job_orchestrator_"
    pool_size: int = 5             # Connection pool size
    pool_timeout: float = 30.0     # Connection timeout
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `backend` | str | "memory" | Storage backend type |
| `connection_string` | str | "" | Database URL |
| `table_prefix` | str | "job_orchestrator_" | Table/key prefix |
| `pool_size` | int | 5 | Database connection pool size |
| `pool_timeout` | float | 30.0 | Pool connection timeout |

**Supported Backends:**

| Backend | Connection String Example |
|---------|---------------------------|
| `memory` | (none required) |
| `sqlite` | `sqlite:///jobs.db` |
| `redis` | `redis://localhost:6379/0` |
| `postgresql` | `postgresql://user:pass@host/db` |

---

### LockConfig

Configures distributed locking.

```python
@dataclass
class LockConfig:
    backend: str = "memory"        # memory, redis, file, consul
    connection_string: str = ""    # Backend connection
    default_ttl: float = 60.0      # Default lock TTL seconds
    retry_interval: float = 0.1    # Retry interval for waiting
    key_prefix: str = "job_orchestrator:lock:"
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `backend` | str | "memory" | Lock backend type |
| `connection_string` | str | "" | Backend connection URL |
| `default_ttl` | float | 60.0 | Default lock timeout |
| `retry_interval` | float | 0.1 | Polling interval when waiting |
| `key_prefix` | str | "job_orchestrator:lock:" | Lock key prefix |

**Supported Backends:**

| Backend | Use Case |
|---------|----------|
| `memory` | Single process, testing |
| `redis` | Distributed, production |
| `file` | Single machine, multiple processes |
| `consul` | Service mesh environments |

---

### LoggingConfig

Configures logging behavior.

```python
@dataclass
class LoggingConfig:
    level: str = "INFO"            # DEBUG, INFO, WARNING, ERROR
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: Optional[str] = None     # Log file path
    max_bytes: int = 10_485_760    # 10MB rotation size
    backup_count: int = 5          # Keep 5 rotated files
    json_format: bool = False      # Use JSON logging
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `level` | str | "INFO" | Minimum log level |
| `format` | str | (see above) | Log message format |
| `file` | Optional[str] | None | Log file path (None = stdout) |
| `max_bytes` | int | 10485760 | Rotate at this size |
| `backup_count` | int | 5 | Rotated files to keep |
| `json_format` | bool | False | Output as JSON |

---

### MetricsConfig

Configures metrics collection.

```python
@dataclass
class MetricsConfig:
    enabled: bool = True           # Enable metrics
    prefix: str = "job_orchestrator"
    export_interval: float = 60.0  # Export interval seconds
    exporter: str = "prometheus"   # prometheus, statsd, none
    endpoint: str = ""             # Exporter endpoint
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | True | Enable metrics collection |
| `prefix` | str | "job_orchestrator" | Metric name prefix |
| `export_interval` | float | 60.0 | Export frequency |
| `exporter` | str | "prometheus" | Metrics exporter |
| `endpoint` | str | "" | Exporter endpoint URL |

---

## Environment Variables

All configuration options can be set via environment variables with the `JOB_ORCHESTRATOR_` prefix.

### Naming Convention

```
JOB_ORCHESTRATOR_{SECTION}_{PARAMETER}
```

### Complete Environment Variable Reference

```bash
# Worker Pool
JOB_ORCHESTRATOR_WORKER_POOL_MIN_WORKERS=4
JOB_ORCHESTRATOR_WORKER_POOL_MAX_WORKERS=16
JOB_ORCHESTRATOR_WORKER_POOL_WORKER_TYPE=thread
JOB_ORCHESTRATOR_WORKER_POOL_SCALE_UP_THRESHOLD=0.8
JOB_ORCHESTRATOR_WORKER_POOL_SCALE_DOWN_THRESHOLD=0.2
JOB_ORCHESTRATOR_WORKER_POOL_HEALTH_CHECK_INTERVAL=5.0
JOB_ORCHESTRATOR_WORKER_POOL_WORKER_MAX_IDLE_TIME=60.0
JOB_ORCHESTRATOR_WORKER_POOL_SCALE_INTERVAL=10.0
JOB_ORCHESTRATOR_WORKER_POOL_WORKER_HEARTBEAT_TIMEOUT=30.0

# Queue
JOB_ORCHESTRATOR_QUEUE_MAX_SIZE=50000
JOB_ORCHESTRATOR_QUEUE_DEFAULT_PRIORITY=2

# Retry
JOB_ORCHESTRATOR_RETRY_MAX_RETRIES=3
JOB_ORCHESTRATOR_RETRY_BASE_DELAY=1.0
JOB_ORCHESTRATOR_RETRY_MAX_DELAY=300.0
JOB_ORCHESTRATOR_RETRY_EXPONENTIAL_BASE=2.0
JOB_ORCHESTRATOR_RETRY_JITTER=true
JOB_ORCHESTRATOR_RETRY_RETRY_ON_TIMEOUT=true

# Dead Letter Queue
JOB_ORCHESTRATOR_DLQ_MAX_SIZE=10000
JOB_ORCHESTRATOR_DLQ_TTL_DAYS=30
JOB_ORCHESTRATOR_DLQ_CLEANUP_INTERVAL=3600.0
JOB_ORCHESTRATOR_DLQ_ENABLE_ANALYTICS=true

# Storage
JOB_ORCHESTRATOR_STORAGE_BACKEND=redis
JOB_ORCHESTRATOR_STORAGE_CONNECTION_STRING=redis://localhost:6379/0
JOB_ORCHESTRATOR_STORAGE_TABLE_PREFIX=job_orchestrator_
JOB_ORCHESTRATOR_STORAGE_POOL_SIZE=5
JOB_ORCHESTRATOR_STORAGE_POOL_TIMEOUT=30.0

# Locking
JOB_ORCHESTRATOR_LOCKING_BACKEND=redis
JOB_ORCHESTRATOR_LOCKING_CONNECTION_STRING=redis://localhost:6379/1
JOB_ORCHESTRATOR_LOCKING_DEFAULT_TTL=60.0
JOB_ORCHESTRATOR_LOCKING_RETRY_INTERVAL=0.1
JOB_ORCHESTRATOR_LOCKING_KEY_PREFIX=job_orchestrator:lock:

# Logging
JOB_ORCHESTRATOR_LOGGING_LEVEL=INFO
JOB_ORCHESTRATOR_LOGGING_FILE=/var/log/job-orchestrator.log
JOB_ORCHESTRATOR_LOGGING_MAX_BYTES=10485760
JOB_ORCHESTRATOR_LOGGING_BACKUP_COUNT=5
JOB_ORCHESTRATOR_LOGGING_JSON_FORMAT=true

# Metrics
JOB_ORCHESTRATOR_METRICS_ENABLED=true
JOB_ORCHESTRATOR_METRICS_PREFIX=job_orchestrator
JOB_ORCHESTRATOR_METRICS_EXPORT_INTERVAL=60.0
JOB_ORCHESTRATOR_METRICS_EXPORTER=prometheus
JOB_ORCHESTRATOR_METRICS_ENDPOINT=http://localhost:9090
```

### Boolean Values

Boolean environment variables accept: `true`, `false`, `1`, `0`, `yes`, `no`

---

## Configuration Files

### YAML Configuration

```yaml
# config.yaml
worker_pool:
  min_workers: 4
  max_workers: 16
  worker_type: thread
  scale_up_threshold: 0.8
  scale_down_threshold: 0.2
  health_check_interval: 5.0
  worker_max_idle_time: 60.0
  scale_interval: 10.0

queue:
  max_size: 50000
  default_priority: 2

retry:
  max_retries: 5
  base_delay: 1.0
  max_delay: 300.0
  exponential_base: 2.0
  jitter: true
  retry_on_timeout: true

dlq:
  max_size: 10000
  ttl_days: 30
  cleanup_interval: 3600.0
  enable_analytics: true

storage:
  backend: redis
  connection_string: redis://localhost:6379/0
  table_prefix: job_orchestrator_
  pool_size: 10
  pool_timeout: 30.0

locking:
  backend: redis
  connection_string: redis://localhost:6379/1
  default_ttl: 60.0
  retry_interval: 0.1
  key_prefix: "job_orchestrator:lock:"

logging:
  level: INFO
  file: /var/log/job-orchestrator.log
  max_bytes: 10485760
  backup_count: 5
  json_format: true

metrics:
  enabled: true
  prefix: job_orchestrator
  export_interval: 60.0
  exporter: prometheus
  endpoint: http://localhost:9090
```

### TOML Configuration

```toml
# config.toml
[worker_pool]
min_workers = 4
max_workers = 16
worker_type = "thread"
scale_up_threshold = 0.8
scale_down_threshold = 0.2
health_check_interval = 5.0
worker_max_idle_time = 60.0
scale_interval = 10.0

[queue]
max_size = 50000
default_priority = 2

[retry]
max_retries = 5
base_delay = 1.0
max_delay = 300.0
exponential_base = 2.0
jitter = true
retry_on_timeout = true

[dlq]
max_size = 10000
ttl_days = 30
cleanup_interval = 3600.0
enable_analytics = true

[storage]
backend = "redis"
connection_string = "redis://localhost:6379/0"
table_prefix = "job_orchestrator_"
pool_size = 10
pool_timeout = 30.0

[locking]
backend = "redis"
connection_string = "redis://localhost:6379/1"
default_ttl = 60.0
retry_interval = 0.1
key_prefix = "job_orchestrator:lock:"

[logging]
level = "INFO"
file = "/var/log/job-orchestrator.log"
max_bytes = 10485760
backup_count = 5
json_format = true

[metrics]
enabled = true
prefix = "job_orchestrator"
export_interval = 60.0
exporter = "prometheus"
endpoint = "http://localhost:9090"
```

---

## Best Practices

### Development Configuration

```yaml
# config.dev.yaml
worker_pool:
  min_workers: 1
  max_workers: 4
  worker_type: thread

retry:
  max_retries: 1
  base_delay: 0.5

dlq:
  max_size: 100
  ttl_days: 1

storage:
  backend: memory

locking:
  backend: memory

logging:
  level: DEBUG
  json_format: false
```

### Production Configuration

```yaml
# config.prod.yaml
worker_pool:
  min_workers: 8
  max_workers: 32
  worker_type: process
  scale_up_threshold: 0.75
  scale_down_threshold: 0.25
  health_check_interval: 3.0

queue:
  max_size: 100000

retry:
  max_retries: 5
  base_delay: 2.0
  max_delay: 600.0
  jitter: true

dlq:
  max_size: 50000
  ttl_days: 30
  cleanup_interval: 1800.0

storage:
  backend: postgresql
  connection_string: ${DATABASE_URL}
  pool_size: 20
  pool_timeout: 10.0

locking:
  backend: redis
  connection_string: ${REDIS_URL}
  default_ttl: 120.0

logging:
  level: INFO
  file: /var/log/job-orchestrator/app.log
  max_bytes: 52428800
  backup_count: 10
  json_format: true

metrics:
  enabled: true
  exporter: prometheus
  endpoint: http://prometheus:9090
```

### High-Throughput Configuration

```yaml
# config.high-throughput.yaml
worker_pool:
  min_workers: 16
  max_workers: 64
  worker_type: async  # Best for I/O
  scale_interval: 5.0
  scale_up_threshold: 0.7

queue:
  max_size: 500000

retry:
  max_retries: 2
  base_delay: 0.5
  max_delay: 30.0

storage:
  backend: redis
  pool_size: 50
```

### Reliability-Focused Configuration

```yaml
# config.reliable.yaml
worker_pool:
  min_workers: 4
  max_workers: 8
  health_check_interval: 2.0
  worker_heartbeat_timeout: 15.0

retry:
  max_retries: 10
  base_delay: 5.0
  max_delay: 3600.0
  exponential_base: 1.5
  jitter: true

dlq:
  max_size: 100000
  ttl_days: 90
  enable_analytics: true

storage:
  backend: postgresql
  pool_size: 10

locking:
  default_ttl: 300.0
```

### Configuration Hierarchy Example

```python
import os
from job_orchestrator import OrchestratorConfig

def load_config():
    """Load configuration with environment-appropriate settings."""
    env = os.getenv("ENVIRONMENT", "development")
    
    # Load base configuration
    base = OrchestratorConfig.from_yaml("config/base.yaml")
    
    # Load environment-specific overrides
    env_config = OrchestratorConfig.from_yaml(f"config/{env}.yaml")
    
    # Apply environment variable overrides
    env_vars = OrchestratorConfig.from_env()
    
    # Merge in order of precedence
    return base.merge(env_config).merge(env_vars)

config = load_config()
```

---

## Troubleshooting

### Common Issues

#### "ConfigurationError: Validation failed"

```python
# Check specific validation error
try:
    config = OrchestratorConfig.from_dict(data)
except ConfigurationError as e:
    print(f"Invalid configuration: {e}")
```

#### Environment Variables Not Loading

```bash
# Ensure proper prefix
export JOB_ORCHESTRATOR_WORKER_POOL_MIN_WORKERS=4

# Check in Python
import os
print(os.environ.get("JOB_ORCHESTRATOR_WORKER_POOL_MIN_WORKERS"))
```

#### YAML Parsing Errors

```yaml
# Wrong: Using tabs
worker_pool:
	min_workers: 4  # Tab character

# Correct: Using spaces
worker_pool:
  min_workers: 4  # 2 spaces
```

### Validation

```python
from job_orchestrator import OrchestratorConfig, ConfigurationError

def validate_config(path: str) -> bool:
    """Validate configuration file."""
    try:
        config = OrchestratorConfig.from_yaml(path)
        config.validate()
        print("✓ Configuration is valid")
        return True
    except ConfigurationError as e:
        print(f"✗ Configuration error: {e}")
        return False
    except Exception as e:
        print(f"✗ Error loading config: {e}")
        return False