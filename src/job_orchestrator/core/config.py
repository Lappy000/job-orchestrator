"""
Configuration dataclasses for the Job Orchestrator.

This module provides configuration classes for all components of the
job orchestrator, supporting loading from dictionaries, YAML files,
and environment variables.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional
import os

from .job import JobPriority


@dataclass
class WorkerPoolConfig:
    """
    Worker pool configuration.
    
    Controls the worker pool behavior including sizing, scaling,
    and health monitoring.
    
    Attributes:
        min_workers: Minimum number of workers to maintain.
        max_workers: Maximum number of workers allowed.
        worker_type: Type of workers to use ('thread', 'process', or 'async').
        scale_up_threshold: Queue utilization threshold to trigger scale up.
        scale_down_threshold: Queue utilization threshold to trigger scale down.
        scale_interval: Seconds between scaling decisions.
        heartbeat_interval: Seconds between worker heartbeats.
        worker_timeout: Maximum time without heartbeat before worker is considered dead.
    """
    min_workers: int = 2
    max_workers: int = 10
    worker_type: Literal["thread", "process", "async"] = "thread"
    
    # Auto-scaling
    scale_up_threshold: float = 0.8
    scale_down_threshold: float = 0.2
    scale_interval: float = 10.0
    
    # Health checks
    heartbeat_interval: float = 5.0
    worker_timeout: float = 300.0
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.min_workers < 1:
            raise ValueError("min_workers must be at least 1")
        if self.max_workers < self.min_workers:
            raise ValueError("max_workers must be >= min_workers")
        if self.worker_type not in ("thread", "process", "async"):
            raise ValueError(f"Invalid worker_type: {self.worker_type}")
        if not 0 <= self.scale_up_threshold <= 1:
            raise ValueError("scale_up_threshold must be between 0 and 1")
        if not 0 <= self.scale_down_threshold <= 1:
            raise ValueError("scale_down_threshold must be between 0 and 1")
        if self.scale_down_threshold > self.scale_up_threshold:
            raise ValueError("scale_down_threshold must be <= scale_up_threshold")


# Backwards compatibility alias expected by legacy tests
WorkerConfig = WorkerPoolConfig


@dataclass
class QueueConfig:
    """
    Queue configuration.
    
    Controls the priority queue behavior and limits.
    
    Attributes:
        max_size: Maximum queue size (None for unlimited).
        default_priority: Default priority for jobs without explicit priority.
    """
    max_size: Optional[int] = None
    default_priority: JobPriority = JobPriority.NORMAL
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.max_size is not None and self.max_size < 1:
            raise ValueError("max_size must be at least 1 or None")


@dataclass
class RetryConfig:
    """
    Default retry configuration.
    
    Controls default retry behavior for jobs without explicit retry policies.
    
    Attributes:
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds for the first retry.
        max_delay: Maximum delay in seconds (caps exponential growth).
        exponential_base: Multiplier for exponential backoff.
        jitter: If True, adds randomness to prevent thundering herd.
    """
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 300.0
    exponential_base: float = 2.0
    jitter: bool = True
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be > 0")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if self.exponential_base < 1:
            raise ValueError("exponential_base must be >= 1")


@dataclass
class DeadLetterQueueConfig:
    """
    Dead letter queue configuration.
    
    Controls the behavior of the dead letter queue for failed jobs.
    
    Attributes:
        enabled: If True, failed jobs are stored in the DLQ.
        max_size: Maximum number of entries in the DLQ.
        auto_cleanup_days: Number of days to keep resolved entries.
    """
    enabled: bool = True
    max_size: int = 10000
    auto_cleanup_days: int = 7
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.max_size < 1:
            raise ValueError("max_size must be at least 1")
        if self.auto_cleanup_days < 1:
            raise ValueError("auto_cleanup_days must be at least 1")


@dataclass
class StorageConfig:
    """
    Storage backend configuration.
    
    Controls which storage backend is used and its settings.
    
    Attributes:
        backend: Storage backend type ('memory', 'redis', or 'postgresql').
        redis_url: Redis connection URL (for redis backend).
        redis_prefix: Key prefix for Redis storage.
        postgresql_url: PostgreSQL connection URL (for postgresql backend).
        postgresql_pool_size: Connection pool size for PostgreSQL.
    """
    backend: Literal["memory", "redis", "postgresql"] = "memory"
    
    # Redis options
    redis_url: Optional[str] = None
    redis_prefix: str = "job_orchestrator:"
    
    # PostgreSQL options
    postgresql_url: Optional[str] = None
    postgresql_pool_size: int = 5
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.backend not in ("memory", "redis", "postgresql"):
            raise ValueError(f"Invalid backend: {self.backend}")
        if self.backend == "redis" and not self.redis_url:
            raise ValueError("redis_url is required when backend is 'redis'")
        if self.backend == "postgresql" and not self.postgresql_url:
            raise ValueError("postgresql_url is required when backend is 'postgresql'")
        if self.postgresql_pool_size < 1:
            raise ValueError("postgresql_pool_size must be at least 1")


@dataclass
class LockConfig:
    """
    Distributed lock configuration.
    
    Controls the distributed locking mechanism.
    
    Attributes:
        backend: Lock backend type ('memory', 'redis', or 'postgresql').
        default_timeout: Default timeout for lock acquisition in seconds.
        default_expiry: Default lock expiration time in seconds.
    """
    backend: Literal["memory", "redis", "postgresql"] = "memory"
    default_timeout: float = 30.0
    default_expiry: float = 60.0
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.backend not in ("memory", "redis", "postgresql"):
            raise ValueError(f"Invalid backend: {self.backend}")
        if self.default_timeout <= 0:
            raise ValueError("default_timeout must be > 0")
        if self.default_expiry <= 0:
            raise ValueError("default_expiry must be > 0")


@dataclass
class LoggingConfig:
    """
    Logging configuration.
    
    Controls logging behavior for the orchestrator.
    
    Attributes:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        format: Log message format string.
        log_file: Path to log file (None for stdout only).
    """
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: Optional[str] = None
    
    def validate(self) -> None:
        """Validate configuration values."""
        valid_levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        if self.level.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {self.level}")


@dataclass
class MetricsConfig:
    """
    Metrics configuration.
    
    Controls metrics collection and export.
    
    Attributes:
        enabled: If True, collect and export metrics.
        export_interval: Seconds between metric exports.
        prometheus_port: Port for Prometheus metrics endpoint (None to disable).
    """
    enabled: bool = True
    export_interval: float = 60.0
    prometheus_port: Optional[int] = None
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.export_interval <= 0:
            raise ValueError("export_interval must be > 0")
        if self.prometheus_port is not None:
            if not 1 <= self.prometheus_port <= 65535:
                raise ValueError("prometheus_port must be between 1 and 65535")


@dataclass
class OrchestratorConfig:
    """
    Main configuration for the job orchestrator.
    
    Aggregates all component configurations and provides methods
    for loading from various sources.
    
    Attributes:
        worker_pool: Worker pool configuration.
        queue: Queue configuration.
        retry: Retry configuration.
        dlq: Dead letter queue configuration.
        storage: Storage backend configuration.
        lock: Lock configuration.
        logging: Logging configuration.
        metrics: Metrics configuration.
        job_timeout: Default job execution timeout in seconds.
        graceful_shutdown_timeout: Timeout for graceful shutdown in seconds.
    """
    
    # Component configs
    worker_pool: WorkerPoolConfig = field(default_factory=WorkerPoolConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    dlq: DeadLetterQueueConfig = field(default_factory=DeadLetterQueueConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    lock: LockConfig = field(default_factory=LockConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    
    # Global settings
    job_timeout: float = 3600.0  # Default 1 hour
    graceful_shutdown_timeout: float = 30.0
    
    def validate(self) -> None:
        """
        Validate all configuration values.
        
        Raises:
            ValueError: If any configuration value is invalid.
        """
        self.worker_pool.validate()
        self.queue.validate()
        self.retry.validate()
        self.dlq.validate()
        self.storage.validate()
        self.lock.validate()
        self.logging.validate()
        self.metrics.validate()
        
        if self.job_timeout <= 0:
            raise ValueError("job_timeout must be > 0")
        if self.graceful_shutdown_timeout <= 0:
            raise ValueError("graceful_shutdown_timeout must be > 0")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrchestratorConfig":
        """
        Create config from dictionary.
        
        Args:
            data: Configuration dictionary.
            
        Returns:
            New OrchestratorConfig instance.
        """
        config = cls()
        
        if "worker_pool" in data:
            wp_data = data["worker_pool"]
            config.worker_pool = WorkerPoolConfig(
                min_workers=wp_data.get("min_workers", 2),
                max_workers=wp_data.get("max_workers", 10),
                worker_type=wp_data.get("worker_type", "thread"),
                scale_up_threshold=wp_data.get("scale_up_threshold", 0.8),
                scale_down_threshold=wp_data.get("scale_down_threshold", 0.2),
                scale_interval=wp_data.get("scale_interval", 10.0),
                heartbeat_interval=wp_data.get("heartbeat_interval", 5.0),
                worker_timeout=wp_data.get("worker_timeout", 300.0),
            )
        
        if "queue" in data:
            q_data = data["queue"]
            priority = q_data.get("default_priority", "normal")
            if isinstance(priority, str):
                priority_map = {
                    "critical": JobPriority.CRITICAL,
                    "high": JobPriority.HIGH,
                    "normal": JobPriority.NORMAL,
                    "low": JobPriority.LOW,
                    "background": JobPriority.BACKGROUND,
                }
                priority = priority_map.get(priority.lower(), JobPriority.NORMAL)
            config.queue = QueueConfig(
                max_size=q_data.get("max_size"),
                default_priority=priority,
            )
        
        if "retry" in data:
            r_data = data["retry"]
            config.retry = RetryConfig(
                max_retries=r_data.get("max_retries", 3),
                base_delay=r_data.get("base_delay", 1.0),
                max_delay=r_data.get("max_delay", 300.0),
                exponential_base=r_data.get("exponential_base", 2.0),
                jitter=r_data.get("jitter", True),
            )
        
        if "dlq" in data:
            d_data = data["dlq"]
            config.dlq = DeadLetterQueueConfig(
                enabled=d_data.get("enabled", True),
                max_size=d_data.get("max_size", 10000),
                auto_cleanup_days=d_data.get("auto_cleanup_days", 7),
            )
        
        if "storage" in data:
            s_data = data["storage"]
            config.storage = StorageConfig(
                backend=s_data.get("backend", "memory"),
                redis_url=s_data.get("redis_url"),
                redis_prefix=s_data.get("redis_prefix", "job_orchestrator:"),
                postgresql_url=s_data.get("postgresql_url"),
                postgresql_pool_size=s_data.get("postgresql_pool_size", 5),
            )
        
        if "lock" in data:
            l_data = data["lock"]
            config.lock = LockConfig(
                backend=l_data.get("backend", "memory"),
                default_timeout=l_data.get("default_timeout", 30.0),
                default_expiry=l_data.get("default_expiry", 60.0),
            )
        
        if "logging" in data:
            log_data = data["logging"]
            config.logging = LoggingConfig(
                level=log_data.get("level", "INFO"),
                format=log_data.get("format", config.logging.format),
                log_file=log_data.get("log_file"),
            )
        
        if "metrics" in data:
            m_data = data["metrics"]
            config.metrics = MetricsConfig(
                enabled=m_data.get("enabled", True),
                export_interval=m_data.get("export_interval", 60.0),
                prometheus_port=m_data.get("prometheus_port"),
            )
        
        if "job_timeout" in data:
            config.job_timeout = data["job_timeout"]
        if "graceful_shutdown_timeout" in data:
            config.graceful_shutdown_timeout = data["graceful_shutdown_timeout"]
        
        return config
    
    @classmethod
    def from_yaml(cls, path: str) -> "OrchestratorConfig":
        """
        Load config from YAML file.
        
        Requires PyYAML to be installed.
        
        Args:
            path: Path to YAML configuration file.
            
        Returns:
            New OrchestratorConfig instance.
        """
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML is required to load YAML config files. "
                "Install it with: pip install pyyaml"
            )
        
        with open(path) as f:
            data = yaml.safe_load(f)
        
        return cls.from_dict(data or {})
    
    @classmethod
    def from_toml(cls, path: str) -> "OrchestratorConfig":
        """
        Load config from TOML file.
        
        Requires tomllib (Python 3.11+) or tomli.
        
        Args:
            path: Path to TOML configuration file.
            
        Returns:
            New OrchestratorConfig instance.
        """
        try:
            import tomllib
        except ImportError:
            try:
                import tomli as tomllib
            except ImportError:
                raise ImportError(
                    "tomllib (Python 3.11+) or tomli is required to load TOML config files. "
                    "Install it with: pip install tomli"
                )
        
        with open(path, "rb") as f:
            data = tomllib.load(f)
        
        return cls.from_dict(data)
    
    @classmethod
    def from_env(cls) -> "OrchestratorConfig":
        """
        Load config from environment variables.
        
        Environment variables use the prefix JOB_ORCH_ and use
        underscores for nested values.
        
        Returns:
            New OrchestratorConfig instance.
        """
        config = cls()
        
        # Worker pool
        if val := os.getenv("JOB_ORCH_MIN_WORKERS"):
            config.worker_pool.min_workers = int(val)
        if val := os.getenv("JOB_ORCH_MAX_WORKERS"):
            config.worker_pool.max_workers = int(val)
        if val := os.getenv("JOB_ORCH_WORKER_TYPE"):
            config.worker_pool.worker_type = val  # type: ignore
        if val := os.getenv("JOB_ORCH_SCALE_UP_THRESHOLD"):
            config.worker_pool.scale_up_threshold = float(val)
        if val := os.getenv("JOB_ORCH_SCALE_DOWN_THRESHOLD"):
            config.worker_pool.scale_down_threshold = float(val)
        if val := os.getenv("JOB_ORCH_SCALE_INTERVAL"):
            config.worker_pool.scale_interval = float(val)
        if val := os.getenv("JOB_ORCH_HEARTBEAT_INTERVAL"):
            config.worker_pool.heartbeat_interval = float(val)
        if val := os.getenv("JOB_ORCH_WORKER_TIMEOUT"):
            config.worker_pool.worker_timeout = float(val)
        
        # Queue
        if val := os.getenv("JOB_ORCH_QUEUE_MAX_SIZE"):
            config.queue.max_size = int(val)
        
        # Retry
        if val := os.getenv("JOB_ORCH_MAX_RETRIES"):
            config.retry.max_retries = int(val)
        if val := os.getenv("JOB_ORCH_RETRY_BASE_DELAY"):
            config.retry.base_delay = float(val)
        if val := os.getenv("JOB_ORCH_RETRY_MAX_DELAY"):
            config.retry.max_delay = float(val)
        
        # Storage
        if val := os.getenv("JOB_ORCH_STORAGE_BACKEND"):
            config.storage.backend = val  # type: ignore
        if val := os.getenv("JOB_ORCH_REDIS_URL"):
            config.storage.redis_url = val
        if val := os.getenv("JOB_ORCH_REDIS_PREFIX"):
            config.storage.redis_prefix = val
        if val := os.getenv("JOB_ORCH_POSTGRESQL_URL"):
            config.storage.postgresql_url = val
        if val := os.getenv("JOB_ORCH_POSTGRESQL_POOL_SIZE"):
            config.storage.postgresql_pool_size = int(val)
        
        # Lock
        if val := os.getenv("JOB_ORCH_LOCK_BACKEND"):
            config.lock.backend = val  # type: ignore
        if val := os.getenv("JOB_ORCH_LOCK_TIMEOUT"):
            config.lock.default_timeout = float(val)
        if val := os.getenv("JOB_ORCH_LOCK_EXPIRY"):
            config.lock.default_expiry = float(val)
        
        # Logging
        if val := os.getenv("JOB_ORCH_LOG_LEVEL"):
            config.logging.level = val
        if val := os.getenv("JOB_ORCH_LOG_FILE"):
            config.logging.log_file = val
        
        # Metrics
        if val := os.getenv("JOB_ORCH_METRICS_ENABLED"):
            config.metrics.enabled = val.lower() in ("true", "1", "yes")
        if val := os.getenv("JOB_ORCH_PROMETHEUS_PORT"):
            config.metrics.prometheus_port = int(val)
        
        # Global
        if val := os.getenv("JOB_ORCH_JOB_TIMEOUT"):
            config.job_timeout = float(val)
        if val := os.getenv("JOB_ORCH_SHUTDOWN_TIMEOUT"):
            config.graceful_shutdown_timeout = float(val)
        
        return config
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dictionary representation of the configuration.
        """
        return {
            "worker_pool": {
                "min_workers": self.worker_pool.min_workers,
                "max_workers": self.worker_pool.max_workers,
                "worker_type": self.worker_pool.worker_type,
                "scale_up_threshold": self.worker_pool.scale_up_threshold,
                "scale_down_threshold": self.worker_pool.scale_down_threshold,
                "scale_interval": self.worker_pool.scale_interval,
                "heartbeat_interval": self.worker_pool.heartbeat_interval,
                "worker_timeout": self.worker_pool.worker_timeout,
            },
            "queue": {
                "max_size": self.queue.max_size,
                "default_priority": self.queue.default_priority.name.lower(),
            },
            "retry": {
                "max_retries": self.retry.max_retries,
                "base_delay": self.retry.base_delay,
                "max_delay": self.retry.max_delay,
                "exponential_base": self.retry.exponential_base,
                "jitter": self.retry.jitter,
            },
            "dlq": {
                "enabled": self.dlq.enabled,
                "max_size": self.dlq.max_size,
                "auto_cleanup_days": self.dlq.auto_cleanup_days,
            },
            "storage": {
                "backend": self.storage.backend,
                "redis_url": self.storage.redis_url,
                "redis_prefix": self.storage.redis_prefix,
                "postgresql_url": self.storage.postgresql_url,
                "postgresql_pool_size": self.storage.postgresql_pool_size,
            },
            "lock": {
                "backend": self.lock.backend,
                "default_timeout": self.lock.default_timeout,
                "default_expiry": self.lock.default_expiry,
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "log_file": self.logging.log_file,
            },
            "metrics": {
                "enabled": self.metrics.enabled,
                "export_interval": self.metrics.export_interval,
                "prometheus_port": self.metrics.prometheus_port,
            },
            "job_timeout": self.job_timeout,
            "graceful_shutdown_timeout": self.graceful_shutdown_timeout,
        }


__all__ = [
    "WorkerPoolConfig",
    "WorkerConfig",
    "QueueConfig",
    "RetryConfig",
    "DeadLetterQueueConfig",
    "StorageConfig",
    "LockConfig",
    "LoggingConfig",
    "MetricsConfig",
    "OrchestratorConfig",
]