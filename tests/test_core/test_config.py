"""
Tests for OrchestratorConfig and configuration classes.

This module tests configuration loading, validation, environment variable
handling, and default values.
"""

import os
import pytest
from unittest.mock import patch

from job_orchestrator import OrchestratorConfig
from job_orchestrator.core.config import (
    WorkerPoolConfig,
    QueueConfig,
    RetryConfig,
    DeadLetterQueueConfig,
    LockConfig,
    StorageConfig,
)


class TestOrchestratorConfigCreation:
    """Tests for OrchestratorConfig creation and initialization."""

    def test_default_config_creation(self, default_config):
        """Test creating config with default values."""
        assert default_config is not None
        assert isinstance(default_config, OrchestratorConfig)

    def test_config_has_worker_pool_settings(self, default_config):
        """Test config has worker pool configuration."""
        assert hasattr(default_config, 'worker_pool')
        assert isinstance(default_config.worker_pool, WorkerPoolConfig)

    def test_config_has_queue_settings(self, default_config):
        """Test config has queue configuration."""
        assert hasattr(default_config, 'queue')
        assert isinstance(default_config.queue, QueueConfig)

    def test_config_has_retry_settings(self, default_config):
        """Test config has retry configuration."""
        assert hasattr(default_config, 'retry')
        assert isinstance(default_config.retry, RetryConfig)

    def test_config_has_dlq_settings(self, default_config):
        """Test config has DLQ configuration."""
        assert hasattr(default_config, 'dlq')
        assert isinstance(default_config.dlq, DeadLetterQueueConfig)

    def test_config_has_lock_settings(self, default_config):
        """Test config has lock configuration."""
        assert hasattr(default_config, 'lock')
        assert isinstance(default_config.lock, LockConfig)


class TestWorkerPoolConfig:
    """Tests for WorkerPoolConfig."""

    def test_worker_pool_config_defaults(self):
        """Test default worker pool configuration values."""
        config = WorkerPoolConfig()
        
        assert config.min_workers >= 1
        assert config.max_workers >= config.min_workers
        assert config.heartbeat_interval > 0

    def test_worker_pool_config_custom_values(self):
        """Test custom worker pool configuration values."""
        config = WorkerPoolConfig(
            min_workers=2,
            max_workers=10,
            heartbeat_interval=10.0,
        )
        
        assert config.min_workers == 2
        assert config.max_workers == 10
        assert config.heartbeat_interval == 10.0

    def test_worker_pool_config_validation_min_workers(self):
        """Test min_workers must be positive."""
        config = WorkerPoolConfig(min_workers=0)
        with pytest.raises(ValueError):
            config.validate()

    def test_worker_pool_config_validation_max_less_than_min(self):
        """Test max_workers must be >= min_workers."""
        config = WorkerPoolConfig(min_workers=5, max_workers=2)
        with pytest.raises(ValueError):
            config.validate()

    def test_worker_pool_config_worker_type(self):
        """Test worker type configuration."""
        config = WorkerPoolConfig(worker_type="process")
        
        assert config.worker_type == "process"


class TestQueueConfig:
    """Tests for QueueConfig."""

    def test_queue_config_defaults(self):
        """Test default queue configuration values."""
        config = QueueConfig()
        
        assert hasattr(config, 'max_size')
        assert hasattr(config, 'default_priority')

    def test_queue_config_custom_max_size(self):
        """Test custom max size."""
        config = QueueConfig(max_size=1000)
        
        assert config.max_size == 1000

    def test_queue_config_unlimited(self):
        """Test unlimited queue size."""
        config = QueueConfig(max_size=None)
        
        assert config.max_size is None


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_retry_config_defaults(self):
        """Test default retry configuration values."""
        config = RetryConfig()
        
        assert config.max_retries >= 0
        assert config.base_delay > 0
        assert config.max_delay >= config.base_delay

    def test_retry_config_custom_values(self):
        """Test custom retry configuration values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=2.0,
            max_delay=120.0,
            exponential_base=3.0,
            jitter=True,
        )
        
        assert config.max_retries == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 120.0
        assert config.exponential_base == 3.0
        assert config.jitter is True

    def test_retry_config_no_jitter(self):
        """Test retry config without jitter."""
        config = RetryConfig(jitter=False)
        
        assert config.jitter is False

    def test_retry_config_validation(self):
        """Test retry config validation."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=60.0,
            exponential_base=2.0,
        )
        
        # Should not raise
        config.validate()

    def test_retry_config_validation_invalid_base_delay(self):
        """Test validation catches invalid base_delay."""
        config = RetryConfig(base_delay=0)
        with pytest.raises(ValueError):
            config.validate()


class TestDeadLetterQueueConfig:
    """Tests for DeadLetterQueueConfig."""

    def test_dlq_config_defaults(self):
        """Test default DLQ configuration values."""
        config = DeadLetterQueueConfig()
        
        assert config.enabled is True
        assert config.max_size > 0
        assert config.auto_cleanup_days > 0

    def test_dlq_config_disabled(self):
        """Test DLQ disabled configuration."""
        config = DeadLetterQueueConfig(enabled=False)
        
        assert config.enabled is False

    def test_dlq_config_custom_cleanup_days(self):
        """Test custom cleanup days for DLQ entries."""
        config = DeadLetterQueueConfig(auto_cleanup_days=14)
        
        assert config.auto_cleanup_days == 14

    def test_dlq_config_max_size(self):
        """Test maximum size configuration."""
        config = DeadLetterQueueConfig(max_size=100)
        
        assert config.max_size == 100


class TestLockConfig:
    """Tests for LockConfig."""

    def test_lock_config_defaults(self):
        """Test default lock configuration values."""
        config = LockConfig()
        
        assert config.default_expiry > 0
        assert config.default_timeout > 0

    def test_lock_config_custom_values(self):
        """Test custom lock configuration values."""
        config = LockConfig(
            default_expiry=60.0,
            default_timeout=10.0,
        )
        
        assert config.default_expiry == 60.0
        assert config.default_timeout == 10.0

    def test_lock_config_backend(self):
        """Test lock backend configuration."""
        config = LockConfig(backend="memory")
        
        assert config.backend == "memory"


class TestStorageConfig:
    """Tests for StorageConfig."""

    def test_storage_config_defaults(self):
        """Test default storage configuration values."""
        config = StorageConfig()
        
        assert hasattr(config, 'backend')

    def test_storage_config_memory_backend(self):
        """Test memory storage backend configuration."""
        config = StorageConfig(backend="memory")
        
        assert config.backend == "memory"

    def test_storage_config_redis_backend(self):
        """Test Redis storage backend configuration."""
        config = StorageConfig(
            backend="redis",
            redis_url="redis://localhost:6379",
        )
        
        assert config.backend == "redis"
        assert config.redis_url == "redis://localhost:6379"


class TestOrchestratorConfigFromEnv:
    """Tests for loading configuration from environment variables."""

    def test_config_from_env_workers(self):
        """Test loading worker count from environment."""
        with patch.dict(os.environ, {
            "JOB_ORCH_MIN_WORKERS": "4",
            "JOB_ORCH_MAX_WORKERS": "16",
        }):
            config = OrchestratorConfig.from_env()
            
            assert config.worker_pool.min_workers == 4
            assert config.worker_pool.max_workers == 16

    def test_config_from_env_retry(self):
        """Test loading retry config from environment."""
        with patch.dict(os.environ, {
            "JOB_ORCH_MAX_RETRIES": "5",
            "JOB_ORCH_RETRY_BASE_DELAY": "2.0",
        }):
            config = OrchestratorConfig.from_env()
            
            assert config.retry.max_retries == 5
            assert config.retry.base_delay == 2.0

    def test_config_from_env_missing_vars_use_defaults(self):
        """Test missing environment variables use defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = OrchestratorConfig.from_env()
            
            # Should use default values
            assert config.worker_pool.min_workers >= 1
            assert config.retry.max_retries >= 0


class TestOrchestratorConfigFromDict:
    """Tests for loading configuration from dictionary."""

    def test_config_from_dict(self):
        """Test loading config from dictionary."""
        data = {
            "worker_pool": {
                "min_workers": 2,
                "max_workers": 8,
            },
            "retry": {
                "max_retries": 3,
            },
        }
        
        config = OrchestratorConfig.from_dict(data)
        
        assert config.worker_pool.min_workers == 2
        assert config.worker_pool.max_workers == 8
        assert config.retry.max_retries == 3

    def test_config_from_dict_partial(self):
        """Test loading partial config from dictionary."""
        data = {
            "retry": {
                "max_retries": 5,
            },
        }
        
        config = OrchestratorConfig.from_dict(data)
        
        # Retry should be updated, others use defaults
        assert config.retry.max_retries == 5
        assert config.worker_pool.min_workers >= 1

    def test_config_from_dict_empty(self):
        """Test loading config from empty dictionary."""
        config = OrchestratorConfig.from_dict({})
        
        # Should use all defaults
        assert config.worker_pool.min_workers >= 1


class TestOrchestratorConfigToDict:
    """Tests for exporting configuration to dictionary."""

    def test_config_to_dict(self):
        """Test exporting config to dictionary."""
        config = OrchestratorConfig()
        data = config.to_dict()
        
        assert isinstance(data, dict)
        assert "worker_pool" in data
        assert "queue" in data
        assert "retry" in data
        assert "dlq" in data

    def test_config_round_trip(self):
        """Test config survives dict round-trip."""
        original = OrchestratorConfig(
            worker_pool=WorkerPoolConfig(min_workers=3, max_workers=12),
            retry=RetryConfig(max_retries=5),
        )
        
        data = original.to_dict()
        restored = OrchestratorConfig.from_dict(data)
        
        assert restored.worker_pool.min_workers == 3
        assert restored.worker_pool.max_workers == 12
        assert restored.retry.max_retries == 5


class TestOrchestratorConfigValidation:
    """Tests for configuration validation."""

    def test_validate_valid_config(self, default_config):
        """Test validation of valid config does not raise."""
        default_config.validate()  # Should not raise

    def test_validate_catches_invalid_workers(self):
        """Test validation catches invalid worker config."""
        config = OrchestratorConfig(
            worker_pool=WorkerPoolConfig(min_workers=10, max_workers=5)
        )
        with pytest.raises(ValueError):
            config.validate()

    def test_validate_catches_negative_retry_values(self):
        """Test validation catches negative retry values."""
        config = OrchestratorConfig(
            retry=RetryConfig(max_retries=-1)
        )
        with pytest.raises(ValueError):
            config.validate()


class TestOrchestratorConfigCopy:
    """Tests for copying configurations."""

    def test_copy_configs(self):
        """Test copying configuration."""
        from dataclasses import replace
        
        original = OrchestratorConfig(
            worker_pool=WorkerPoolConfig(min_workers=2, max_workers=10),
        )
        
        # Create a modified copy
        copy = OrchestratorConfig(
            worker_pool=WorkerPoolConfig(
                min_workers=original.worker_pool.min_workers,
                max_workers=20
            ),
            queue=original.queue,
            retry=original.retry,
            dlq=original.dlq,
            storage=original.storage,
            lock=original.lock,
        )
        
        # Modified values in copy
        assert copy.worker_pool.max_workers == 20
        # Original unchanged
        assert original.worker_pool.max_workers == 10


class TestConfigRepr:
    """Tests for string representation of configs."""

    def test_orchestrator_config_str(self, default_config):
        """Test string representation of OrchestratorConfig."""
        result = str(default_config)
        
        assert "OrchestratorConfig" in result or len(result) > 0

    def test_worker_pool_config_str(self):
        """Test string representation of WorkerPoolConfig."""
        config = WorkerPoolConfig(min_workers=2, max_workers=8)
        result = str(config)
        
        assert len(result) > 0

    def test_retry_config_str(self):
        """Test string representation of RetryConfig."""
        config = RetryConfig(max_retries=3)
        result = str(config)
        
        assert len(result) > 0


class TestConfigModification:
    """Tests for configuration modification."""

    def test_modifying_config_with_replace(self):
        """Test modifying config using dataclass replace."""
        from dataclasses import replace
        
        original = WorkerPoolConfig(min_workers=2, max_workers=8)
        modified = replace(original, min_workers=4)
        
        assert original.min_workers == 2
        assert modified.min_workers == 4