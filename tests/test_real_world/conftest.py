"""
Shared fixtures for real-world scenario tests.

These fixtures provide common setup for testing realistic use cases
of the job-orchestrator library.
"""

import pytest
import threading
from datetime import datetime
from typing import Dict, List, Any

from job_orchestrator import (
    Job,
    JobState,
    JobPriority,
    DAG,
    DAGBuilder,
    Scheduler,
    OrchestratorConfig,
)
from job_orchestrator.core.config import WorkerPoolConfig, RetryConfig
from job_orchestrator.locking.memory import InMemoryLockManager


# Default timeout for all tests in this module (30 seconds)
def pytest_collection_modifyitems(items):
    """Add timeout marker to all tests to prevent hanging."""
    for item in items:
        if "test_real_world" in str(item.fspath):
            # Add 30 second timeout to prevent hanging tests
            if not any(mark.name == "timeout" for mark in item.iter_markers()):
                item.add_marker(pytest.mark.timeout(30))


@pytest.fixture
def basic_scheduler():
    """Create a basic scheduler for simple tests."""
    scheduler = Scheduler()
    scheduler.start()
    yield scheduler
    scheduler.stop()


@pytest.fixture
def configured_scheduler():
    """Create a scheduler with production-like configuration."""
    config = OrchestratorConfig.from_dict({
        "worker_pool": {
            "min_workers": 2,
            "max_workers": 8,
        },
        "retry": {
            "max_retries": 3,
            "base_delay": 0.1,
            "max_delay": 5.0,
            "exponential_base": 2.0,
            "jitter": True,
        },
        "dlq": {
            "enabled": True,
            "max_size": 1000,
        },
    })
    
    scheduler = Scheduler(config)
    scheduler.start()
    yield scheduler
    scheduler.stop()


@pytest.fixture
def lock_manager():
    """Create an in-memory lock manager for distributed locking tests."""
    return InMemoryLockManager()


@pytest.fixture
def thread_safe_counter():
    """Create a thread-safe counter for testing concurrent operations."""
    class ThreadSafeCounter:
        def __init__(self):
            self._value = 0
            self._lock = threading.Lock()
        
        def increment(self, by: int = 1) -> int:
            with self._lock:
                self._value += by
                return self._value
        
        def decrement(self, by: int = 1) -> int:
            with self._lock:
                self._value -= by
                return self._value
        
        @property
        def value(self) -> int:
            with self._lock:
                return self._value
        
        def reset(self) -> None:
            with self._lock:
                self._value = 0
    
    return ThreadSafeCounter()


@pytest.fixture
def execution_tracker():
    """Create a tracker for monitoring job execution order."""
    class ExecutionTracker:
        def __init__(self):
            self._executions: List[Dict[str, Any]] = []
            self._lock = threading.Lock()
        
        def record(self, name: str, **kwargs) -> None:
            with self._lock:
                self._executions.append({
                    "name": name,
                    "timestamp": datetime.utcnow(),
                    **kwargs
                })
        
        def get_order(self) -> List[str]:
            with self._lock:
                return [e["name"] for e in self._executions]
        
        def get_all(self) -> List[Dict[str, Any]]:
            with self._lock:
                return list(self._executions)
        
        def clear(self) -> None:
            with self._lock:
                self._executions.clear()
        
        def count(self) -> int:
            with self._lock:
                return len(self._executions)
    
    return ExecutionTracker()


@pytest.fixture
def mock_external_api():
    """Create a mock external API for testing service integrations."""
    class MockExternalAPI:
        def __init__(self, failure_rate: float = 0.0, latency: float = 0.01):
            self.failure_rate = failure_rate
            self.latency = latency
            self.call_count = 0
            self.last_request = None
            self._lock = threading.Lock()
        
        def call(self, endpoint: str, data: Dict = None) -> Dict[str, Any]:
            import time
            import random
            
            with self._lock:
                self.call_count += 1
                self.last_request = {"endpoint": endpoint, "data": data}
            
            time.sleep(self.latency)
            
            if random.random() < self.failure_rate:
                raise ConnectionError(f"API call to {endpoint} failed")
            
            return {
                "status": "success",
                "endpoint": endpoint,
                "data": data,
            }
        
        def reset(self) -> None:
            with self._lock:
                self.call_count = 0
                self.last_request = None
    
    return MockExternalAPI()


@pytest.fixture
def sample_order_data():
    """Create sample e-commerce order data for testing."""
    return {
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "items": [
            {"product_id": "PROD-001", "quantity": 2, "price": 29.99},
            {"product_id": "PROD-002", "quantity": 1, "price": 49.99},
        ],
        "total": 109.97,
        "shipping_address": {
            "street": "123 Test St",
            "city": "Testville",
            "zip": "12345",
        },
    }


@pytest.fixture
def sample_pipeline_data():
    """Create sample data pipeline records for testing."""
    import random
    
    return [
        {
            "id": f"record_{i:04d}",
            "source": random.choice(["database", "api", "file"]),
            "value": random.uniform(0, 1000),
            "category": random.choice(["A", "B", "C"]),
            "timestamp": datetime.utcnow().isoformat(),
        }
        for i in range(100)
    ]


@pytest.fixture
def sample_service_config():
    """Create sample microservice configuration for testing."""
    return {
        "services": [
            {
                "name": "api-gateway",
                "version": "1.0.0",
                "replicas": 2,
                "depends_on": [],
            },
            {
                "name": "auth-service",
                "version": "1.0.0",
                "replicas": 2,
                "depends_on": ["api-gateway"],
            },
            {
                "name": "order-service",
                "version": "1.0.0",
                "replicas": 3,
                "depends_on": ["api-gateway", "auth-service"],
            },
            {
                "name": "inventory-service",
                "version": "1.0.0",
                "replicas": 2,
                "depends_on": ["api-gateway"],
            },
            {
                "name": "notification-service",
                "version": "1.0.0",
                "replicas": 1,
                "depends_on": ["order-service"],
            },
        ],
    }
