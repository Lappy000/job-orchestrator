"""
Locking module for the Job Orchestrator.

Provides distributed locking mechanisms for multi-node deployments
to prevent race conditions when multiple workers try to process the
same job or access shared resources.

Available Lock Managers:
- InMemoryLockManager: Thread-safe locks for single-node deployments
- RedisLockManager: Distributed locks using Redis (Redlock algorithm)
- FileLockManager: File-based locks for multiprocessing scenarios

Usage:
    from job_orchestrator.locking import InMemoryLockManager, LockInfo
    
    # Create a lock manager
    lock_manager = InMemoryLockManager()
    
    # Method 1: Using context manager (recommended)
    with lock_manager.lock("job:123", owner="worker-1", ttl=60.0) as lock_info:
        # Do work with exclusive access to the resource
        print(f"Acquired lock: {lock_info.lock_id}")
    
    # Method 2: Manual acquire/release
    lock_info = lock_manager.acquire("job:456", owner="worker-1", ttl=30.0)
    if lock_info:
        try:
            # Do work
            pass
        finally:
            lock_manager.release("job:456", owner="worker-1")

For Redis-based distributed locking:
    from job_orchestrator.locking import RedisLockManager
    
    # Single Redis instance
    import redis
    client = redis.Redis(host='localhost', port=6379)
    lock_manager = RedisLockManager(redis_client=client)
    
    # Multiple Redis instances (Redlock algorithm)
    lock_manager = RedisLockManager(
        redis_urls=[
            "redis://host1:6379",
            "redis://host2:6379",
            "redis://host3:6379"
        ]
    )

For file-based locking (multiprocessing without Redis):
    from job_orchestrator.locking import FileLockManager
    
    lock_manager = FileLockManager(lock_dir="/tmp/my_app_locks")
    
    with lock_manager.lock("resource:123"):
        # Do work with exclusive access
        pass
"""

from .base import LockInfo, LockManager
from .memory import InMemoryLockManager
from .redis_lock import RedisLockManager
from .file_lock import FileLockManager

__all__ = [
    # Base classes
    "LockManager",
    "LockInfo",
    # Implementations
    "InMemoryLockManager",
    "RedisLockManager",
    "FileLockManager",
]