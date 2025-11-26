"""
Abstract base class for lock managers.

This module provides the abstract interface for distributed locking,
which is critical for preventing race conditions when multiple workers
try to process the same job or access shared resources.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, Optional
import uuid


@dataclass
class LockInfo:
    """
    Information about an acquired lock.
    
    Attributes:
        lock_id: Unique identifier for this lock acquisition.
        resource: The resource identifier that is locked.
        owner: Identifier of the entity that owns the lock.
        acquired_at: When the lock was acquired.
        expires_at: When the lock will expire (None = no expiration).
        metadata: Additional metadata associated with the lock.
    """
    lock_id: str
    resource: str
    owner: str
    acquired_at: datetime
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_expired(self) -> bool:
        """Check if the lock has expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at
    
    @property
    def remaining_ttl(self) -> Optional[float]:
        """Get remaining time-to-live in seconds, or None if no expiration."""
        if self.expires_at is None:
            return None
        remaining = (self.expires_at - datetime.utcnow()).total_seconds()
        return max(0.0, remaining)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize lock info to dictionary."""
        return {
            "lock_id": self.lock_id,
            "resource": self.resource,
            "owner": self.owner,
            "acquired_at": self.acquired_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LockInfo":
        """Deserialize lock info from dictionary."""
        return cls(
            lock_id=data["lock_id"],
            resource=data["resource"],
            owner=data["owner"],
            acquired_at=datetime.fromisoformat(data["acquired_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]) if data.get("expires_at") else None,
            metadata=data.get("metadata", {}),
        )


class LockManager(ABC):
    """
    Abstract base class for lock managers.
    
    Lock managers provide distributed locking functionality to prevent
    race conditions when multiple workers try to access shared resources.
    
    Implementations include:
    - InMemoryLockManager: Thread-safe locks for single-node deployments
    - RedisLockManager: Distributed locks using Redis (Redlock algorithm)
    - FileLockManager: File-based locks for multiprocessing
    
    Usage:
        lock_manager = InMemoryLockManager()
        
        # Method 1: Using context manager (recommended)
        with lock_manager.lock("resource-123") as lock_info:
            # Do work with the locked resource
            pass
        
        # Method 2: Manual acquire/release
        lock_info = lock_manager.acquire("resource-123", owner="worker-1")
        if lock_info:
            try:
                # Do work
                pass
            finally:
                lock_manager.release("resource-123", owner="worker-1")
    """
    
    @abstractmethod
    def acquire(
        self,
        resource: str,
        owner: Optional[str] = None,
        timeout: Optional[float] = None,
        ttl: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[LockInfo]:
        """
        Acquire a lock on a resource.
        
        Args:
            resource: Resource identifier to lock. This should be a unique
                string identifying the resource (e.g., "job:123", "queue:main").
            owner: Owner identifier. Defaults to a unique UUID if not provided.
                The same owner can re-acquire a lock they already hold.
            timeout: Maximum time in seconds to wait for lock acquisition.
                - None or 0: Return immediately if lock is not available
                - > 0: Block up to this many seconds waiting for the lock
            ttl: Lock time-to-live in seconds. The lock will automatically
                expire after this duration to prevent deadlocks from crashed
                processes. Default is 30 seconds.
            metadata: Optional metadata to store with the lock.
        
        Returns:
            LockInfo if the lock was acquired successfully, None otherwise.
        
        Example:
            # Non-blocking acquire
            lock = manager.acquire("job:123")
            if lock:
                print(f"Acquired lock: {lock.lock_id}")
            
            # Blocking acquire with timeout
            lock = manager.acquire("job:456", timeout=5.0, ttl=60.0)
        """
        pass
    
    @abstractmethod
    def release(self, resource: str, owner: Optional[str] = None) -> bool:
        """
        Release a lock on a resource.
        
        A lock can only be released by its owner. If owner is not specified,
        the lock will be released regardless of owner (use with caution).
        
        Args:
            resource: Resource identifier to unlock.
            owner: Owner identifier. If provided, the lock will only be
                released if it's currently held by this owner.
        
        Returns:
            True if the lock was released successfully, False if:
            - The lock doesn't exist
            - The lock is held by a different owner
        
        Example:
            released = manager.release("job:123", owner="worker-1")
            if not released:
                print("Failed to release lock - not held by this owner")
        """
        pass
    
    @abstractmethod
    def extend(self, resource: str, owner: str, ttl: float) -> bool:
        """
        Extend the TTL of an existing lock.
        
        This is useful for long-running operations where you need to keep
        the lock alive beyond the original TTL.
        
        Args:
            resource: Resource identifier.
            owner: Owner identifier. Must match the current lock owner.
            ttl: New time-to-live in seconds from now.
        
        Returns:
            True if the lock was extended, False if:
            - The lock doesn't exist
            - The lock is held by a different owner
            - The lock has already expired
        
        Example:
            # Extend lock by another 60 seconds
            if not manager.extend("job:123", owner="worker-1", ttl=60.0):
                # Lock lost - need to re-acquire
                pass
        """
        pass
    
    @abstractmethod
    def is_locked(self, resource: str) -> bool:
        """
        Check if a resource is currently locked.
        
        Note: This is a point-in-time check. The lock status may change
        immediately after this call returns.
        
        Args:
            resource: Resource identifier to check.
        
        Returns:
            True if the resource is currently locked, False otherwise.
        """
        pass
    
    @abstractmethod
    def get_lock_info(self, resource: str) -> Optional[LockInfo]:
        """
        Get information about a lock.
        
        Args:
            resource: Resource identifier.
        
        Returns:
            LockInfo if the resource is currently locked, None otherwise.
        """
        pass
    
    @contextmanager
    def lock(
        self,
        resource: str,
        owner: Optional[str] = None,
        timeout: Optional[float] = None,
        ttl: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Generator[LockInfo, None, None]:
        """
        Context manager for automatic lock acquisition and release.
        
        This is the recommended way to use locks as it ensures the lock
        is always released, even if an exception occurs.
        
        Args:
            resource: Resource identifier to lock.
            owner: Owner identifier. Defaults to a unique UUID.
            timeout: Maximum time to wait for lock acquisition.
            ttl: Lock time-to-live in seconds.
            metadata: Optional metadata to store with the lock.
        
        Yields:
            LockInfo for the acquired lock.
        
        Raises:
            LockAcquisitionError: If the lock cannot be acquired.
        
        Example:
            with lock_manager.lock("job:123", ttl=60.0) as lock_info:
                print(f"Acquired lock: {lock_info.lock_id}")
                # Do work...
            # Lock is automatically released here
        """
        from ..core.exceptions import LockAcquisitionError
        
        # Generate owner if not provided
        if owner is None:
            owner = str(uuid.uuid4())
        
        lock_info = self.acquire(
            resource=resource,
            owner=owner,
            timeout=timeout,
            ttl=ttl,
            metadata=metadata,
        )
        
        if lock_info is None:
            raise LockAcquisitionError(
                lock_name=resource,
                timeout=timeout,
                owner=owner,
            )
        
        try:
            yield lock_info
        finally:
            self.release(resource, lock_info.owner)
    
    def try_lock(
        self,
        resource: str,
        owner: Optional[str] = None,
        ttl: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[LockInfo]:
        """
        Try to acquire a lock without blocking.
        
        This is a convenience method equivalent to calling acquire with
        timeout=0.
        
        Args:
            resource: Resource identifier to lock.
            owner: Owner identifier.
            ttl: Lock time-to-live in seconds.
            metadata: Optional metadata to store with the lock.
        
        Returns:
            LockInfo if acquired, None if the lock is already held.
        """
        return self.acquire(
            resource=resource,
            owner=owner,
            timeout=0,
            ttl=ttl,
            metadata=metadata,
        )
    
    def force_release(self, resource: str) -> bool:
        """
        Force release a lock regardless of owner.
        
        Use with caution - this can cause issues if another process
        thinks it still holds the lock.
        
        Args:
            resource: Resource identifier to unlock.
        
        Returns:
            True if a lock was released, False if no lock existed.
        """
        return self.release(resource, owner=None)


__all__ = [
    "LockInfo",
    "LockManager",
]