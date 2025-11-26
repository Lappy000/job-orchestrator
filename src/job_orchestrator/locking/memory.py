"""
In-memory lock manager for single-node deployments.

This module provides a thread-safe lock manager that stores locks in memory.
It's suitable for single-node deployments where all workers are in the same
process or when testing locally.
"""

import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .base import LockInfo, LockManager


class InMemoryLockManager(LockManager):
    """
    Thread-safe in-memory lock manager for single-node deployments.
    
    This implementation uses Python's threading primitives to provide
    safe concurrent access to locks. It's suitable for:
    - Development and testing
    - Single-node deployments with thread-based workers
    - Applications where persistence isn't required
    
    Features:
    - Thread-safe lock acquisition and release
    - Optional blocking with timeout
    - Automatic lock expiration (TTL)
    - Lock reentrance (owner can re-acquire their own lock)
    - Periodic cleanup of expired locks
    
    Limitations:
    - Locks are not persistent - they're lost on process restart
    - Not suitable for distributed deployments across multiple nodes
    - Memory usage grows with number of active locks
    
    Example:
        lock_manager = InMemoryLockManager()
        
        with lock_manager.lock("job:123", owner="worker-1", ttl=60.0):
            # Do work with exclusive access
            pass
    """
    
    def __init__(
        self,
        cleanup_interval: float = 60.0,
        default_ttl: float = 30.0,
        default_timeout: float = 0.0,
    ):
        """
        Initialize the in-memory lock manager.
        
        Args:
            cleanup_interval: Interval in seconds between automatic
                cleanup of expired locks. Set to 0 to disable automatic
                cleanup.
            default_ttl: Default lock time-to-live in seconds.
            default_timeout: Default acquire timeout in seconds.
        """
        self._locks: Dict[str, LockInfo] = {}
        self._lock = threading.RLock()
        self._conditions: Dict[str, threading.Condition] = {}
        self._cleanup_interval = cleanup_interval
        self._shutdown_event = threading.Event()
        self._cleanup_thread: Optional[threading.Thread] = None
        self.default_ttl = default_ttl
        self.default_timeout = default_timeout
        
        if cleanup_interval > 0:
            self._start_cleanup_thread()
    
    def _start_cleanup_thread(self) -> None:
        """Start the background cleanup thread."""
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="lock-cleanup"
        )
        self._cleanup_thread.start()
    
    def _cleanup_loop(self) -> None:
        """Background loop that periodically cleans up expired locks."""
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(timeout=self._cleanup_interval)
            if not self._shutdown_event.is_set():
                self.cleanup_expired()
    
    def shutdown(self) -> None:
        """Shutdown the lock manager and stop cleanup thread."""
        self._shutdown_event.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5.0)
    
    def _get_condition(self, resource: str) -> threading.Condition:
        """Get or create a condition variable for a resource."""
        if resource not in self._conditions:
            self._conditions[resource] = threading.Condition(self._lock)
        return self._conditions[resource]
    
    def _is_expired(self, lock_info: LockInfo) -> bool:
        """Check if a lock has expired."""
        if lock_info.expires_at is None:
            return False
        return datetime.utcnow() > lock_info.expires_at
    
    def acquire(
        self,
        resource: str,
        owner: Optional[str] = None,
        timeout: Optional[float] = None,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[LockInfo]:
        """
        Acquire a lock on a resource with optional blocking wait.
        
        If the lock is already held by the same owner, this returns
        the existing lock info (reentrant behavior).
        
        Args:
            resource: Resource identifier to lock.
            owner: Owner identifier. Defaults to unique UUID.
            timeout: Maximum time to wait for lock.
                - None or 0: Return immediately if lock unavailable
                - > 0: Block up to this many seconds
            ttl: Lock time-to-live in seconds.
            metadata: Optional metadata to store with the lock.
        
        Returns:
            LockInfo if acquired, None if failed.
        """
        owner = owner or str(uuid.uuid4())
        effective_timeout = (
            timeout
            if timeout is not None
            else (self.default_timeout if self.default_timeout > 0 else None)
        )
        deadline = (
            time.time() + effective_timeout
            if effective_timeout and effective_timeout > 0
            else None
        )
        
        while True:
            with self._lock:
                # Check if lock is free or expired
                existing = self._locks.get(resource)
                
                if existing is None or self._is_expired(existing):
                    # Lock is available - acquire it
                    effective_ttl = ttl if ttl is not None else self.default_ttl
                    lock_info = LockInfo(
                        lock_id=str(uuid.uuid4()),
                        resource=resource,
                        owner=owner,
                        acquired_at=datetime.utcnow(),
                        expires_at=(
                            datetime.utcnow() + timedelta(seconds=effective_ttl)
                            if effective_ttl and effective_ttl > 0
                            else None
                        ),
                        metadata=metadata or {},
                    )
                    self._locks[resource] = lock_info
                    return lock_info
                
                # Check if we already own the lock (reentrant)
                if existing.owner == owner:
                    # Extend the lock TTL and return
                    effective_ttl = ttl if ttl is not None else self.default_ttl
                    if effective_ttl and effective_ttl > 0:
                        existing.expires_at = datetime.utcnow() + timedelta(seconds=effective_ttl)
                    return existing
                
                # Lock is held by someone else
                if effective_timeout is None or effective_timeout <= 0:
                    return None  # Don't wait
                
                # Check deadline
                if deadline and time.time() >= deadline:
                    return None
                
                # Wait for lock to become available
                condition = self._get_condition(resource)
                remaining = deadline - time.time() if deadline else None
                condition.wait(timeout=min(remaining, 0.1) if remaining else 0.1)
            
            # Check deadline again after waiting
            if deadline and time.time() >= deadline:
                return None
    
    def release(self, resource: str, owner: Optional[str] = None) -> bool:
        """
        Release a lock on a resource.
        
        Args:
            resource: Resource identifier to unlock.
            owner: Owner identifier. If provided, only releases if
                the lock is held by this owner.
        
        Returns:
            True if released, False if not held or wrong owner.
        """
        with self._lock:
            existing = self._locks.get(resource)
            
            if existing is None:
                return False
            
            if owner is not None and existing.owner != owner:
                return False  # Not the owner
            
            del self._locks[resource]
            
            # Notify waiters
            if resource in self._conditions:
                self._conditions[resource].notify_all()
            
            return True
    
    def extend(self, resource: str, owner: str, ttl: float) -> bool:
        """
        Extend the TTL of an existing lock.
        
        Args:
            resource: Resource identifier.
            owner: Owner identifier. Must match current owner.
            ttl: New time-to-live in seconds from now.
        
        Returns:
            True if extended, False if lock not held by owner.
        """
        with self._lock:
            existing = self._locks.get(resource)
            
            if existing is None:
                return False
            
            if existing.owner != owner:
                return False
            
            if self._is_expired(existing):
                # Lock has expired, remove it
                del self._locks[resource]
                return False
            
            # Extend the TTL
            existing.expires_at = datetime.utcnow() + timedelta(seconds=ttl) if ttl > 0 else None
            return True
    
    def is_locked(self, resource: str) -> bool:
        """
        Check if a resource is currently locked.
        
        Args:
            resource: Resource identifier.
        
        Returns:
            True if locked (and not expired), False otherwise.
        """
        with self._lock:
            existing = self._locks.get(resource)
            
            if existing is None:
                return False
            
            if self._is_expired(existing):
                # Clean up expired lock
                del self._locks[resource]
                return False
            
            return True
    
    def get_lock_info(self, resource: str) -> Optional[LockInfo]:
        """
        Get information about a lock.
        
        Args:
            resource: Resource identifier.
        
        Returns:
            LockInfo if locked, None otherwise.
        """
        with self._lock:
            existing = self._locks.get(resource)
            
            if existing is None:
                return None
            
            if self._is_expired(existing):
                # Clean up expired lock
                del self._locks[resource]
                return None
            
            return existing
    
    def cleanup_expired(self) -> int:
        """
        Remove all expired locks.
        
        This is called automatically by the cleanup thread, but can
        also be called manually.
        
        Returns:
            Number of expired locks that were removed.
        """
        with self._lock:
            expired_resources = [
                resource
                for resource, lock_info in self._locks.items()
                if self._is_expired(lock_info)
            ]
            
            for resource in expired_resources:
                del self._locks[resource]
                # Notify waiters that lock is now available
                if resource in self._conditions:
                    self._conditions[resource].notify_all()
            
            return len(expired_resources)
    
    def get_all_locks(self) -> List[LockInfo]:
        """
        Get all currently active locks.
        
        Returns:
            List of LockInfo for all non-expired locks.
        """
        with self._lock:
            # Clean up expired locks first
            self.cleanup_expired()
            return list(self._locks.values())
    
    def clear(self) -> int:
        """
        Clear all locks.
        
        Use with caution - this will release all locks regardless of owner.
        
        Returns:
            Number of locks that were cleared.
        """
        with self._lock:
            count = len(self._locks)
            self._locks.clear()
            
            # Notify all waiters
            for condition in self._conditions.values():
                condition.notify_all()
            
            return count
    
    @property
    def lock_count(self) -> int:
        """Get the number of currently active locks."""
        with self._lock:
            # Don't count expired locks
            return sum(
                1 for lock_info in self._locks.values()
                if not self._is_expired(lock_info)
            )
    
    def __len__(self) -> int:
        """Get the number of currently active locks."""
        return self.lock_count
    
    def __contains__(self, resource: str) -> bool:
        """Check if a resource is locked."""
        return self.is_locked(resource)
    
    def __enter__(self) -> "InMemoryLockManager":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - shutdown the lock manager."""
        self.shutdown()


__all__ = [
    "InMemoryLockManager",
]