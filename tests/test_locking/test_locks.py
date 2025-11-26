"""
Tests for Lock Manager implementations.

This module tests the InMemoryLockManager, lock acquisition,
TTL expiration, and concurrent locking operations.
"""

import pytest
import time
import threading
from uuid import uuid4
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from job_orchestrator.locking.memory import InMemoryLockManager
from job_orchestrator.locking.base import LockManager, LockInfo


class TestLockManagerCreation:
    """Tests for LockManager creation and initialization."""

    def test_memory_lock_manager_creation(self, memory_lock_manager):
        """Test creating an in-memory lock manager."""
        assert memory_lock_manager is not None
        assert isinstance(memory_lock_manager, InMemoryLockManager)

    def test_lock_manager_with_default_ttl(self):
        """Test creating lock manager with default TTL."""
        manager = InMemoryLockManager(default_ttl=60.0)
        
        assert manager.default_ttl == 60.0

    def test_lock_manager_with_default_timeout(self):
        """Test creating lock manager with default timeout."""
        manager = InMemoryLockManager(default_timeout=10.0)
        
        assert manager.default_timeout == 10.0


class TestAcquireRelease:
    """Tests for basic lock acquire and release."""

    def test_acquire_lock(self, memory_lock_manager):
        """Test acquiring a lock returns LockInfo."""
        lock_id = "test_lock"
        owner = "test_owner"
        
        # acquire() returns LockInfo or None, not bool
        lock_info = memory_lock_manager.acquire(lock_id, owner=owner)
        
        assert lock_info is not None
        assert isinstance(lock_info, LockInfo)
        assert lock_info.resource == lock_id
        assert lock_info.owner == owner
        
        memory_lock_manager.release(lock_id, owner=owner)

    def test_release_lock(self, memory_lock_manager):
        """Test releasing a lock."""
        lock_id = "test_lock"
        owner = "test_owner"
        
        memory_lock_manager.acquire(lock_id, owner=owner)
        released = memory_lock_manager.release(lock_id, owner=owner)
        
        assert released is True

    def test_acquire_after_release(self, memory_lock_manager):
        """Test acquiring a lock after it was released."""
        lock_id = "test_lock"
        owner = "test_owner"
        
        memory_lock_manager.acquire(lock_id, owner=owner)
        memory_lock_manager.release(lock_id, owner=owner)
        
        lock_info = memory_lock_manager.acquire(lock_id, owner="new_owner")
        
        assert lock_info is not None
        memory_lock_manager.release(lock_id, owner="new_owner")

    def test_is_locked(self, memory_lock_manager):
        """Test checking if a lock is held."""
        lock_id = "test_lock"
        owner = "test_owner"
        
        assert memory_lock_manager.is_locked(lock_id) is False
        
        memory_lock_manager.acquire(lock_id, owner=owner)
        
        assert memory_lock_manager.is_locked(lock_id) is True
        
        memory_lock_manager.release(lock_id, owner=owner)

    def test_get_lock_info(self, memory_lock_manager):
        """Test getting lock info."""
        lock_id = "test_lock"
        owner = "test_owner"
        
        memory_lock_manager.acquire(lock_id, owner=owner)
        
        lock_info = memory_lock_manager.get_lock_info(lock_id)
        
        assert lock_info is not None
        assert lock_info.owner == owner
        memory_lock_manager.release(lock_id, owner=owner)


class TestLockContention:
    """Tests for concurrent lock attempts."""

    def test_second_acquire_fails(self, memory_lock_manager):
        """Test second acquire attempt returns None."""
        lock_id = "contended_lock"
        
        memory_lock_manager.acquire(lock_id, owner="owner1")
        
        # Second acquire should return None
        lock_info = memory_lock_manager.acquire(lock_id, owner="owner2", timeout=0)
        
        assert lock_info is None
        memory_lock_manager.release(lock_id, owner="owner1")

    def test_blocking_acquire(self, memory_lock_manager):
        """Test blocking acquire waits for lock."""
        lock_id = "blocking_lock"
        acquired_in_thread = [None]
        
        memory_lock_manager.acquire(lock_id, owner="owner1")
        
        def wait_and_release():
            time.sleep(0.2)
            memory_lock_manager.release(lock_id, owner="owner1")
        
        def try_acquire():
            result = memory_lock_manager.acquire(lock_id, owner="owner2", timeout=1.0)
            acquired_in_thread[0] = result
        
        release_thread = threading.Thread(target=wait_and_release)
        acquire_thread = threading.Thread(target=try_acquire)
        
        release_thread.start()
        acquire_thread.start()
        
        release_thread.join()
        acquire_thread.join()
        
        # Should have acquired (non-None result)
        assert acquired_in_thread[0] is not None
        memory_lock_manager.release(lock_id, owner="owner2")

    def test_concurrent_acquire_one_wins(self, memory_lock_manager):
        """Test concurrent acquire - only one succeeds."""
        lock_id = "race_lock"
        results = []
        lock = threading.Lock()
        
        def try_acquire(owner):
            result = memory_lock_manager.acquire(lock_id, owner=owner, timeout=0)
            with lock:
                results.append((owner, result))
        
        threads = [
            threading.Thread(target=try_acquire, args=(f"owner_{i}",))
            for i in range(5)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Count non-None results (successful acquisitions)
        successful = sum(1 for _, result in results if result is not None)
        
        assert successful == 1
        
        # Release the lock
        winner = next(owner for owner, result in results if result is not None)
        memory_lock_manager.release(lock_id, owner=winner)


class TestLockTimeout:
    """Tests for lock acquisition timeout."""

    def test_acquire_timeout_zero(self, memory_lock_manager):
        """Test acquire with zero timeout is non-blocking."""
        lock_id = "timeout_lock"
        
        memory_lock_manager.acquire(lock_id, owner="owner1")
        
        start = time.time()
        lock_info = memory_lock_manager.acquire(lock_id, owner="owner2", timeout=0)
        elapsed = time.time() - start
        
        assert lock_info is None
        assert elapsed < 0.1
        
        memory_lock_manager.release(lock_id, owner="owner1")

    def test_acquire_timeout_waits(self, memory_lock_manager):
        """Test acquire waits for specified timeout."""
        lock_id = "timeout_lock"
        
        memory_lock_manager.acquire(lock_id, owner="owner1")
        
        start = time.time()
        lock_info = memory_lock_manager.acquire(lock_id, owner="owner2", timeout=0.3)
        elapsed = time.time() - start
        
        assert lock_info is None
        assert elapsed >= 0.25  # Allow some tolerance
        
        memory_lock_manager.release(lock_id, owner="owner1")

    def test_acquire_succeeds_before_timeout(self, memory_lock_manager):
        """Test acquire succeeds when lock released before timeout."""
        lock_id = "timeout_lock"
        
        memory_lock_manager.acquire(lock_id, owner="owner1")
        
        def release_soon():
            time.sleep(0.1)
            memory_lock_manager.release(lock_id, owner="owner1")
        
        threading.Thread(target=release_soon).start()
        
        start = time.time()
        lock_info = memory_lock_manager.acquire(lock_id, owner="owner2", timeout=1.0)
        elapsed = time.time() - start
        
        assert lock_info is not None
        assert elapsed < 0.5
        
        memory_lock_manager.release(lock_id, owner="owner2")


class TestLockTTLExpiration:
    """Tests for lock TTL expiration."""

    def test_lock_expires_after_ttl(self):
        """Test lock automatically expires after TTL."""
        manager = InMemoryLockManager(default_ttl=0.2)
        lock_id = "expiring_lock"
        
        manager.acquire(lock_id, owner="owner1")
        
        assert manager.is_locked(lock_id) is True
        
        time.sleep(0.3)
        
        # Lock should have expired
        assert manager.is_locked(lock_id) is False

    def test_can_acquire_after_expiration(self):
        """Test can acquire lock after previous owner's TTL expires."""
        manager = InMemoryLockManager(default_ttl=0.1)
        lock_id = "expiring_lock"
        
        manager.acquire(lock_id, owner="owner1")
        
        time.sleep(0.2)
        
        # Should be able to acquire now
        lock_info = manager.acquire(lock_id, owner="owner2", timeout=0)
        
        assert lock_info is not None
        manager.release(lock_id, owner="owner2")

    def test_custom_ttl_per_lock(self, memory_lock_manager):
        """Test custom TTL for specific lock."""
        lock_id = "custom_ttl_lock"
        
        memory_lock_manager.acquire(lock_id, owner="owner1", ttl=0.1)
        
        time.sleep(0.2)
        
        # Should be able to acquire after custom TTL
        lock_info = memory_lock_manager.acquire(lock_id, owner="owner2", timeout=0)
        
        assert lock_info is not None


class TestReentrantLock:
    """Tests for reentrant (same owner) lock behavior."""

    def test_same_owner_can_reacquire(self, memory_lock_manager):
        """Test same owner can re-acquire lock."""
        lock_id = "reentrant_lock"
        owner = "owner1"
        
        lock_info1 = memory_lock_manager.acquire(lock_id, owner=owner)
        lock_info2 = memory_lock_manager.acquire(lock_id, owner=owner)
        
        assert lock_info1 is not None
        assert lock_info2 is not None
        
        memory_lock_manager.release(lock_id, owner=owner)


class TestLockExtension:
    """Tests for extending lock TTL."""

    def test_extend_lock(self, memory_lock_manager):
        """Test extending lock TTL."""
        lock_id = "extend_lock"
        owner = "owner1"
        
        memory_lock_manager.acquire(lock_id, owner=owner, ttl=1.0)
        
        extended = memory_lock_manager.extend(lock_id, owner=owner, ttl=2.0)
        
        assert extended is True
        
        memory_lock_manager.release(lock_id, owner=owner)

    def test_extend_by_wrong_owner_fails(self, memory_lock_manager):
        """Test extending lock by wrong owner fails."""
        lock_id = "extend_lock"
        
        memory_lock_manager.acquire(lock_id, owner="owner1")
        
        extended = memory_lock_manager.extend(lock_id, owner="owner2", ttl=2.0)
        
        assert extended is False
        memory_lock_manager.release(lock_id, owner="owner1")


class TestMultipleLocks:
    """Tests for managing multiple locks."""

    def test_multiple_locks(self, memory_lock_manager):
        """Test acquiring multiple different locks."""
        owner = "owner1"
        
        for i in range(5):
            lock_id = f"lock_{i}"
            lock_info = memory_lock_manager.acquire(lock_id, owner=owner)
            assert lock_info is not None
        
        for i in range(5):
            memory_lock_manager.release(f"lock_{i}", owner=owner)

    def test_get_all_locks(self, memory_lock_manager):
        """Test getting all held locks."""
        owner = "owner1"
        
        for i in range(3):
            memory_lock_manager.acquire(f"lock_{i}", owner=owner)
        
        all_locks = memory_lock_manager.get_all_locks()
        
        assert len(all_locks) == 3
        
        for i in range(3):
            memory_lock_manager.release(f"lock_{i}", owner=owner)


class TestThreadSafety:
    """Tests for thread-safe lock operations."""

    def test_concurrent_different_locks(self, memory_lock_manager):
        """Test concurrent acquisition of different locks."""
        results = []
        lock = threading.Lock()
        
        def acquire_lock(i):
            lock_info = memory_lock_manager.acquire(f"lock_{i}", owner=f"owner_{i}")
            with lock:
                results.append(lock_info is not None)
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(acquire_lock, range(10))
        
        assert all(results)
        
        for i in range(10):
            memory_lock_manager.release(f"lock_{i}", owner=f"owner_{i}")

    def test_stress_test(self, memory_lock_manager):
        """Stress test concurrent lock operations."""
        lock_id = "stress_lock"
        success_count = [0]
        total_count = [0]
        results_lock = threading.Lock()
        owners = []
        
        def operate(i):
            owner = f"owner_{i}_{uuid4()}"
            lock_info = memory_lock_manager.acquire(lock_id, owner=owner, timeout=0.1)
            with results_lock:
                total_count[0] += 1
                if lock_info is not None:
                    success_count[0] += 1
                    owners.append(owner)
        
        threads = [threading.Thread(target=operate, args=(i,)) for i in range(50)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # At least some should succeed
        assert success_count[0] >= 1
        
        # Clean up
        for owner in owners:
            memory_lock_manager.release(lock_id, owner=owner)


class TestLockCleanup:
    """Tests for lock cleanup operations."""

    def test_cleanup_expired_locks(self):
        """Test cleaning up expired locks."""
        manager = InMemoryLockManager(default_ttl=0.1, cleanup_interval=0)
        
        for i in range(5):
            manager.acquire(f"lock_{i}", owner="owner")
        
        time.sleep(0.2)
        
        cleaned = manager.cleanup_expired()
        
        assert cleaned >= 5

    def test_lock_count(self, memory_lock_manager):
        """Test getting lock count."""
        owner = "owner1"
        
        assert memory_lock_manager.lock_count == 0
        
        for i in range(3):
            memory_lock_manager.acquire(f"lock_{i}", owner=owner)
        
        assert memory_lock_manager.lock_count == 3
        
        for i in range(3):
            memory_lock_manager.release(f"lock_{i}", owner=owner)

    def test_clear_all_locks(self, memory_lock_manager):
        """Test clearing all locks."""
        owner = "owner1"
        
        for i in range(3):
            memory_lock_manager.acquire(f"lock_{i}", owner=owner)
        
        cleared = memory_lock_manager.clear()
        
        assert cleared == 3
        assert memory_lock_manager.lock_count == 0


class TestLockRepr:
    """Tests for string representation."""

    def test_lock_manager_repr(self, memory_lock_manager):
        """Test repr of lock manager."""
        result = repr(memory_lock_manager)
        
        assert len(result) > 0

    def test_lock_info_attributes(self, memory_lock_manager):
        """Test LockInfo has expected attributes."""
        lock_id = "test_lock"
        owner = "test_owner"
        
        lock_info = memory_lock_manager.acquire(lock_id, owner=owner)
        
        assert hasattr(lock_info, 'lock_id')
        assert hasattr(lock_info, 'resource')
        assert hasattr(lock_info, 'owner')
        assert hasattr(lock_info, 'acquired_at')
        assert hasattr(lock_info, 'expires_at')
        
        memory_lock_manager.release(lock_id, owner=owner)


class TestContainsMethods:
    """Tests for container-like methods."""

    def test_len_method(self, memory_lock_manager):
        """Test __len__ method works."""
        owner = "owner1"
        
        assert len(memory_lock_manager) == 0
        
        memory_lock_manager.acquire("lock_1", owner=owner)
        assert len(memory_lock_manager) == 1
        
        memory_lock_manager.acquire("lock_2", owner=owner)
        assert len(memory_lock_manager) == 2
        
        memory_lock_manager.release("lock_1", owner=owner)
        memory_lock_manager.release("lock_2", owner=owner)

    def test_contains_method(self, memory_lock_manager):
        """Test __contains__ method works."""
        owner = "owner1"
        
        assert "lock_1" not in memory_lock_manager
        
        memory_lock_manager.acquire("lock_1", owner=owner)
        assert "lock_1" in memory_lock_manager
        
        memory_lock_manager.release("lock_1", owner=owner)
        assert "lock_1" not in memory_lock_manager


class TestContextManagerSupport:
    """Tests for context manager support."""

    def test_manager_as_context_manager(self):
        """Test lock manager can be used as context manager."""
        with InMemoryLockManager(cleanup_interval=0) as manager:
            lock_info = manager.acquire("test_lock", owner="owner")
            assert lock_info is not None