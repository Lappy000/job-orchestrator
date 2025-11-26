#!/usr/bin/env python3
"""
Distributed Locking Example
===========================

This example demonstrates using distributed locks with the Job Orchestrator
to ensure safe access to shared resources. It covers:

- In-memory locking for single-process
- Context manager pattern for locks
- Lock timeouts and TTL
- Lock extension
- Handling lock failures
- Redis-based distributed locking (conceptual)

Run this example:
    python examples/distributed_locking.py
"""

import time
import threading
from datetime import datetime
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from job_orchestrator import Job, OrchestratorConfig
from job_orchestrator.scheduler import Scheduler
from job_orchestrator.locking import InMemoryLockManager, LockAcquisitionError


# =============================================================================
# Shared Resources (simulated)
# =============================================================================

class SharedCounter:
    """A shared counter that needs synchronized access."""
    
    def __init__(self) -> None:
        self.value = 0
        self._lock = threading.Lock()
    
    def get(self) -> int:
        return self.value
    
    def increment(self) -> int:
        """Non-atomic increment (unsafe without external lock)."""
        current = self.value
        time.sleep(0.01)  # Simulate some processing
        self.value = current + 1
        return self.value
    
    def decrement(self) -> int:
        """Non-atomic decrement (unsafe without external lock)."""
        current = self.value
        time.sleep(0.01)
        self.value = current - 1
        return self.value


class SharedResource:
    """A shared resource that can only be accessed by one worker at a time."""
    
    def __init__(self, name: str) -> None:
        self.name = name
        self.in_use = False
        self.user: str | None = None
        self.access_log: list[dict] = []
    
    def acquire(self, user: str) -> bool:
        if self.in_use:
            return False
        self.in_use = True
        self.user = user
        self.access_log.append({
            "action": "acquire",
            "user": user,
            "time": datetime.utcnow().isoformat(),
        })
        return True
    
    def release(self, user: str) -> bool:
        if not self.in_use or self.user != user:
            return False
        self.in_use = False
        self.access_log.append({
            "action": "release",
            "user": user,
            "time": datetime.utcnow().isoformat(),
        })
        self.user = None
        return True
    
    def use(self, user: str, duration: float = 0.1) -> dict:
        """Simulate using the resource."""
        if self.user != user:
            raise RuntimeError(f"Resource not owned by {user}")
        
        time.sleep(duration)
        return {
            "resource": self.name,
            "user": user,
            "duration": duration,
            "time": datetime.utcnow().isoformat(),
        }


# Global shared resources
shared_counter = SharedCounter()
shared_printer = SharedResource("printer")
shared_database = SharedResource("database_connection")


# =============================================================================
# Example Functions
# =============================================================================

def example_basic_locking() -> None:
    """Example 1: Basic lock acquire and release."""
    print("\n" + "=" * 60)
    print("Example 1: Basic Lock Operations")
    print("=" * 60)
    
    lock_manager = InMemoryLockManager()
    
    # Acquire a lock
    print("\nAcquiring lock...")
    lock_info = lock_manager.acquire(
        resource="shared_file",
        owner="worker-1",
        ttl=30.0,  # Lock expires after 30 seconds
    )
    
    if lock_info:
        print(f"  Lock acquired!")
        print(f"  Lock ID: {lock_info.lock_id}")
        print(f"  Resource: {lock_info.resource}")
        print(f"  Owner: {lock_info.owner}")
        print(f"  Expires at: {lock_info.expires_at}")
        print(f"  Remaining TTL: {lock_info.remaining_ttl:.1f}s")
        
        # Check if resource is locked
        is_locked = lock_manager.is_locked("shared_file")
        print(f"\n  Resource is locked: {is_locked}")
        
        # Try to acquire same lock (should fail)
        print("\n  Attempting second acquire (should fail)...")
        second_lock = lock_manager.acquire(
            resource="shared_file",
            owner="worker-2",
            timeout=0.1,  # Short timeout
        )
        print(f"  Second acquire succeeded: {second_lock is not None}")
        
        # Release the lock
        print("\n  Releasing lock...")
        released = lock_manager.release("shared_file", "worker-1")
        print(f"  Lock released: {released}")
        
        # Verify release
        is_locked = lock_manager.is_locked("shared_file")
        print(f"  Resource is now locked: {is_locked}")
    else:
        print("  Failed to acquire lock!")


def example_context_manager() -> None:
    """Example 2: Using locks with context manager."""
    print("\n" + "=" * 60)
    print("Example 2: Context Manager Pattern")
    print("=" * 60)
    
    lock_manager = InMemoryLockManager()
    counter = SharedCounter()
    
    print("\nUsing context manager for safe access:")
    
    # Safe access with context manager
    with lock_manager.lock("counter", owner="main", ttl=10.0) as lock:
        print(f"  Lock acquired: {lock.lock_id}")
        
        # Safe operations inside the context
        for i in range(5):
            value = counter.increment()
            print(f"    Increment {i + 1}: counter = {value}")
        
        print(f"  Final value: {counter.get()}")
    
    print("  Lock automatically released")
    
    # Verify lock is released
    is_locked = lock_manager.is_locked("counter")
    print(f"  Resource still locked: {is_locked}")


def example_lock_timeout() -> None:
    """Example 3: Lock acquisition with timeout."""
    print("\n" + "=" * 60)
    print("Example 3: Lock Timeout")
    print("=" * 60)
    
    lock_manager = InMemoryLockManager()
    
    # First worker acquires the lock
    print("\nWorker 1 acquiring lock...")
    lock_manager.acquire("resource", owner="worker-1", ttl=5.0)
    print("  Worker 1 has the lock")
    
    # Second worker tries with timeout
    print("\nWorker 2 trying to acquire (2 second timeout)...")
    start = time.time()
    
    lock = lock_manager.acquire(
        resource="resource",
        owner="worker-2",
        timeout=2.0,  # Wait up to 2 seconds
    )
    
    elapsed = time.time() - start
    
    if lock:
        print(f"  Worker 2 acquired lock after {elapsed:.1f}s")
    else:
        print(f"  Worker 2 failed after {elapsed:.1f}s (timeout)")
    
    # Release for cleanup
    lock_manager.release("resource", "worker-1")


def example_lock_ttl_expiration() -> None:
    """Example 4: Lock TTL expiration."""
    print("\n" + "=" * 60)
    print("Example 4: Lock TTL Expiration")
    print("=" * 60)
    
    lock_manager = InMemoryLockManager()
    
    # Acquire with short TTL
    print("\nAcquiring lock with 1 second TTL...")
    lock = lock_manager.acquire("resource", owner="worker-1", ttl=1.0)
    print(f"  Lock acquired, expires at: {lock.expires_at}")
    
    # Check immediately
    print(f"  Is locked: {lock_manager.is_locked('resource')}")
    print(f"  Is expired: {lock.is_expired}")
    
    # Wait for expiration
    print("\nWaiting for TTL expiration (1.5 seconds)...")
    time.sleep(1.5)
    
    # Check after expiration
    print(f"  Is locked: {lock_manager.is_locked('resource')}")
    
    # Another worker can now acquire
    print("\nWorker 2 attempting to acquire...")
    new_lock = lock_manager.acquire("resource", owner="worker-2", ttl=5.0)
    print(f"  Worker 2 acquired: {new_lock is not None}")
    
    if new_lock:
        lock_manager.release("resource", "worker-2")


def example_lock_extension() -> None:
    """Example 5: Extending lock TTL."""
    print("\n" + "=" * 60)
    print("Example 5: Lock Extension")
    print("=" * 60)
    
    lock_manager = InMemoryLockManager()
    
    # Acquire with short TTL
    print("\nAcquiring lock with 2 second TTL...")
    lock = lock_manager.acquire("resource", owner="worker-1", ttl=2.0)
    print(f"  Remaining TTL: {lock.remaining_ttl:.1f}s")
    
    # Do some work
    print("\nDoing work for 1 second...")
    time.sleep(1.0)
    print(f"  Remaining TTL: {lock.remaining_ttl:.1f}s")
    
    # Extend the lock
    print("\nExtending lock by 10 seconds...")
    extended = lock_manager.extend("resource", "worker-1", ttl=10.0)
    print(f"  Extension successful: {extended}")
    
    # Check new lock info
    new_info = lock_manager.get_lock_info("resource")
    if new_info:
        print(f"  New remaining TTL: {new_info.remaining_ttl:.1f}s")
    
    # Cleanup
    lock_manager.release("resource", "worker-1")


def example_concurrent_access() -> None:
    """Example 6: Concurrent access with locking."""
    print("\n" + "=" * 60)
    print("Example 6: Safe Concurrent Access")
    print("=" * 60)
    
    lock_manager = InMemoryLockManager()
    counter = SharedCounter()
    counter.value = 0  # Reset
    
    results: list[dict] = []
    
    def worker_task(worker_id: str, iterations: int) -> dict:
        """Worker that increments counter safely."""
        increments = 0
        lock_waits = 0
        
        for _ in range(iterations):
            # Acquire lock before incrementing
            try:
                with lock_manager.lock(
                    "counter",
                    owner=worker_id,
                    timeout=5.0,
                    ttl=1.0,
                ):
                    counter.increment()
                    increments += 1
            except LockAcquisitionError:
                lock_waits += 1
        
        return {
            "worker": worker_id,
            "increments": increments,
            "lock_waits": lock_waits,
        }
    
    print("\nStarting 5 workers, each doing 10 increments...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for i in range(5):
            future = executor.submit(worker_task, f"worker-{i}", 10)
            futures.append(future)
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(f"  {result['worker']}: {result['increments']} increments")
    
    print(f"\nFinal counter value: {counter.get()}")
    print(f"Expected value: 50")
    print(f"Correct: {counter.get() == 50}")


def example_lock_with_jobs() -> None:
    """Example 7: Using locks with scheduled jobs."""
    print("\n" + "=" * 60)
    print("Example 7: Locks with Scheduled Jobs")
    print("=" * 60)
    
    lock_manager = InMemoryLockManager()
    access_results: list[str] = []
    
    def job_with_lock(job_name: str, resource: str, duration: float) -> str:
        """Job that acquires a lock before doing work."""
        with lock_manager.lock(resource, owner=job_name, timeout=10.0, ttl=5.0):
            access_results.append(f"{job_name} acquired {resource}")
            print(f"  {job_name}: Working with {resource}...")
            time.sleep(duration)
            access_results.append(f"{job_name} released {resource}")
            return f"{job_name} completed"
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Create jobs that need the same resource
        jobs = [
            Job(
                name="job_a",
                func=job_with_lock,
                args=("job_a", "shared_printer", 0.3),
            ),
            Job(
                name="job_b",
                func=job_with_lock,
                args=("job_b", "shared_printer", 0.2),
            ),
            Job(
                name="job_c",
                func=job_with_lock,
                args=("job_c", "shared_printer", 0.1),
            ),
        ]
        
        print("\nSubmitting 3 jobs that need the same resource...")
        
        # Run jobs (they will serialize on the lock)
        for job in jobs:
            result = scheduler.run_job(job)
            print(f"  {job.name}: {result.state.name}")
        
        print("\nAccess order:")
        for entry in access_results:
            print(f"  {entry}")
            
    finally:
        scheduler.stop()


def example_multiple_resources() -> None:
    """Example 8: Locking multiple resources."""
    print("\n" + "=" * 60)
    print("Example 8: Multiple Resource Locking")
    print("=" * 60)
    
    lock_manager = InMemoryLockManager()
    
    def transfer_funds(from_account: str, to_account: str, amount: float) -> dict:
        """Transfer funds between accounts with proper locking."""
        # Always acquire locks in sorted order to prevent deadlocks
        accounts = sorted([from_account, to_account])
        
        print(f"  Acquiring locks for {accounts}...")
        
        with lock_manager.lock(accounts[0], owner="transfer", ttl=10.0):
            with lock_manager.lock(accounts[1], owner="transfer", ttl=10.0):
                print(f"  Both locks acquired")
                print(f"  Transferring ${amount} from {from_account} to {to_account}")
                time.sleep(0.2)  # Simulate database operations
                
        print(f"  Locks released")
        return {
            "from": from_account,
            "to": to_account,
            "amount": amount,
            "status": "completed",
        }
    
    print("\nPerforming fund transfer with multi-resource locking...")
    result = transfer_funds("account_A", "account_B", 100.0)
    print(f"Transfer result: {result}")
    
    # Demonstrate deadlock prevention
    print("\nDemonstrating deadlock prevention with ordered locking...")
    
    def concurrent_transfer(name: str, acc1: str, acc2: str) -> None:
        transfer_funds(acc1, acc2, 50.0)
        print(f"  {name} completed")
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        # These could deadlock without ordered locking
        executor.submit(concurrent_transfer, "Transfer1", "account_A", "account_B")
        executor.submit(concurrent_transfer, "Transfer2", "account_B", "account_A")
    
    print("\nBoth transfers completed without deadlock!")


def example_redis_locking_concept() -> None:
    """Example 9: Redis-based distributed locking (conceptual)."""
    print("\n" + "=" * 60)
    print("Example 9: Redis Distributed Locking (Conceptual)")
    print("=" * 60)
    
    print("""
This example shows the concept of Redis-based distributed locking.
In production, you would use the RedisLockManager:

    from job_orchestrator.locking import RedisLockManager
    import redis
    
    # Single Redis instance
    client = redis.Redis(host='localhost', port=6379)
    lock_manager = RedisLockManager(redis_client=client)
    
    # Or multiple instances for Redlock algorithm
    lock_manager = RedisLockManager(redis_urls=[
        "redis://node1:6379",
        "redis://node2:6379",
        "redis://node3:6379",
    ])
    
    # Usage is identical to InMemoryLockManager
    with lock_manager.lock("resource", owner="worker-1", ttl=30.0) as lock:
        # Safe distributed access
        process_shared_resource()

Key differences from in-memory locking:
1. Works across multiple processes/machines
2. Survives process restarts (lock state in Redis)
3. Uses Redlock algorithm for high availability
4. Network latency affects lock operations
""")


def main() -> None:
    """Run all distributed locking examples."""
    print("=" * 60)
    print("Job Orchestrator - Distributed Locking Examples")
    print("=" * 60)
    
    example_basic_locking()
    example_context_manager()
    example_lock_timeout()
    example_lock_ttl_expiration()
    example_lock_extension()
    example_concurrent_access()
    example_lock_with_jobs()
    example_multiple_resources()
    example_redis_locking_concept()
    
    print("\n" + "=" * 60)
    print("All locking examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()