"""
File-based lock manager for multiprocessing environments.

This module provides a lock manager that uses filesystem locks for
synchronization between processes on the same machine. It's useful for
multiprocessing scenarios without Redis.
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from .base import LockInfo, LockManager

logger = logging.getLogger(__name__)


# Platform-specific file locking
if sys.platform == "win32":
    import msvcrt

    def _lock_file_exclusive(file_handle, blocking: bool = True) -> bool:
        """Acquire exclusive lock on Windows."""
        try:
            if blocking:
                msvcrt.locking(file_handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                msvcrt.locking(file_handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except (IOError, OSError):
            return False

    def _unlock_file(file_handle) -> bool:
        """Release lock on Windows."""
        try:
            msvcrt.locking(file_handle.fileno(), msvcrt.LK_UNLCK, 1)
            return True
        except (IOError, OSError):
            return False

else:
    import fcntl

    def _lock_file_exclusive(file_handle, blocking: bool = True) -> bool:
        """Acquire exclusive lock on Unix."""
        try:
            if blocking:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX)
            else:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (IOError, OSError):
            return False

    def _unlock_file(file_handle) -> bool:
        """Release lock on Unix."""
        try:
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
            return True
        except (IOError, OSError):
            return False


class FileLockManager(LockManager):
    """
    Lock manager using filesystem locks for multiprocessing.
    
    This implementation uses file-based locking to synchronize access
    between multiple processes on the same machine. It's suitable for:
    - Process-based workers on a single node
    - Environments without Redis or other distributed lock stores
    - Local development and testing with multiprocessing
    
    Features:
    - Cross-platform support (Unix and Windows)
    - Optional blocking with timeout
    - Soft TTL enforcement (cleaned up on next access)
    - Lock metadata stored in lock files
    
    Limitations:
    - Only works for processes on the same filesystem
    - Not suitable for distributed deployments across multiple nodes
    - TTL is "soft" - relies on cooperation from lockers
    - File system operations have higher latency than memory
    
    Example:
        lock_manager = FileLockManager(lock_dir="/tmp/my_app_locks")
        
        with lock_manager.lock("job:123", owner="worker-1"):
            # Do work with exclusive access
            pass
    """
    
    def __init__(
        self,
        lock_dir: Optional[str] = None,
        auto_cleanup: bool = True,
        cleanup_interval: float = 300.0,
    ):
        """
        Initialize the file lock manager.
        
        Args:
            lock_dir: Directory to store lock files. Defaults to a
                subdirectory in the system temp directory.
            auto_cleanup: Whether to automatically clean up expired
                lock files.
            cleanup_interval: Interval between automatic cleanups
                in seconds.
        """
        if lock_dir is None:
            import tempfile
            lock_dir = os.path.join(tempfile.gettempdir(), "job_orchestrator_locks")
        
        self._lock_dir = Path(lock_dir)
        self._lock_dir.mkdir(parents=True, exist_ok=True)
        
        self._auto_cleanup = auto_cleanup
        self._cleanup_interval = cleanup_interval
        self._last_cleanup = 0.0
        
        # Track file handles for locks we hold
        self._held_locks: Dict[str, Any] = {}
    
    def _get_lock_path(self, resource: str) -> Path:
        """Get the file path for a lock."""
        # Sanitize resource name for filesystem
        safe_name = resource.replace("/", "_").replace("\\", "_").replace(":", "_")
        return self._lock_dir / f"{safe_name}.lock"
    
    def _maybe_cleanup(self) -> None:
        """Run cleanup if enough time has passed."""
        if not self._auto_cleanup:
            return
        
        now = time.time()
        if now - self._last_cleanup >= self._cleanup_interval:
            self._last_cleanup = now
            self.cleanup_expired()
    
    def _read_lock_info(self, lock_path: Path) -> Optional[Dict[str, Any]]:
        """Read lock info from a lock file."""
        try:
            if lock_path.exists():
                with open(lock_path, "r") as f:
                    return json.load(f)
        except (IOError, json.JSONDecodeError):
            pass
        return None
    
    def _write_lock_info(
        self,
        lock_path: Path,
        owner: str,
        expires_at: Optional[datetime],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write lock info to a lock file."""
        lock_data = {
            "owner": owner,
            "acquired_at": datetime.utcnow().isoformat(),
            "expires_at": expires_at.isoformat() if expires_at else None,
            "metadata": metadata or {},
            "pid": os.getpid(),
        }
        with open(lock_path, "w") as f:
            json.dump(lock_data, f)
            f.flush()
            os.fsync(f.fileno())
    
    def _is_lock_expired(self, lock_data: Dict[str, Any]) -> bool:
        """Check if a lock has expired based on its data."""
        expires_at_str = lock_data.get("expires_at")
        if expires_at_str is None:
            return False
        
        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            return datetime.utcnow() > expires_at
        except (ValueError, TypeError):
            return False
    
    def _is_process_alive(self, pid: int) -> bool:
        """Check if a process is still alive."""
        try:
            if sys.platform == "win32":
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x100000, False, pid)
                if handle:
                    kernel32.CloseHandle(handle)
                    return True
                return False
            else:
                os.kill(pid, 0)
                return True
        except (OSError, PermissionError):
            return False
    
    def acquire(
        self,
        resource: str,
        owner: Optional[str] = None,
        timeout: Optional[float] = None,
        ttl: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[LockInfo]:
        """
        Acquire a file-based lock on a resource.
        
        Args:
            resource: Resource identifier to lock.
            owner: Owner identifier. Defaults to unique UUID.
            timeout: Maximum time to wait for lock acquisition.
            ttl: Lock time-to-live in seconds.
            metadata: Optional metadata to store in lock file.
        
        Returns:
            LockInfo if acquired, None if failed.
        """
        self._maybe_cleanup()
        
        owner = owner or str(uuid.uuid4())
        lock_path = self._get_lock_path(resource)
        deadline = time.time() + timeout if timeout and timeout > 0 else None
        expires_at = datetime.utcnow() + timedelta(seconds=ttl) if ttl > 0 else None
        
        while True:
            try:
                # Check if we already hold this lock
                if resource in self._held_locks:
                    file_handle = self._held_locks[resource]
                    # Update TTL
                    self._write_lock_info(lock_path, owner, expires_at, metadata)
                    lock_data = self._read_lock_info(lock_path)
                    return LockInfo(
                        lock_id=str(uuid.uuid4()),
                        resource=resource,
                        owner=owner,
                        acquired_at=datetime.fromisoformat(lock_data["acquired_at"]),
                        expires_at=expires_at,
                        metadata=metadata or {},
                    )
                
                # Check for existing lock
                existing_data = self._read_lock_info(lock_path)
                if existing_data:
                    # Check if lock is expired
                    if self._is_lock_expired(existing_data):
                        # Clean up expired lock
                        try:
                            lock_path.unlink()
                        except (IOError, OSError):
                            pass
                    # Check if owning process is dead
                    elif "pid" in existing_data:
                        if not self._is_process_alive(existing_data["pid"]):
                            # Process is dead, clean up stale lock
                            try:
                                lock_path.unlink()
                            except (IOError, OSError):
                                pass
                
                # Try to acquire the lock
                # Open file in exclusive create mode
                try:
                    file_handle = open(lock_path, "x")
                except FileExistsError:
                    file_handle = open(lock_path, "r+")
                
                # Try to get exclusive file lock
                blocking = timeout is not None and timeout > 0
                if _lock_file_exclusive(file_handle, blocking=False):
                    # We got the lock
                    self._write_lock_info(lock_path, owner, expires_at, metadata)
                    self._held_locks[resource] = file_handle
                    
                    return LockInfo(
                        lock_id=str(uuid.uuid4()),
                        resource=resource,
                        owner=owner,
                        acquired_at=datetime.utcnow(),
                        expires_at=expires_at,
                        metadata=metadata or {},
                    )
                else:
                    file_handle.close()
                    
                # Check deadline
                if deadline is None or timeout <= 0:
                    return None
                
                if time.time() >= deadline:
                    return None
                
                # Wait and retry
                time.sleep(0.01)
                
            except (IOError, OSError) as e:
                logger.debug(f"Lock acquisition failed: {e}")
                
                if deadline is None or timeout <= 0:
                    return None
                
                if time.time() >= deadline:
                    return None
                
                time.sleep(0.01)
    
    def release(self, resource: str, owner: Optional[str] = None) -> bool:
        """
        Release a file-based lock.
        
        Args:
            resource: Resource identifier to unlock.
            owner: Owner identifier. If provided, verifies ownership.
        
        Returns:
            True if released, False if not held or wrong owner.
        """
        lock_path = self._get_lock_path(resource)
        
        try:
            # Check if we hold this lock
            if resource in self._held_locks:
                file_handle = self._held_locks[resource]
                
                # Verify owner if provided
                if owner is not None:
                    lock_data = self._read_lock_info(lock_path)
                    if lock_data and lock_data.get("owner") != owner:
                        return False
                
                # Release the lock
                _unlock_file(file_handle)
                file_handle.close()
                del self._held_locks[resource]
                
                # Remove lock file
                try:
                    lock_path.unlink()
                except (IOError, OSError):
                    pass
                
                return True
            else:
                # We don't hold this lock, but try to clean it up if we're the owner
                if owner is not None:
                    lock_data = self._read_lock_info(lock_path)
                    if lock_data and lock_data.get("owner") == owner:
                        try:
                            lock_path.unlink()
                            return True
                        except (IOError, OSError):
                            pass
                elif owner is None:
                    # Force release
                    try:
                        lock_path.unlink()
                        return True
                    except (IOError, OSError):
                        pass
                
                return False
                
        except (IOError, OSError) as e:
            logger.warning(f"Failed to release lock: {e}")
            return False
    
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
        lock_path = self._get_lock_path(resource)
        
        try:
            # Check if we hold this lock
            if resource not in self._held_locks:
                return False
            
            # Verify owner
            lock_data = self._read_lock_info(lock_path)
            if not lock_data or lock_data.get("owner") != owner:
                return False
            
            # Update TTL
            expires_at = datetime.utcnow() + timedelta(seconds=ttl) if ttl > 0 else None
            self._write_lock_info(
                lock_path,
                owner,
                expires_at,
                lock_data.get("metadata"),
            )
            
            return True
            
        except (IOError, OSError) as e:
            logger.warning(f"Failed to extend lock: {e}")
            return False
    
    def is_locked(self, resource: str) -> bool:
        """
        Check if a resource is currently locked.
        
        Args:
            resource: Resource identifier.
        
        Returns:
            True if locked (and not expired), False otherwise.
        """
        lock_path = self._get_lock_path(resource)
        
        if not lock_path.exists():
            return False
        
        lock_data = self._read_lock_info(lock_path)
        if lock_data is None:
            return False
        
        # Check if expired
        if self._is_lock_expired(lock_data):
            return False
        
        # Check if owning process is alive
        if "pid" in lock_data:
            if not self._is_process_alive(lock_data["pid"]):
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
        lock_path = self._get_lock_path(resource)
        
        if not lock_path.exists():
            return None
        
        lock_data = self._read_lock_info(lock_path)
        if lock_data is None:
            return None
        
        # Check if expired
        if self._is_lock_expired(lock_data):
            return None
        
        # Check if owning process is alive
        if "pid" in lock_data:
            if not self._is_process_alive(lock_data["pid"]):
                return None
        
        expires_at = None
        if lock_data.get("expires_at"):
            try:
                expires_at = datetime.fromisoformat(lock_data["expires_at"])
            except (ValueError, TypeError):
                pass
        
        return LockInfo(
            lock_id=str(uuid.uuid4()),
            resource=resource,
            owner=lock_data["owner"],
            acquired_at=datetime.fromisoformat(lock_data["acquired_at"]),
            expires_at=expires_at,
            metadata=lock_data.get("metadata", {}),
        )
    
    def cleanup_expired(self) -> int:
        """
        Remove all expired lock files.
        
        Returns:
            Number of expired locks that were removed.
        """
        removed = 0
        
        try:
            for lock_file in self._lock_dir.glob("*.lock"):
                lock_data = self._read_lock_info(lock_file)
                should_remove = False
                
                if lock_data is None:
                    should_remove = True
                elif self._is_lock_expired(lock_data):
                    should_remove = True
                elif "pid" in lock_data:
                    if not self._is_process_alive(lock_data["pid"]):
                        should_remove = True
                
                if should_remove:
                    try:
                        lock_file.unlink()
                        removed += 1
                    except (IOError, OSError):
                        pass
        except (IOError, OSError) as e:
            logger.warning(f"Error during cleanup: {e}")
        
        return removed
    
    def clear(self) -> int:
        """
        Clear all lock files.
        
        Use with caution - this forcibly removes all locks.
        
        Returns:
            Number of locks that were cleared.
        """
        # First release any locks we hold
        for resource in list(self._held_locks.keys()):
            self.release(resource)
        
        # Then remove all lock files
        removed = 0
        try:
            for lock_file in self._lock_dir.glob("*.lock"):
                try:
                    lock_file.unlink()
                    removed += 1
                except (IOError, OSError):
                    pass
        except (IOError, OSError) as e:
            logger.warning(f"Error during clear: {e}")
        
        return removed
    
    def __enter__(self) -> "FileLockManager":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - release all held locks."""
        for resource in list(self._held_locks.keys()):
            self.release(resource)


__all__ = [
    "FileLockManager",
]